package coordinator

import (
	"context"
	"errors"
	"sync"
	"time"

	"vault/internal/events"
	"vault/internal/metadata"
	"vault/internal/objectid"
	"vault/internal/storage"
)

// DeleteResult reports what happened to each replica of a deleted object.
type DeleteResult struct {
	ObjectID string `json:"object_id"`
	// Version is the live version that was deleted.
	Version uint64 `json:"version"`
	// TombstoneVersion is the durable tombstone's version (Version+1). Any
	// later write creates TombstoneVersion+1.
	TombstoneVersion uint64 `json:"tombstone_version"`
	// Deleted: nodes that removed their replica.
	Deleted []string `json:"deleted"`
	// AlreadyAbsent: nodes that reported no replica to delete.
	AlreadyAbsent []string `json:"already_absent,omitempty"`
	// Unreachable: nodes that could not be contacted or failed. Their bytes
	// remain on disk until the scrubber purges them; the tombstone makes
	// them unreadable and prevents them from ever resurrecting the object.
	Unreachable []string `json:"unreachable,omitempty"`
	// FullyPurged is true only if no node may still hold the object's bytes.
	FullyPurged bool `json:"fully_purged"`
}

// Delete deletes object id. See DeleteIfVersion.
func (c *Coordinator) Delete(ctx context.Context, id string) (DeleteResult, error) {
	return c.delete(ctx, id, nil)
}

// DeleteIfVersion deletes object id only if its current live version equals
// expected; otherwise it returns *ConflictError and changes nothing.
//
// Deleting writes a durable tombstone at Version+1 BEFORE touching any node,
// so the object is unreadable from that instant and no replica left behind
// (on an unreachable node, say) can ever make it reappear. Replica bytes are
// then removed from every replica node; the scrubber purges any that could
// not be reached.
func (c *Coordinator) DeleteIfVersion(ctx context.Context, id string, expected uint64) (DeleteResult, error) {
	return c.delete(ctx, id, &expected)
}

func (c *Coordinator) delete(ctx context.Context, id string, expected *uint64) (DeleteResult, error) {
	if err := objectid.Validate(id); err != nil {
		return DeleteResult{}, err
	}
	unlock := c.locks.Lock(id)
	defer unlock()

	obj, err := c.meta.GetObject(ctx, id)
	if err != nil {
		return DeleteResult{}, err
	}
	if err := c.checkExpected(ctx, id, expected, "delete"); err != nil {
		return DeleteResult{}, err
	}
	now := time.Now().UTC()
	tomb := metadata.Object{
		ID: id, Version: obj.Version + 1, CreatedAt: obj.CreatedAt, UpdatedAt: now,
		ReplicationFactor: obj.ReplicationFactor, Deleted: true, DeletedAt: &now,
	}
	if err := c.meta.UpdateObject(ctx, tomb); err != nil {
		return DeleteResult{}, err
	}
	c.log.Emit(ctx, events.TombstoneCreated, "object_id", id, "tombstone_version", tomb.Version,
		"deleted_version", obj.Version, "replicas", obj.Replicas)

	res := DeleteResult{ObjectID: id, Version: obj.Version, TombstoneVersion: tomb.Version}
	dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	var (
		mu sync.Mutex
		wg sync.WaitGroup
	)
	// Removing every version on these nodes is safe: we hold the object
	// lock, so no newer write can be landing on them concurrently.
	for _, nodeID := range obj.Replicas {
		wg.Add(1)
		go func(nodeID string) {
			defer wg.Done()
			n, err := c.node(nodeID)
			if err == nil {
				err = n.DeleteObject(dctx, id)
			}
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				res.Deleted = append(res.Deleted, nodeID)
			case errors.Is(err, storage.ErrNotFound):
				res.AlreadyAbsent = append(res.AlreadyAbsent, nodeID)
			default:
				c.reportIfUnavailable(dctx, nodeID, err)
				res.Unreachable = append(res.Unreachable, nodeID)
			}
		}(nodeID)
	}
	wg.Wait()
	res.FullyPurged = len(res.Unreachable) == 0

	c.log.Emit(ctx, events.ObjectDelete, "object_id", id, "version", obj.Version,
		"tombstone_version", tomb.Version, "deleted", res.Deleted, "already_absent", res.AlreadyAbsent,
		"unreachable", res.Unreachable, "fully_purged", res.FullyPurged)
	return res, nil
}
