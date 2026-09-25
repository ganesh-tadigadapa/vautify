package coordinator

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"vault/internal/events"
	"vault/internal/integrity"
	"vault/internal/metadata"
	"vault/internal/objectid"
	"vault/internal/placement"
	"vault/internal/storage"
)

// PutResult describes a committed write.
type PutResult struct {
	Object   metadata.Object  `json:"object"`
	Created  bool             `json:"created"`
	Attempts []ReplicaAttempt `json:"attempts"`
	// PreviousVersion is the version this write superseded: the previous
	// live version, the tombstone version after a delete, or 0 for a brand
	// new object.
	PreviousVersion uint64 `json:"previous_version"`
	// RetainedStaleReplicas lists nodes that could not delete the previous
	// version's replica after an overwrite. The new version is fully
	// committed regardless; these are leftover bytes, not live data.
	RetainedStaleReplicas []string `json:"retained_stale_replicas,omitempty"`
	// WriteQuorum is the number of verified replicas this write required.
	// When it is below the replication factor, len(Object.Replicas) may be
	// below RF too: the object is committed DEGRADED and repaired.
	WriteQuorum int `json:"write_quorum"`
}

// Put stores body as object id with the given replication factor (0 means
// the configured default) and the configured write quorum. With the default
// quorum (all) it succeeds only if every one of the required replicas was
// written and verified by its node; otherwise it rolls back and returns
// *ReplicationError or *placement.InsufficientNodesError. With a smaller
// quorum W it succeeds once W replicas are stored and verified, and fails
// with *QuorumError (rolled back) below W.
func (c *Coordinator) Put(ctx context.Context, id string, body io.Reader, replicationFactor int) (PutResult, error) {
	return c.put(ctx, id, body, PutOptions{ReplicationFactor: replicationFactor})
}

// PutOptions tunes one write.
type PutOptions struct {
	// ReplicationFactor: 0 means the configured default.
	ReplicationFactor int
	// WriteQuorum overrides the configured write quorum for this write.
	WriteQuorum *Quorum
	// ExpectedVersion makes the write a compare-and-swap (see PutIfVersion).
	ExpectedVersion *uint64
}

// PutWithOptions is Put with per-write replication, quorum and CAS options.
func (c *Coordinator) PutWithOptions(ctx context.Context, id string, body io.Reader, opts PutOptions) (PutResult, error) {
	return c.put(ctx, id, body, opts)
}

// PutIfVersion is Put with compare-and-swap: it proceeds only if the
// object's current live version equals expected (0 = the object must not
// currently exist). A mismatch returns *ConflictError and changes nothing:
// no replica is written and no metadata is touched.
func (c *Coordinator) PutIfVersion(ctx context.Context, id string, body io.Reader, replicationFactor int, expected uint64) (PutResult, error) {
	return c.put(ctx, id, body, PutOptions{ReplicationFactor: replicationFactor, ExpectedVersion: &expected})
}

// notEnoughNodes is the error for a write that cannot even start: fewer
// healthy nodes than its quorum. With the default quorum (all replicas) it
// is the P1 *placement.InsufficientNodesError.
func (c *Coordinator) notEnoughNodes(ctx context.Context, id string, rf, w, healthy int) error {
	if w == rf {
		return &placement.InsufficientNodesError{Required: rf, Available: healthy}
	}
	return c.quorumFailed(ctx, &QuorumError{ObjectID: id, Operation: "write", Required: w, ReplicationFactor: rf,
		Detail: fmt.Sprintf("only %d healthy nodes; nothing was written", healthy)})
}

func (c *Coordinator) put(ctx context.Context, id string, body io.Reader, opts PutOptions) (PutResult, error) {
	expected := opts.ExpectedVersion
	if err := objectid.Validate(id); err != nil {
		return PutResult{}, err
	}
	rf, err := c.resolveReplicationFactor(opts.ReplicationFactor)
	if err != nil {
		return PutResult{}, err
	}
	w, err := c.writeQuorum(opts.WriteQuorum, rf)
	if err != nil {
		return PutResult{}, err
	}
	// Fail fast, before reading the body, on a CAS mismatch or a policy
	// that is clearly unmeetable. Both are re-checked under the lock.
	if err := c.checkExpected(ctx, id, expected, "put"); err != nil {
		return PutResult{}, err
	}
	if healthy := len(c.health.Healthy()); healthy < w {
		return PutResult{}, c.notEnoughNodes(ctx, id, rf, w, healthy)
	}

	release, err := c.acquireTransfer(ctx)
	if err != nil {
		return PutResult{}, err
	}
	defer release()

	spool, err := integrity.Spool(body, c.cfg.TempDir, c.cfg.MaxObjectSize)
	if err != nil {
		return PutResult{}, err
	}
	defer spool.Close()

	unlock := c.locks.Lock(id)
	defer unlock()

	// Authoritative CAS check: under the object lock, before any replica
	// is written, so a failed CAS leaves nothing behind.
	if err := c.checkExpected(ctx, id, expected, "put"); err != nil {
		return PutResult{}, err
	}
	prev, err := c.meta.GetRecord(ctx, id)
	exists := err == nil
	if err != nil && !errors.Is(err, metadata.ErrNotFound) {
		return PutResult{}, err
	}
	live := exists && !prev.Deleted
	// Versions are monotonic per object ID across its whole history,
	// deletes included: a tombstone holds a version too.
	version := uint64(1)
	if exists {
		version = prev.Version + 1
	}

	c.log.Emit(ctx, events.ObjectWriteStarted, "object_id", id, "version", version,
		"size", spool.Digest.Size, "sha256", spool.Digest.SHA256, "replication_factor", rf, "write_quorum", w)

	// Aim for RF replicas; with a quorum below RF, fewer healthy nodes than
	// RF is acceptable as long as there are at least W.
	healthy := c.health.Healthy()
	targets := rf
	if w < rf && len(healthy) < rf {
		if len(healthy) < w {
			err := c.notEnoughNodes(ctx, id, rf, w, len(healthy))
			c.log.Warn(ctx, events.ObjectWriteFailed, "object_id", id, "version", version, "error", err.Error())
			return PutResult{}, err
		}
		targets = len(healthy)
	}
	plan, err := c.placer.Place(id, healthy, targets)
	if err != nil {
		c.log.Warn(ctx, events.ObjectWriteFailed, "object_id", id, "version", version, "error", err.Error())
		return PutResult{}, err
	}

	written, attempts := c.writeReplicas(ctx, id, version, spool, plan)
	if len(written) < w {
		c.rollback(ctx, id, version, written)
		var rerr error = &ReplicationError{ObjectID: id, Required: rf, Achieved: len(written), Attempts: attempts}
		if w < rf {
			rerr = c.quorumFailed(ctx, &QuorumError{ObjectID: id, Operation: "write", Version: version,
				Required: w, Achieved: len(written), ReplicationFactor: rf, Attempts: attempts,
				Detail: "written replicas were rolled back; nothing was committed"})
		}
		c.log.Warn(ctx, events.ObjectWriteFailed, "object_id", id, "version", version,
			"required", w, "achieved", len(written), "error", rerr.Error())
		return PutResult{}, rerr
	}

	now := time.Now().UTC()
	obj := metadata.Object{
		ID: id, Version: version, Size: spool.Digest.Size, SHA256: spool.Digest.SHA256,
		CreatedAt: now, UpdatedAt: now, ReplicationFactor: rf, Replicas: written,
	}
	if live {
		obj.CreatedAt = prev.CreatedAt
	}
	if exists {
		err = c.meta.UpdateObject(ctx, obj)
	} else {
		err = c.meta.PutObject(ctx, obj)
	}
	if err != nil {
		c.rollback(ctx, id, version, written)
		c.log.Warn(ctx, events.ObjectWriteFailed, "object_id", id, "version", version, "error", "metadata commit: "+err.Error())
		return PutResult{}, err
	}

	res := PutResult{Object: obj, Created: !live, Attempts: attempts, WriteQuorum: w}
	previousState := "absent"
	if exists {
		res.PreviousVersion = prev.Version
		previousState = "deleted"
	}
	if live {
		previousState = "live"
		// The new version was verified by each of its nodes as it was
		// written (checksum and version, see putReplica) and is committed;
		// only now do the superseded replicas go, through the guarded path.
		res.RetainedStaleReplicas = c.cleanupVersion(ctx, obj, prev.Version, prev.Replicas, written)
	}
	c.log.Emit(ctx, events.VersionCreated, "object_id", id, "version", version,
		"previous_version", res.PreviousVersion, "previous_state", previousState,
		"sha256", obj.SHA256, "size", obj.Size, "replicas", written)
	c.log.Emit(ctx, events.ObjectWriteCompleted, "object_id", id, "version", version,
		"replicas", written, "replication_factor", rf, "achieved", len(written), "write_quorum", w)
	return res, nil
}

// writeReplicas writes the spooled bytes to every target concurrently. If a
// target fails, the worker retries on the next unused spare. It returns the
// nodes that stored the replica, in the order of plan.Targets slots.
func (c *Coordinator) writeReplicas(ctx context.Context, id string, version uint64, spool *integrity.Spooled, plan placement.Plan) ([]string, []ReplicaAttempt) {
	var (
		mu       sync.Mutex
		spares   = append([]string(nil), plan.Spares...)
		attempts []ReplicaAttempt
		slots    = make([]string, len(plan.Targets))
		wg       sync.WaitGroup
	)
	nextSpare := func() (string, bool) {
		mu.Lock()
		defer mu.Unlock()
		if len(spares) == 0 {
			return "", false
		}
		n := spares[0]
		spares = spares[1:]
		return n, true
	}

	for i, target := range plan.Targets {
		wg.Add(1)
		go func(slot int, nodeID string) {
			defer wg.Done()
			for {
				a := c.writeOne(ctx, id, version, spool, nodeID)
				mu.Lock()
				attempts = append(attempts, a)
				mu.Unlock()
				if a.Outcome == OutcomeStored {
					slots[slot] = nodeID
					return
				}
				if ctx.Err() != nil {
					return
				}
				var ok bool
				if nodeID, ok = nextSpare(); !ok {
					return
				}
			}
		}(i, target)
	}
	wg.Wait()

	var written []string
	for _, n := range slots {
		if n != "" {
			written = append(written, n)
		}
	}
	return written, attempts
}

func (c *Coordinator) writeOne(ctx context.Context, id string, version uint64, spool *integrity.Spooled, nodeID string) ReplicaAttempt {
	a := c.putReplica(ctx, id, version, spool, nodeID)
	if a.Error != "" {
		c.log.Warn(ctx, events.ObjectReplicaWrite, "object_id", id, "version", version,
			"node_id", nodeID, "outcome", a.Outcome, "error", a.Error, "duration_ms", a.DurationMS)
	} else {
		c.log.Emit(ctx, events.ObjectReplicaWrite, "object_id", id, "version", version,
			"node_id", nodeID, "outcome", a.Outcome, "sha256", spool.Digest.SHA256, "duration_ms", a.DurationMS)
	}
	return a
}

// putReplica uploads the spooled bytes to one node, which verifies them
// against the declared digest before committing. It emits no events.
func (c *Coordinator) putReplica(ctx context.Context, id string, version uint64, spool *integrity.Spooled, nodeID string) ReplicaAttempt {
	start := time.Now()
	a := ReplicaAttempt{NodeID: nodeID}
	n, err := c.node(nodeID)
	if err == nil {
		var r io.ReadCloser
		if r, err = spool.Open(); err == nil {
			rctx, cancel := context.WithTimeout(ctx, c.cfg.ReplicaTimeout)
			var info storage.ReplicaInfo
			info, err = n.PutReplica(rctx, id, version, spool.Digest, r)
			cancel()
			r.Close()
			// The node hashed the bytes as it stored them and refuses a
			// mismatch; also check that its committed record names exactly
			// this object, version and checksum.
			if err == nil && (info.ObjectID != id || info.Version != version ||
				info.SHA256 != spool.Digest.SHA256 || info.Size != spool.Digest.Size) {
				err = fmt.Errorf("node %s acknowledged %s v%d %s/%d, expected v%d %s/%d", nodeID,
					info.ObjectID, info.Version, info.SHA256, info.Size, version, spool.Digest.SHA256, spool.Digest.Size)
			}
		}
	}
	a.DurationMS = since(start)
	switch {
	case err == nil:
		a.Outcome = OutcomeStored
	case errors.Is(err, storage.ErrRejected):
		a.Outcome = OutcomeRejected
	case errors.Is(err, storage.ErrStaleVersion):
		a.Outcome = OutcomeStaleRefused
	case c.reportIfUnavailable(ctx, nodeID, err):
		a.Outcome = OutcomeUnavailable
	default:
		a.Outcome = OutcomeError
	}
	if err != nil {
		a.Error = err.Error()
	}
	return a
}

// rollback removes a version that was not committed. Failures leave orphaned
// bytes on a node, which are logged; they are never referenced by metadata.
func (c *Coordinator) rollback(ctx context.Context, id string, version uint64, nodes []string) {
	if left := c.deleteVersion(ctx, id, version, nodes); len(left) > 0 {
		c.log.Warn(ctx, events.ReplicaCleanup, "object_id", id, "version", version,
			"reason", "rollback", "orphaned_on", left)
	}
}

// cleanupVersion removes a superseded version from nodes after cur (the
// just-committed record) and returns nodes where that failed or was refused.
// verified are the nodes that stored and verified cur's version in this same
// operation, under the object lock the caller still holds: the proof the
// guarded removal needs that a valid newer copy exists.
func (c *Coordinator) cleanupVersion(ctx context.Context, cur metadata.Object, version uint64, nodes, verified []string) []string {
	id := cur.ID
	// Like rollback, cleanup outlives a client that has gone away.
	dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	var (
		mu   sync.Mutex
		wg   sync.WaitGroup
		left []string
	)
	proof := &removalProof{version: cur.Version, nodes: verified}
	for _, n := range nodes {
		wg.Add(1)
		go func(n string) {
			defer wg.Done()
			if _, err := c.removeReplicaLocked(dctx, cur, n, version, "superseded", proof); err != nil {
				mu.Lock()
				left = append(left, n)
				mu.Unlock()
			}
		}(n)
	}
	wg.Wait()
	if len(left) > 0 {
		c.log.Warn(ctx, events.ReplicaCleanup, "object_id", id, "version", version,
			"reason", "superseded", "retained_on", left)
	} else {
		c.log.Emit(ctx, events.ReplicaCleanup, "object_id", id, "version", version,
			"reason", "superseded", "removed_from", nodes)
	}
	return left
}

// deleteVersion deletes (id, version) from nodes concurrently. It runs even
// if ctx is cancelled (the client may have gone away) but with its own
// deadline. Returns nodes where deletion failed.
func (c *Coordinator) deleteVersion(ctx context.Context, id string, version uint64, nodes []string) []string {
	dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	var (
		mu     sync.Mutex
		failed []string
		wg     sync.WaitGroup
	)
	for _, n := range nodes {
		wg.Add(1)
		go func(nodeID string) {
			defer wg.Done()
			n, err := c.node(nodeID)
			if err == nil {
				err = n.DeleteReplica(dctx, id, version)
			}
			if err == nil || errors.Is(err, storage.ErrNotFound) {
				return
			}
			c.reportIfUnavailable(dctx, nodeID, err)
			mu.Lock()
			failed = append(failed, nodeID)
			mu.Unlock()
		}(n)
	}
	wg.Wait()
	return failed
}
