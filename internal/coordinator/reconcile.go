package coordinator

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"time"

	"vault/internal/durability"
	"vault/internal/events"
	"vault/internal/metadata"
	"vault/internal/storage"
)

// Reconcile actions, per object on the reconciled node.
const (
	ReconcileVerified          = "verified"           // listed current replica re-hashed OK
	ReconcileRecordedCorrupted = "recorded_corrupted" // listed replica's bytes are wrong → repair
	ReconcileRecordedMissing   = "recorded_missing"   // listed replica absent → repair
	ReconcileRecordedStale     = "recorded_stale"     // listed replica holds only an older version → repair
	ReconcileAdopted           = "adopted"            // unlisted verified current copy committed as a replica
	ReconcileRemoved           = "removed"            // guarded removal deleted a version
	ReconcileKept              = "kept"               // guarded removal refused: kept on disk
	ReconcileDeferred          = "deferred"           // a repair of the object was running; retried later
	ReconcileUntracked         = "untracked_kept"     // bytes with no metadata record: never deleted
	ReconcileError             = "error"
)

// ReconcileAction is one thing reconciliation did or decided.
type ReconcileAction struct {
	ObjectID string `json:"object_id"`
	Version  uint64 `json:"version,omitempty"`
	Action   string `json:"action"`
	Class    string `json:"class,omitempty"`
	Bytes    int64  `json:"bytes,omitempty"`
	Detail   string `json:"detail,omitempty"`
}

// ReconcileReport summarises reconciling one node against metadata.
type ReconcileReport struct {
	NodeID      string    `json:"node_id"`
	Trigger     string    `json:"trigger"`
	StartedAt   time.Time `json:"started_at"`
	CompletedAt time.Time `json:"completed_at"`
	DurationMS  int64     `json:"duration_ms"`
	Objects     int       `json:"objects"`
	// Replicas is the number of (object, version) files the node held.
	Replicas        int               `json:"replicas"`
	Verified        int               `json:"verified"`
	Adopted         int               `json:"adopted"`
	AdoptedBytes    int64             `json:"adopted_bytes"`
	Removed         map[string]int    `json:"removed_by_class"`
	RemovedReplicas int               `json:"removed_replicas"`
	ReclaimedBytes  int64             `json:"reclaimed_bytes"`
	Kept            int               `json:"kept"`
	Recorded        int               `json:"recorded_for_repair"`
	Deferred        int               `json:"deferred"`
	Untracked       int               `json:"untracked"`
	Errors          []string          `json:"errors,omitempty"`
	Actions         []ReconcileAction `json:"actions"`
}

func (r *ReconcileReport) add(a ReconcileAction) {
	r.Actions = append(r.Actions, a)
	switch a.Action {
	case ReconcileVerified:
		r.Verified++
	case ReconcileAdopted:
		r.Adopted++
		r.AdoptedBytes += a.Bytes
	case ReconcileRemoved:
		r.Removed[a.Class]++
		r.RemovedReplicas++
		r.ReclaimedBytes += a.Bytes
	case ReconcileKept:
		r.Kept++
	case ReconcileRecordedCorrupted, ReconcileRecordedMissing, ReconcileRecordedStale:
		r.Recorded++
	case ReconcileDeferred:
		r.Deferred++
	case ReconcileUntracked:
		r.Untracked++
	case ReconcileError:
		r.Errors = append(r.Errors, a.ObjectID+": "+a.Detail)
	}
}

// ReconcileNode brings one reachable node back in line with metadata, using
// the Phase 3 version and tombstone rules. It is run when a node becomes
// reachable again after a partition or outage, and on demand.
//
// For every object the node holds or is listed for, under the object lock
// and the object's repair guard (so no write, delete or repair of that
// object is in flight), against the latest record re-read under the lock:
//
//   - tombstone: every version on the node is data of a deleted object and
//     is removed — it can never resurrect the object;
//   - listed replica: re-verified like a scrub (version + checksum), and
//     recorded CORRUPTED / MISSING / STALE if it is not the current version
//     with the right bytes, which queues the existing repair;
//   - unlisted copy of the current version that verifies: adopted into the
//     replica set (a commit under the same guard as repair) if the object
//     is below RF — RF restored with no data transfer — otherwise removed as
//     an extra copy once RF verified copies exist elsewhere;
//   - unlisted copy that does not verify, older (stale) or newer
//     (uncommitted) versions: removed through the guarded removal path,
//     which keeps them if the current version has no verified copy;
//   - bytes of an object with no record at all: kept and reported.
//
// Nothing here ever makes a node's bytes authoritative on its own: the
// latest metadata record decides, and only verified copies are adopted.
func (c *Coordinator) ReconcileNode(ctx context.Context, nodeID, trigger string) (ReconcileReport, error) {
	rep := ReconcileReport{NodeID: nodeID, Trigger: trigger, StartedAt: time.Now().UTC(),
		Removed: map[string]int{}, Actions: []ReconcileAction{}}
	n, err := c.node(nodeID)
	if err != nil {
		return rep, err
	}
	if c.health.State(nodeID) != storage.StateHealthy {
		return rep, fmt.Errorf("%w: %s is not reachable; nothing to reconcile yet", storage.ErrUnavailable, nodeID)
	}
	ictx, cancel := context.WithTimeout(ctx, c.cfg.ReplicaTimeout)
	inv, err := n.Inventory(ictx)
	cancel()
	if err != nil {
		c.reportIfUnavailable(ctx, nodeID, err)
		return rep, fmt.Errorf("inventory of %s: %w", nodeID, err)
	}
	c.log.Emit(ctx, events.ReconcileStarted, "node_id", nodeID, "trigger", trigger, "replicas_held", len(inv))

	ids := map[string]bool{}
	for _, r := range inv {
		ids[r.ObjectID] = true
		rep.Replicas++
	}
	objs, err := c.meta.ListObjects(ctx)
	if err != nil {
		return rep, err
	}
	for _, o := range objs {
		if slices.Contains(o.Replicas, nodeID) {
			ids[o.ID] = true
		}
	}
	sorted := make([]string, 0, len(ids))
	for id := range ids {
		sorted = append(sorted, id)
	}
	sort.Strings(sorted)
	rep.Objects = len(sorted)
	var deferred []string
	for _, id := range sorted {
		if ctx.Err() != nil {
			break
		}
		if !c.reconcileObject(ctx, n, id, &rep) {
			deferred = append(deferred, id)
		}
	}
	// An object whose repair is running is retried for a bounded time: the
	// repair holds the guard only while it copies and commits.
	for attempt := 0; attempt < 30 && len(deferred) > 0; attempt++ {
		select {
		case <-ctx.Done():
		case <-time.After(100 * time.Millisecond):
		}
		if ctx.Err() != nil {
			break
		}
		retry := deferred
		deferred = nil
		for _, id := range retry {
			if !c.reconcileObject(ctx, n, id, &rep) {
				deferred = append(deferred, id)
			}
		}
	}
	for _, id := range deferred {
		rep.add(ReconcileAction{ObjectID: id, Action: ReconcileDeferred,
			Detail: "a repair of this object kept running; the next reconcile or scrub handles it"})
	}

	rep.CompletedAt = time.Now().UTC()
	rep.DurationMS = rep.CompletedAt.Sub(rep.StartedAt).Milliseconds()
	c.log.Emit(ctx, events.ReconcileCompleted, "node_id", nodeID, "trigger", trigger,
		"objects", rep.Objects, "replicas_held", rep.Replicas, "verified", rep.Verified,
		"adopted", rep.Adopted, "removed", rep.Removed, "reclaimed_bytes", rep.ReclaimedBytes,
		"kept", rep.Kept, "recorded_for_repair", rep.Recorded, "deferred", rep.Deferred,
		"untracked", rep.Untracked, "errors", len(rep.Errors), "duration_ms", rep.DurationMS)
	c.noteReconcile(rep)
	return rep, ctx.Err()
}

// reconcileObject reconciles one object on n. It returns false, having
// done nothing, if a repair of the object is running.
func (c *Coordinator) reconcileObject(ctx context.Context, n storage.StorageNode, id string, rep *ReconcileReport) bool {
	nodeID := n.ID()
	if !c.beginRepair(id) {
		return false
	}
	defer c.endRepair(id)
	unlock := c.locks.Lock(id)
	defer unlock()

	// Holdings re-read under the lock: the inventory may predate a write
	// that has completed since.
	lctx, cancel := context.WithTimeout(ctx, c.cfg.ReplicaTimeout)
	held, err := n.ListReplicas(lctx, id)
	cancel()
	if err != nil {
		c.reportIfUnavailable(ctx, nodeID, err)
		rep.add(ReconcileAction{ObjectID: id, Action: ReconcileError, Detail: err.Error()})
		return true
	}
	rec, err := c.meta.GetRecord(ctx, id)
	if errors.Is(err, metadata.ErrNotFound) {
		if len(held) > 0 {
			rep.add(ReconcileAction{ObjectID: id, Action: ReconcileUntracked,
				Detail: "no metadata record: bytes were never acknowledged, but are kept rather than guessed about"})
		}
		return true
	}
	if err != nil {
		rep.add(ReconcileAction{ObjectID: id, Action: ReconcileError, Detail: err.Error()})
		return true
	}
	size := func(v uint64) int64 {
		for _, h := range held {
			if h.Version == v {
				return h.Size
			}
		}
		return 0
	}
	remove := func(rec metadata.Object, v uint64, proof *removalProof) *removalProof {
		res, err := c.removeReplicaLocked(ctx, rec, nodeID, v, "partition_reconcile", proof)
		switch {
		case errors.Is(err, ErrRemovalRefused):
			rep.add(ReconcileAction{ObjectID: id, Version: v, Action: ReconcileKept, Class: res.Class, Detail: res.Refused})
		case err != nil:
			rep.add(ReconcileAction{ObjectID: id, Version: v, Action: ReconcileError, Class: res.Class, Detail: err.Error()})
		case res.Removed:
			rep.add(ReconcileAction{ObjectID: id, Version: v, Action: ReconcileRemoved, Class: res.Class, Bytes: size(v)})
			if len(res.VerifiedBy) > 0 && !rec.Deleted {
				return &removalProof{version: rec.Version, nodes: res.VerifiedBy}
			}
		}
		return proof
	}

	if rec.Deleted {
		for _, h := range held {
			remove(rec, h.Version, nil)
		}
		return true
	}

	listed := slices.Contains(rec.Replicas, nodeID)
	hasCurrent := slices.ContainsFunc(held, func(r storage.ReplicaInfo) bool { return r.Version == rec.Version })
	var proof *removalProof
	switch {
	case listed:
		f, _ := c.scrubReplica(ctx, rec, nodeID)
		a := ReconcileAction{ObjectID: id, Version: rec.Version}
		switch f.Outcome {
		case ScrubVerified:
			a.Action = ReconcileVerified
			proof = &removalProof{version: rec.Version, nodes: []string{nodeID}}
		case ScrubCorrupted:
			a.Action, a.Detail = ReconcileRecordedCorrupted, f.Error
		case ScrubMissing:
			a.Action = ReconcileRecordedMissing
		case ScrubStale:
			a.Action, a.Detail = ReconcileRecordedStale, fmt.Sprintf("holds v%d", f.ObservedVersion)
		default:
			a.Action, a.Detail = ReconcileError, f.Outcome+": "+f.Error
		}
		rep.add(a)
		if f.Outcome != ScrubVerified {
			if cur, err := c.meta.GetRecord(ctx, id); err == nil {
				rec = cur // the finding was recorded on the record
			}
		}
	case hasCurrent:
		ok, _ := c.verifyCopy(ctx, rec, nodeID)
		h := durability.Assess(rec, c.health.State)
		if ok && h.HealthyReplicas < rec.ReplicationFactor {
			committed, replaced, _, err := c.commitRepair(ctx, rec, []string{nodeID})
			if err != nil {
				rep.add(ReconcileAction{ObjectID: id, Version: rec.Version, Action: ReconcileError, Detail: "adopt: " + err.Error()})
				break
			}
			c.log.Emit(ctx, events.ReplicaAdopted, "object_id", id, "version", rec.Version, "node_id", nodeID,
				"bytes", rec.Size, "replaced", replaced, "replicas_before", rec.Replicas,
				"replicas_after", committed.Replicas, "note", "verified copy committed without data transfer")
			rep.add(ReconcileAction{ObjectID: id, Version: rec.Version, Action: ReconcileAdopted, Bytes: rec.Size,
				Detail: fmt.Sprintf("replaced %v", replaced)})
			rec = committed
			proof = &removalProof{version: rec.Version, nodes: []string{nodeID}}
			for _, r := range replaced {
				if c.health.State(r) == storage.StateHealthy {
					c.removeReplicaLocked(ctx, rec, r, rec.Version, "replaced_by_adoption", proof)
				}
			}
		} else {
			proof = remove(rec, rec.Version, nil)
		}
	}
	for _, h := range held {
		if h.Version != rec.Version {
			proof = remove(rec, h.Version, proof)
		}
	}
	return true
}

// ReconcileAll reconciles every reachable node.
func (c *Coordinator) ReconcileAll(ctx context.Context, trigger string) []ReconcileReport {
	var out []ReconcileReport
	for _, id := range c.health.Healthy() {
		rep, err := c.ReconcileNode(ctx, id, trigger)
		if err != nil {
			rep.Errors = append(rep.Errors, err.Error())
		}
		out = append(out, rep)
	}
	return out
}
