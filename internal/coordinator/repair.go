package coordinator

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"vault/internal/durability"
	"vault/internal/events"
	"vault/internal/integrity"
	"vault/internal/metadata"
	"vault/internal/placement"
	"vault/internal/repair"
	"vault/internal/storage"
)

var (
	// ErrNoVerifiedSource: no replica could be read and verified, so there
	// is nothing trustworthy to copy from.
	ErrNoVerifiedSource = errors.New("no verified source replica available")
	// ErrNoEligibleTarget: every healthy node already holds a healthy
	// replica; there is nowhere to put a new one. The object stays below
	// policy until capacity appears.
	ErrNoEligibleTarget = errors.New("repair unavailable: no eligible healthy target node")
	// ErrAllTargetsFailed: eligible targets existed but none accepted and
	// verified a replacement.
	ErrAllTargetsFailed = errors.New("every eligible repair target failed")
)

var _ repair.Executor = (*Coordinator)(nil)

// Repair implements repair.Executor.
func (c *Coordinator) Repair(ctx context.Context, t repair.Task) (repair.Result, error) {
	return c.RepairObject(ctx, t.ObjectID)
}

func (c *Coordinator) beginRepair(id string) bool {
	c.repairMu.Lock()
	defer c.repairMu.Unlock()
	if c.repairing[id] {
		return false
	}
	c.repairing[id] = true
	return true
}

func (c *Coordinator) endRepair(id string) {
	c.repairMu.Lock()
	delete(c.repairing, id)
	c.repairMu.Unlock()
}

// RepairObject restores object id toward its replication factor:
//
//  1. take the object's repair guard (one repair per object at a time);
//  2. assess the current version from metadata + node states;
//  3. read a source replica and verify its SHA-256 end to end — sources
//     that fail are recorded as corrupted and skipped;
//  4. write the verified bytes to healthy nodes that do not already hold a
//     healthy replica; each target verifies on write, then re-hashes the
//     replica from its own disk;
//  5. under the object's write lock, re-check that the version and checksum
//     are still current and only then commit the new replica set.
//
// Metadata never names a replica before its bytes are written and verified.
// If the object changed meanwhile, nothing is committed, the copies are
// deleted, and repair.ErrSuperseded is returned.
func (c *Coordinator) RepairObject(ctx context.Context, id string) (repair.Result, error) {
	res := repair.Result{ObjectID: id}
	if !c.beginRepair(id) {
		return res, repair.ErrInProgress
	}
	defer c.endRepair(id)

	obj, err := c.meta.GetObject(ctx, id)
	if errors.Is(err, metadata.ErrNotFound) {
		return res, fmt.Errorf("%w: object was deleted", repair.ErrSuperseded)
	}
	if err != nil {
		return res, err
	}
	before := durability.Assess(obj, c.health.State)
	res.Version, res.ReplicationFactor = obj.Version, obj.ReplicationFactor
	res.StateBefore, res.HealthyBefore, res.ReplicasBefore = before.State, before.HealthyReplicas, obj.Replicas
	res.StateAfter, res.HealthyAfter, res.ReplicasAfter = before.State, before.HealthyReplicas, obj.Replicas

	if before.HealthyReplicas >= obj.ReplicationFactor {
		res.NoOp, res.Restored = true, true
		return res, nil
	}
	if before.HealthyReplicas == 0 {
		return res, fmt.Errorf("%w: %s", ErrNoVerifiedSource, before.Reason)
	}
	targets := c.repairTargets(obj, before)
	if len(targets) == 0 {
		return res, fmt.Errorf("%w (healthy nodes %v all hold a healthy replica or none are free); %s",
			ErrNoEligibleTarget, c.health.Healthy(), before.Reason)
	}

	release, err := c.acquireTransfer(ctx)
	if err != nil {
		return res, err
	}
	defer release()

	spool, source, rejected := c.fetchVerifiedSource(ctx, obj, before)
	res.RejectedSources = rejected
	if spool == nil {
		return res, fmt.Errorf("%w: all candidate sources failed verification or were unreachable (%v)", ErrNoVerifiedSource, rejected)
	}
	defer spool.Close()
	res.Source = source
	c.log.Emit(ctx, events.RepairSourceSelected, "object_id", id, "version", obj.Version,
		"source", source, "sha256", obj.SHA256, "bytes", obj.Size, "rejected_sources", rejected)

	needed := obj.ReplicationFactor - before.HealthyReplicas
	var written []string
	for _, target := range targets {
		if len(written) == needed {
			break
		}
		inPlace := slices.Contains(obj.Replicas, target)
		c.log.Emit(ctx, events.RepairTargetSelected, "object_id", id, "version", obj.Version,
			"source", source, "target", target, "in_place", inPlace)
		if err := c.copyToTarget(ctx, obj, spool, source, target); err != nil {
			res.FailedTargets = append(res.FailedTargets, target)
			c.log.Warn(ctx, events.RepairTargetFailed, "object_id", id, "version", obj.Version,
				"source", source, "target", target, "error", err.Error())
			continue
		}
		written = append(written, target)
		res.BytesTransferred += obj.Size
	}
	if len(written) == 0 {
		return res, fmt.Errorf("%w: %v", ErrAllTargetsFailed, res.FailedTargets)
	}

	if hook := c.hooks.beforeRepairCommit; hook != nil {
		hook(id)
	}
	if d := c.cfg.RepairCommitDelay; d > 0 {
		c.log.Warn(ctx, events.RepairRetry, "object_id", id, "version", obj.Version,
			"note", "fault injection: pausing before metadata commit", "delay_ms", d.Milliseconds())
		select {
		case <-time.After(d):
		case <-ctx.Done():
			c.deleteVersion(ctx, id, obj.Version, notIn(written, obj.Replicas))
			return res, ctx.Err()
		}
	}
	unlock := c.locks.Lock(id)
	committed, replaced, staleFixed, err := c.commitRepair(ctx, obj, written)
	unlock()
	if err != nil {
		cleanup := written
		if !errors.Is(err, repair.ErrSuperseded) {
			// Keep in-place rewrites: those files are still the tracked
			// replicas of the current version.
			cleanup = nil
			for _, n := range written {
				if !slices.Contains(obj.Replicas, n) {
					cleanup = append(cleanup, n)
				}
			}
		}
		if left := c.deleteVersion(ctx, id, obj.Version, cleanup); len(left) > 0 {
			c.log.Warn(ctx, events.ReplicaCleanup, "object_id", id, "version", obj.Version,
				"reason", "repair_not_committed", "orphaned_on", left)
		}
		return res, err
	}
	res.Targets = written
	res.ReplicasAfter = committed.Replicas
	c.log.Emit(ctx, events.RepairMetadataCommitted, "object_id", id, "version", obj.Version,
		"source", source, "added", written, "replaced", replaced,
		"replicas_before", obj.Replicas, "replicas_after", committed.Replicas)

	// Only now — replacement written, version and checksum verified,
	// replacement committed — may the replaced replicas go, and only through
	// the guarded removal path, which re-reads the record and requires
	// verified copies elsewhere. Only specific versions are deleted (never
	// "all versions"): a newer write may already be landing on that node.
	var garbage []string
	for _, n := range replaced {
		if c.health.State(n) == storage.StateHealthy {
			garbage = append(garbage, n)
		}
	}
	var staleGarbage []staleFix
	for _, sf := range staleFixed {
		if sf.action == "replaced" && c.health.State(sf.node) == storage.StateHealthy && sf.observed > 0 && sf.observed < obj.Version {
			staleGarbage = append(staleGarbage, sf)
		}
	}
	if len(garbage) > 0 || len(staleGarbage) > 0 {
		proof := &removalProof{version: obj.Version, nodes: written}
		var removed, left []string
		unlock := c.locks.Lock(id)
		if rec, err := c.meta.GetRecord(ctx, id); err == nil {
			for _, n := range garbage {
				if _, err := c.removeReplicaLocked(ctx, rec, n, obj.Version, "replaced_by_repair", proof); err != nil {
					left = append(left, n)
				} else {
					removed = append(removed, n)
				}
			}
			for _, sf := range staleGarbage {
				c.removeReplicaLocked(ctx, rec, sf.node, sf.observed, "stale_replaced_by_repair", proof)
			}
		} else {
			left = garbage
		}
		unlock()
		if len(garbage) > 0 {
			c.log.Emit(ctx, events.ReplicaCleanup, "object_id", id, "version", obj.Version,
				"reason", "replaced_by_repair", "removed_from", removed, "retained_on", left)
		}
	}
	for _, sf := range staleFixed {
		c.log.Emit(ctx, events.StaleReplicaRepaired, "object_id", id, "version", obj.Version,
			"node_id", sf.node, "stale_version", sf.observed, "action", sf.action, "source", source)
	}

	after := durability.Assess(committed, c.health.State)
	res.StateAfter, res.HealthyAfter = after.State, after.HealthyReplicas
	res.Restored = after.HealthyReplicas >= committed.ReplicationFactor
	return res, nil
}

// repairTargets returns healthy nodes that do not hold a healthy replica,
// in rendezvous order: fresh nodes first, then live nodes whose replica is
// corrupted or missing (an in-place rewrite, used when no fresh node exists).
func (c *Coordinator) repairTargets(obj metadata.Object, h durability.ObjectHealth) []string {
	healthyHolders := h.HealthyNodes()
	var fresh, inPlace []string
	for _, n := range placement.Rank(obj.ID, c.health.Healthy()) {
		switch {
		case slices.Contains(healthyHolders, n):
		case slices.Contains(obj.Replicas, n):
			inPlace = append(inPlace, n)
		default:
			fresh = append(fresh, n)
		}
	}
	return append(fresh, inPlace...)
}

// fetchVerifiedSource reads candidate sources in metadata order until one
// yields bytes whose SHA-256 and size match the metadata. Only replicas
// currently assessed HEALTHY are candidates; a candidate that turns out to
// be corrupt is recorded (and becomes CORRUPTED) and never used.
func (c *Coordinator) fetchVerifiedSource(ctx context.Context, obj metadata.Object, h durability.ObjectHealth) (*integrity.Spooled, string, []string) {
	want := integrity.Digest{SHA256: obj.SHA256, Size: obj.Size}
	var rejected []string
	for _, n := range h.HealthyNodes() {
		spool, a := c.readOne(ctx, obj, want, n)
		if spool != nil {
			return spool, n, rejected
		}
		rejected = append(rejected, n+"="+a.Outcome)
		c.log.Warn(ctx, events.RepairSourceRejected, "object_id", obj.ID, "version", obj.Version,
			"source", n, "outcome", a.Outcome, "error", a.Error)
	}
	return nil, "", rejected
}

// copyToTarget writes the verified spool to target and then asks target to
// re-hash what it stored. Any failure removes the partial replica.
func (c *Coordinator) copyToTarget(ctx context.Context, obj metadata.Object, spool *integrity.Spooled, source, target string) error {
	start := time.Now()
	a := c.putReplica(ctx, obj.ID, obj.Version, spool, target)
	if a.Outcome != OutcomeStored {
		return fmt.Errorf("write to %s: %s: %s", target, a.Outcome, a.Error)
	}
	c.log.Emit(ctx, events.RepairReplicaWritten, "object_id", obj.ID, "version", obj.Version,
		"source", source, "target", target, "bytes", obj.Size, "duration_ms", since(start),
		"verified_by_node_on_write", true)

	vctx, cancel := context.WithTimeout(ctx, c.cfg.ReplicaTimeout)
	var vr storage.VerifyResult
	n, err := c.node(target)
	if err == nil {
		vr, err = n.VerifyReplica(vctx, obj.ID, obj.Version)
	}
	cancel()
	want := integrity.Digest{SHA256: obj.SHA256, Size: obj.Size}
	if err == nil && (vr.ObjectID != obj.ID || vr.Version != obj.Version) {
		// The node's own record must name exactly the version being repaired.
		err = fmt.Errorf("version check: node record names %s v%d, expected %s v%d", vr.ObjectID, vr.Version, obj.ID, obj.Version)
	}
	if err == nil {
		err = integrity.Verify(want, vr.Actual)
	}
	if err != nil {
		c.deleteVersion(ctx, obj.ID, obj.Version, []string{target})
		return fmt.Errorf("post-write verification on %s failed: %w", target, err)
	}
	c.log.Emit(ctx, events.RepairChecksumVerified, "object_id", obj.ID, "version", obj.Version,
		"source", source, "target", target, "sha256", vr.Actual.SHA256, "bytes", vr.Actual.Size,
		"version_verified", true)
	return nil
}

type staleFix struct {
	node     string
	observed uint64
	action   string // "rewritten" in place, or "replaced" by another node
}

func notIn(nodes, exclude []string) []string {
	var out []string
	for _, n := range nodes {
		if !slices.Contains(exclude, n) {
			out = append(out, n)
		}
	}
	return out
}

// commitRepair atomically installs the verified targets into the replica
// set of obj's version, if that version is still current (and the object is
// still live: a tombstone reads as not found). Each new target replaces one
// unhealthy entry (corrupted, stale or missing first, then unavailable). It
// returns the committed object, the entries that were replaced, and which
// stale replicas were fixed.
func (c *Coordinator) commitRepair(ctx context.Context, obj metadata.Object, written []string) (metadata.Object, []string, []staleFix, error) {
	now := time.Now().UTC()
	var (
		replaced []string
		fixed    []staleFix
	)
	committed, err := c.meta.Modify(ctx, obj.ID, func(cur metadata.Object) (metadata.Object, error) {
		if cur.Version != obj.Version || cur.SHA256 != obj.SHA256 {
			return cur, fmt.Errorf("%w: object is now v%d, repair copied v%d", repair.ErrSuperseded, cur.Version, obj.Version)
		}
		h := durability.Assess(cur, c.health.State)
		var bad []string
		for _, st := range []durability.ReplicaState{durability.ReplicaCorrupted, durability.ReplicaStale,
			durability.ReplicaMissing, durability.ReplicaUnavailable} {
			for _, r := range h.Replicas {
				if r.State == st && !slices.Contains(written, r.NodeID) {
					bad = append(bad, r.NodeID)
				}
			}
		}
		staleVersion := func(n string) (uint64, bool) {
			f, ok := cur.FailureFor(n)
			return f.ObservedVersion, ok && f.Reason == metadata.ReasonStaleVersion
		}
		set := slices.Clone(cur.Replicas)
		replaced, fixed = nil, nil
		// In-place rewrites first: the node stays, its failure record goes.
		for _, t := range written {
			if slices.Contains(set, t) {
				if v, stale := staleVersion(t); stale {
					fixed = append(fixed, staleFix{node: t, observed: v, action: "rewritten"})
				}
				cur.IntegrityFailures = metadata.WithoutFailure(cur.IntegrityFailures, t, cur.Version)
			}
		}
		for _, t := range written {
			if slices.Contains(set, t) {
				continue
			}
			if len(bad) > 0 {
				r := bad[0]
				bad = bad[1:]
				if v, stale := staleVersion(r); stale {
					fixed = append(fixed, staleFix{node: r, observed: v, action: "replaced"})
				}
				set = slices.DeleteFunc(set, func(n string) bool { return n == r })
				cur.IntegrityFailures = metadata.WithoutFailure(cur.IntegrityFailures, r, cur.Version)
				replaced = append(replaced, r)
			}
			set = append(set, t)
		}
		cur.Replicas = set
		cur.LastRepairedAt = &now
		return cur, nil
	})
	if errors.Is(err, metadata.ErrNotFound) {
		err = fmt.Errorf("%w: object was deleted during repair", repair.ErrSuperseded)
	}
	return committed, replaced, fixed, err
}
