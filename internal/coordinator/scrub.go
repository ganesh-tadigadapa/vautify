package coordinator

import (
	"context"
	"errors"
	"sync"
	"time"

	"vault/internal/events"
	"vault/internal/integrity"
	"vault/internal/metadata"
	"vault/internal/storage"
)

// Scrub outcomes per replica.
const (
	ScrubVerified    = "verified"
	ScrubCorrupted   = "corrupted"
	ScrubMissing     = "missing"
	ScrubStale       = "stale"
	ScrubSkipped     = "skipped_node_unavailable"
	ScrubErrorResult = "error"
)

// ScrubFinding is the result of verifying one replica. Only non-verified
// findings are listed in reports.
type ScrubFinding struct {
	ObjectID        string `json:"object_id"`
	Version         uint64 `json:"version"`
	NodeID          string `json:"node_id"`
	Outcome         string `json:"outcome"`
	ObservedVersion uint64 `json:"observed_version,omitempty"`
	ExpectedSHA256  string `json:"expected_sha256,omitempty"`
	ObservedSHA256  string `json:"observed_sha256,omitempty"`
	ExpectedSize    int64  `json:"expected_size"`
	ObservedSize    int64  `json:"observed_size"`
	Error           string `json:"error,omitempty"`
}

// ScrubReport summarises one full scrub pass.
type ScrubReport struct {
	StartedAt          time.Time      `json:"started_at"`
	CompletedAt        time.Time      `json:"completed_at"`
	DurationMS         int64          `json:"duration_ms"`
	Concurrency        int            `json:"concurrency"`
	Objects            int            `json:"objects"`
	ReplicasChecked    int            `json:"replicas_checked"`
	Verified           int            `json:"verified"`
	Corrupted          int            `json:"corrupted"`
	Missing            int            `json:"missing"`
	SkippedUnavailable int            `json:"skipped_unavailable"`
	Errors             int            `json:"errors"`
	ClearedFailures    int            `json:"cleared_failures"`
	Stale              int            `json:"stale"`
	Findings           []ScrubFinding `json:"findings"`
	// Inventory phase: every node's stored versions compared with metadata.
	StaleVersionsPurged int      `json:"stale_versions_purged"`
	StalePurgeFailed    int      `json:"stale_purge_failed"`
	UntrackedObjects    int      `json:"untracked_objects"`
	InventoryErrors     []string `json:"inventory_errors,omitempty"`
}

type scrubTask struct {
	obj    metadata.Object
	nodeID string
}

// Scrub verifies every replica of every object's current version. Each node
// re-hashes its own file and reports the digest; the coordinator compares it
// with the SHA-256 and size in metadata — never with the node's own record.
// No object bytes are transferred. At most concurrency verifications run at
// once. Mismatches and missing replicas are recorded as integrity failures
// (which marks the replica CORRUPTED/MISSING); a replica that verifies
// cleanly has any stale failure record for it cleared.
func (c *Coordinator) Scrub(ctx context.Context, concurrency int) (ScrubReport, error) {
	if concurrency < 1 {
		concurrency = 1
	}
	rep := ScrubReport{StartedAt: time.Now().UTC(), Concurrency: concurrency, Findings: []ScrubFinding{}}
	objs, err := c.meta.ListObjects(ctx)
	if err != nil {
		return rep, err
	}
	rep.Objects = len(objs)
	c.log.Emit(ctx, events.ScrubStarted, "objects", len(objs), "concurrency", concurrency)

	tasks := make(chan scrubTask)
	var (
		mu sync.Mutex
		wg sync.WaitGroup
	)
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for t := range tasks {
				f, cleared := c.scrubReplica(ctx, t.obj, t.nodeID)
				mu.Lock()
				rep.ReplicasChecked++
				switch f.Outcome {
				case ScrubVerified:
					rep.Verified++
				case ScrubCorrupted:
					rep.Corrupted++
				case ScrubMissing:
					rep.Missing++
				case ScrubStale:
					rep.Stale++
				case ScrubSkipped:
					rep.SkippedUnavailable++
				default:
					rep.Errors++
				}
				if cleared {
					rep.ClearedFailures++
				}
				if f.Outcome != ScrubVerified {
					rep.Findings = append(rep.Findings, f)
				}
				mu.Unlock()
			}
		}()
	}
feed:
	for _, obj := range objs {
		for _, n := range obj.Replicas {
			select {
			case tasks <- scrubTask{obj: obj, nodeID: n}:
			case <-ctx.Done():
				break feed
			}
		}
	}
	close(tasks)
	wg.Wait()

	c.purgeStaleVersions(ctx, &rep)

	rep.CompletedAt = time.Now().UTC()
	rep.DurationMS = rep.CompletedAt.Sub(rep.StartedAt).Milliseconds()
	c.log.Emit(ctx, events.ScrubCompleted, "objects", rep.Objects, "replicas_checked", rep.ReplicasChecked,
		"verified", rep.Verified, "corrupted", rep.Corrupted, "missing", rep.Missing,
		"skipped_unavailable", rep.SkippedUnavailable, "errors", rep.Errors, "stale", rep.Stale,
		"stale_versions_purged", rep.StaleVersionsPurged, "duration_ms", rep.DurationMS)
	return rep, ctx.Err()
}

func (c *Coordinator) scrubReplica(ctx context.Context, obj metadata.Object, nodeID string) (ScrubFinding, bool) {
	want := integrity.Digest{SHA256: obj.SHA256, Size: obj.Size}
	f := ScrubFinding{ObjectID: obj.ID, Version: obj.Version, NodeID: nodeID,
		ExpectedSHA256: obj.SHA256, ExpectedSize: obj.Size}
	if c.health.State(nodeID) != storage.StateHealthy {
		f.Outcome = ScrubSkipped
		return f, false
	}
	vctx, cancel := context.WithTimeout(ctx, c.cfg.ReplicaTimeout)
	var vr storage.VerifyResult
	n, err := c.node(nodeID)
	if err == nil {
		vr, err = n.VerifyReplica(vctx, obj.ID, obj.Version)
	}
	cancel()
	switch {
	case errors.Is(err, storage.ErrNotFound):
		if older := c.newestOlderVersion(ctx, n, obj); older > 0 {
			f.Outcome, f.ObservedVersion = ScrubStale, older
			c.log.Warn(ctx, events.StaleReplicaDetected, "object_id", obj.ID, "version", obj.Version,
				"node_id", nodeID, "stale_version", older, "listed_replica", true, "node_alive", true)
			c.recordFailure(ctx, obj, nodeID, metadata.ReasonStaleVersion, integrity.Digest{}, older)
			return f, false
		}
		f.Outcome = ScrubMissing
		c.log.Warn(ctx, events.ScrubReplicaMissing, "object_id", obj.ID, "version", obj.Version,
			"node_id", nodeID, "node_alive", true)
		c.recordFailure(ctx, obj, nodeID, metadata.ReasonReplicaMissing, integrity.Digest{}, 0)
		return f, false
	case err != nil:
		c.reportIfUnavailable(ctx, nodeID, err)
		f.Outcome, f.Error = ScrubErrorResult, err.Error()
		return f, false
	}
	f.ObservedSHA256, f.ObservedSize = vr.Actual.SHA256, vr.Actual.Size
	if verr := integrity.Verify(want, vr.Actual); verr != nil {
		f.Outcome, f.Error = ScrubCorrupted, verr.Error()
		reason := metadata.ReasonChecksumMismatch
		if vr.Actual.Size != want.Size {
			reason = metadata.ReasonSizeMismatch
		}
		c.log.Warn(ctx, events.ScrubCorruptionDetected, "object_id", obj.ID, "version", obj.Version,
			"node_id", nodeID, "node_alive", true, "reason", reason,
			"expected_sha256", want.SHA256, "actual_sha256", vr.Actual.SHA256,
			"expected_size", want.Size, "actual_size", vr.Actual.Size)
		c.log.Warn(ctx, events.ChecksumMismatch, "stage", "scrub", "object_id", obj.ID, "version", obj.Version,
			"node_id", nodeID, "reason", reason, "node_alive", true,
			"expected_sha256", want.SHA256, "actual_sha256", vr.Actual.SHA256)
		c.recordFailure(ctx, obj, nodeID, reason, vr.Actual, 0)
		return f, false
	}
	f.Outcome = ScrubVerified
	cleared := false
	if _, had := obj.FailureFor(nodeID); had {
		_, err := c.meta.Modify(ctx, obj.ID, func(cur metadata.Object) (metadata.Object, error) {
			if cur.Version != obj.Version {
				return cur, metadata.ErrVersionConflict
			}
			cur.IntegrityFailures = metadata.WithoutFailure(cur.IntegrityFailures, nodeID, cur.Version)
			return cur, nil
		})
		cleared = err == nil
	}
	c.log.Emit(ctx, events.ScrubReplicaVerified, "object_id", obj.ID, "version", obj.Version,
		"node_id", nodeID, "sha256", vr.Actual.SHA256, "cleared_failure", cleared)
	return f, cleared
}

// newestOlderVersion returns the newest version older than obj's current
// version that node holds, or 0.
func (c *Coordinator) newestOlderVersion(ctx context.Context, n storage.StorageNode, obj metadata.Object) uint64 {
	lctx, cancel := context.WithTimeout(ctx, c.cfg.ReplicaTimeout)
	defer cancel()
	reps, err := n.ListReplicas(lctx, obj.ID)
	if err != nil {
		return 0
	}
	var newest uint64
	for _, r := range reps {
		if r.Version < obj.Version && r.Version > newest {
			newest = r.Version
		}
	}
	return newest
}

// purgeStaleVersions compares every reachable node's inventory with the
// metadata records (live objects AND tombstones) and deletes every stored
// version older than the object's latest record: superseded versions left
// behind on a node that was down during an overwrite, and any data of a
// deleted object. Such bytes are never served — reads only fetch the
// current version from listed replicas — and purging them means they can
// never be mistaken for, or resurrect, the object.
//
// Versions equal to or newer than the record are never touched: they may
// belong to a write or repair that is in flight. Objects with no record at
// all are counted, not deleted, for the same reason.
func (c *Coordinator) purgeStaleVersions(ctx context.Context, rep *ScrubReport) {
	records, err := c.meta.ListRecords(ctx)
	if err != nil {
		rep.InventoryErrors = append(rep.InventoryErrors, err.Error())
		return
	}
	byID := make(map[string]metadata.Object, len(records))
	for _, r := range records {
		byID[r.ID] = r
	}
	proofs := map[string]*removalProof{}
	for _, nodeID := range c.health.Healthy() {
		n, err := c.node(nodeID)
		if err != nil {
			continue
		}
		ictx, cancel := context.WithTimeout(ctx, c.cfg.ReplicaTimeout)
		inv, err := n.Inventory(ictx)
		cancel()
		if err != nil {
			c.reportIfUnavailable(ctx, nodeID, err)
			rep.InventoryErrors = append(rep.InventoryErrors, nodeID+": "+err.Error())
			continue
		}
		untracked := map[string]bool{}
		for _, r := range inv {
			rec, ok := byID[r.ObjectID]
			if !ok {
				untracked[r.ObjectID] = true
				continue
			}
			if r.Version >= rec.Version {
				continue
			}
			c.log.Warn(ctx, events.StaleReplicaDetected, "object_id", r.ObjectID, "node_id", nodeID,
				"stale_version", r.Version, "current_version", rec.Version, "object_deleted", rec.Deleted,
				"listed_replica", false)
			// Phase 4: the purge goes through the guarded removal path, under
			// the object lock, against the record as it is now: a stale
			// version is removed only while a verified copy of the current
			// version exists elsewhere (data of a deleted object: always).
			unlock := c.locks.Lock(r.ObjectID)
			cur, err := c.meta.GetRecord(ctx, r.ObjectID)
			if err == nil && r.Version < cur.Version {
				dctx, cancel := context.WithTimeout(ctx, c.cfg.ReplicaTimeout)
				var res RemovalResult
				res, err = c.removeReplicaLocked(dctx, cur, nodeID, r.Version, "scrub_purge", proofs[r.ObjectID])
				cancel()
				if err == nil && len(res.VerifiedBy) > 0 {
					// Reuse this pass's verification for the object's other
					// stale copies; it only counts while the version is current.
					proofs[r.ObjectID] = &removalProof{version: cur.Version, nodes: res.VerifiedBy}
				}
			}
			unlock()
			if err != nil {
				rep.StalePurgeFailed++
				continue
			}
			rep.StaleVersionsPurged++
			c.log.Emit(ctx, events.StaleReplicaRepaired, "object_id", r.ObjectID, "node_id", nodeID,
				"stale_version", r.Version, "current_version", rec.Version, "object_deleted", rec.Deleted,
				"action", "purged")
		}
		rep.UntrackedObjects += len(untracked)
	}
}
