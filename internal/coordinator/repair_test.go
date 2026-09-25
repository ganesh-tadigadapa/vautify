package coordinator

import (
	"bytes"
	"context"
	"errors"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"

	"vault/internal/durability"
	"vault/internal/metadata"
	"vault/internal/node"
	"vault/internal/placement"
	"vault/internal/repair"
	"vault/internal/storage"
)

func (h *harness) assess(id string) durability.ObjectHealth {
	h.t.Helper()
	obj, err := h.meta.GetObject(context.Background(), id)
	if err != nil {
		h.t.Fatal(err)
	}
	return durability.Assess(obj, h.checker.State)
}

func (h *harness) mustHold(nodeID, id string, version uint64, want []byte) {
	h.t.Helper()
	b, err := h.replicaBytes(nodeID, id, version)
	if err != nil || !bytes.Equal(b, want) {
		h.t.Fatalf("%s does not hold correct v%d of %s (err=%v)", nodeID, version, id, err)
	}
}

func TestRepairAfterNodeFailureRestoresRF(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 4, 3)
	data := payload(70_000, 1)
	put, _ := h.coord.Put(ctx, "obj", bytes.NewReader(data), 0)
	dead := put.Object.Replicas[1]
	h.kill(dead, true)
	if st := h.assess("obj").State; st != durability.Degraded {
		t.Fatalf("state after node loss = %s", st)
	}

	res, err := h.coord.RepairObject(ctx, "obj")
	if err != nil {
		t.Fatal(err)
	}
	if !res.Restored || res.HealthyBefore != 2 || res.HealthyAfter != 3 || len(res.Targets) != 1 || res.BytesTransferred != int64(len(data)) {
		t.Fatalf("result %+v", res)
	}
	target := res.Targets[0]
	if slices.Contains(put.Object.Replicas, target) || target == dead {
		t.Fatalf("target %s is not a fresh node", target)
	}
	h.mustHold(target, "obj", 1, data)
	obj, _ := h.meta.GetObject(ctx, "obj")
	if slices.Contains(obj.Replicas, dead) || !slices.Contains(obj.Replicas, target) || len(obj.Replicas) != 3 || obj.LastRepairedAt == nil {
		t.Fatalf("committed metadata %+v", obj)
	}
	if obj.Version != 1 || !obj.UpdatedAt.Equal(put.Object.UpdatedAt) {
		t.Fatal("repair must not change version or content timestamp")
	}
	if st := h.assess("obj").State; st != durability.Healthy {
		t.Fatalf("state after repair = %s", st)
	}
	// Idempotent: a second repair does nothing.
	res, err = h.coord.RepairObject(ctx, "obj")
	if err != nil || !res.NoOp {
		t.Fatalf("second repair %+v %v", res, err)
	}
}

// An undetected corrupt replica that looks healthy is tried as a source,
// fails verification, is recorded, and is never used.
func TestRepairNeverCopiesFromCorruptSource(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 4, 3)
	data := payload(50_000, 2)
	put, _ := h.coord.Put(ctx, "obj", bytes.NewReader(data), 0)
	reps := put.Object.Replicas
	h.corrupt(reps[0], "obj", 1) // silent: nothing recorded yet
	h.kill(reps[2], true)

	res, err := h.coord.RepairObject(ctx, "obj")
	if err != nil {
		t.Fatal(err)
	}
	if res.Source != reps[1] || len(res.RejectedSources) != 1 || res.RejectedSources[0] != reps[0]+"=checksum_mismatch" {
		t.Fatalf("source %s rejected %v", res.Source, res.RejectedSources)
	}
	for _, target := range res.Targets {
		h.mustHold(target, "obj", 1, data)
	}
	// It turned out only one verified replica existed, so one copy leaves
	// the object still below policy — reported honestly.
	if res.Restored || res.HealthyAfter != 2 {
		t.Fatalf("result %+v", res)
	}
	res, err = h.coord.RepairObject(ctx, "obj")
	if err != nil || !res.Restored {
		t.Fatalf("follow-up repair %+v %v", res, err)
	}
	obj, _ := h.meta.GetObject(ctx, "obj")
	for _, n := range obj.Replicas {
		h.mustHold(n, "obj", 1, data)
	}
}

func TestRepairRewritesInPlaceWhenNoFreshNode(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 3, 3)
	data := payload(30_000, 3)
	put, _ := h.coord.Put(ctx, "obj", bytes.NewReader(data), 0)
	victim := put.Object.Replicas[0]
	h.corrupt(victim, "obj", 1)
	rep, err := h.coord.Scrub(ctx, 2)
	if err != nil || rep.Corrupted != 1 || rep.Verified != 2 {
		t.Fatalf("scrub %+v %v", rep, err)
	}
	r, _ := h.assess("obj").Replica(victim)
	if r.NodeState != storage.StateHealthy || r.State != durability.ReplicaCorrupted {
		t.Fatalf("victim replica %+v", r)
	}

	res, err := h.coord.RepairObject(ctx, "obj")
	if err != nil || !res.Restored || !slices.Equal(res.Targets, []string{victim}) || res.Source == victim {
		t.Fatalf("result %+v %v", res, err)
	}
	h.mustHold(victim, "obj", 1, data)
	obj, _ := h.meta.GetObject(ctx, "obj")
	if len(obj.IntegrityFailures) != 0 || !slices.Equal(obj.Replicas, put.Object.Replicas) {
		t.Fatalf("metadata %+v", obj)
	}
}

func TestRepairRetriesNextTargetWhenOneFails(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 5, 3)
	data := payload(10_000, 4)
	put, _ := h.coord.Put(ctx, "obj", bytes.NewReader(data), 0)
	h.kill(put.Object.Replicas[0], true)

	var fresh []string
	for _, n := range placement.Rank("obj", h.checker.Healthy()) {
		if !slices.Contains(put.Object.Replicas, n) {
			fresh = append(fresh, n)
		}
	}
	h.kill(fresh[0], false) // crashed, but health checker still says HEALTHY

	res, err := h.coord.RepairObject(ctx, "obj")
	if err != nil {
		t.Fatal(err)
	}
	if !res.Restored || !slices.Equal(res.FailedTargets, []string{fresh[0]}) || !slices.Equal(res.Targets, []string{fresh[1]}) {
		t.Fatalf("result %+v", res)
	}
	h.mustHold(fresh[1], "obj", 1, data)
}

func TestRepairWithoutCapacityIsHonest(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 3, 3)
	put, _ := h.coord.Put(ctx, "obj", bytes.NewReader([]byte("x")), 0)
	h.kill(put.Object.Replicas[0], true)
	res, err := h.coord.RepairObject(ctx, "obj")
	if !errors.Is(err, ErrNoEligibleTarget) || res.Restored || len(res.Targets) != 0 {
		t.Fatalf("res %+v err %v", res, err)
	}
	obj, _ := h.meta.GetObject(ctx, "obj")
	if !slices.Equal(obj.Replicas, put.Object.Replicas) || obj.LastRepairedAt != nil {
		t.Fatal("metadata changed without a repair")
	}
	if h.assess("obj").State != durability.Degraded {
		t.Fatal("object must remain DEGRADED")
	}
}

func TestRepairLostObjectHasNoSource(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 4, 1)
	put, _ := h.coord.Put(ctx, "obj", bytes.NewReader([]byte("x")), 1)
	h.kill(put.Object.Replicas[0], true)
	if _, err := h.coord.RepairObject(ctx, "obj"); !errors.Is(err, ErrNoVerifiedSource) {
		t.Fatalf("err = %v", err)
	}
}

// HARD GATE H (deterministic): a new version written while a repair is
// copying the old one must win; the repair installs nothing.
func TestRepairDoesNotInstallStaleVersion(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 4, 3)
	v1 := payload(20_000, 5)
	put, _ := h.coord.Put(ctx, "obj", bytes.NewReader(v1), 0)
	h.kill(put.Object.Replicas[0], true)

	v2 := payload(21_000, 6)
	var v2res PutResult
	h.coord.hooks.beforeRepairCommit = func(string) {
		var err error
		if v2res, err = h.coord.Put(ctx, "obj", bytes.NewReader(v2), 0); err != nil {
			t.Errorf("concurrent put: %v", err)
		}
	}
	res, err := h.coord.RepairObject(ctx, "obj")
	h.coord.hooks.beforeRepairCommit = nil
	if !errors.Is(err, repair.ErrSuperseded) {
		t.Fatalf("err = %v, want ErrSuperseded", err)
	}
	obj, _ := h.meta.GetObject(ctx, "obj")
	if obj.Version != 2 || obj.SHA256 != shaHex(v2) || !slices.Equal(obj.Replicas, v2res.Object.Replicas) {
		t.Fatalf("metadata %+v", obj)
	}
	for id := range h.roots {
		if _, err := h.replicaBytes(id, "obj", 1); err == nil && h.checker.State(id) == storage.StateHealthy {
			t.Fatalf("%s still holds stale v1 bytes (repair targets %v)", id, res.Targets)
		}
	}
	r, err := h.coord.Get(ctx, "obj", ReadOptions{})
	if err != nil || !bytes.Equal(readAll(t, r), v2) {
		t.Fatalf("read after race: %v", err)
	}
}

func TestRepairOfDeletedObjectIsSuperseded(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 4, 3)
	put, _ := h.coord.Put(ctx, "obj", bytes.NewReader([]byte("bye")), 0)
	h.kill(put.Object.Replicas[0], true)
	h.coord.hooks.beforeRepairCommit = func(string) { h.coord.Delete(ctx, "obj") }
	_, err := h.coord.RepairObject(ctx, "obj")
	if !errors.Is(err, repair.ErrSuperseded) {
		t.Fatalf("err = %v", err)
	}
	for id := range h.roots {
		if _, err := h.replicaBytes(id, "obj", 1); err == nil && h.checker.State(id) == storage.StateHealthy {
			t.Fatalf("%s holds bytes of a deleted object", id)
		}
	}
	rec, err := h.meta.GetRecord(ctx, "obj")
	if err != nil || !rec.Deleted || rec.Version != 2 || len(rec.Replicas) != 0 {
		t.Fatalf("tombstone after delete-during-repair: %+v %v", rec, err)
	}
}

func TestSameObjectRepairIsExclusive(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 4, 3)
	put, _ := h.coord.Put(ctx, "obj", bytes.NewReader([]byte("one at a time")), 0)
	h.kill(put.Object.Replicas[0], true)

	inside, release := make(chan struct{}), make(chan struct{})
	h.coord.hooks.beforeRepairCommit = func(string) { close(inside); <-release }
	var wg sync.WaitGroup
	wg.Add(1)
	var firstErr error
	go func() { defer wg.Done(); _, firstErr = h.coord.RepairObject(ctx, "obj") }()
	<-inside
	if _, err := h.coord.RepairObject(ctx, "obj"); !errors.Is(err, repair.ErrInProgress) {
		t.Fatalf("second concurrent repair err = %v", err)
	}
	close(release)
	wg.Wait()
	h.coord.hooks.beforeRepairCommit = nil
	if firstErr != nil {
		t.Fatal(firstErr)
	}
}

func TestScrubFindsCorruptionAndMissingAndClearsStaleRecords(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 3, 3)
	a, _ := h.coord.Put(ctx, "a", bytes.NewReader(payload(5000, 1)), 0)
	b, _ := h.coord.Put(ctx, "b", bytes.NewReader(payload(5000, 2)), 0)
	h.corrupt(a.Object.Replicas[1], "a", 1)
	os.Remove(node.ReplicaDataPath(h.roots[b.Object.Replicas[2]], "b", 1))

	rep, err := h.coord.Scrub(ctx, 3)
	if err != nil {
		t.Fatal(err)
	}
	if rep.ReplicasChecked != 6 || rep.Verified != 4 || rep.Corrupted != 1 || rep.Missing != 1 || len(rep.Findings) != 2 {
		t.Fatalf("report %+v", rep)
	}
	if r, _ := h.assess("a").Replica(a.Object.Replicas[1]); r.State != durability.ReplicaCorrupted || r.NodeState != storage.StateHealthy {
		t.Fatalf("a: %+v", r)
	}
	if r, _ := h.assess("b").Replica(b.Object.Replicas[2]); r.State != durability.ReplicaMissing {
		t.Fatalf("b: %+v", r)
	}
	if !strings.Contains(h.logs.String(), `"event":"SCRUB_CORRUPTION_DETECTED"`) {
		t.Fatal("no SCRUB_CORRUPTION_DETECTED event")
	}

	// A failure record for a replica that now verifies is cleared.
	good := a.Object.Replicas[0]
	h.meta.RecordIntegrityFailure(ctx, "a", metadata.IntegrityFailure{NodeID: good, Version: 1, Reason: metadata.ReasonChecksumMismatch})
	rep, _ = h.coord.Scrub(ctx, 1)
	if rep.ClearedFailures != 1 {
		t.Fatalf("cleared = %d", rep.ClearedFailures)
	}
	if r, _ := h.assess("a").Replica(good); r.State != durability.ReplicaCurrent {
		t.Fatalf("stale record not cleared: %+v", r)
	}

	// Unavailable nodes are skipped, not reported as corrupt.
	h.kill(a.Object.Replicas[0], true)
	rep, _ = h.coord.Scrub(ctx, 2)
	if rep.SkippedUnavailable != 2 {
		t.Fatalf("skipped = %d", rep.SkippedUnavailable)
	}
}

func TestChaosCorruptAndKill(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 3, 2)
	data := payload(4096, 8)
	put, _ := h.coord.Put(ctx, "obj", bytes.NewReader(data), 0)
	victim := put.Object.Replicas[0]

	res, err := h.coord.CorruptReplica(ctx, "obj", victim)
	if err != nil {
		t.Fatal(err)
	}
	onDisk, _ := h.replicaBytes(victim, "obj", 1)
	if res.Before.SHA256 != shaHex(data) || res.After.SHA256 != shaHex(onDisk) || res.After.SHA256 == res.Before.SHA256 || len(onDisk) != len(data) {
		t.Fatalf("corrupt result %+v", res)
	}
	obj, _ := h.meta.GetObject(ctx, "obj")
	if len(obj.IntegrityFailures) != 0 || h.checker.State(victim) != storage.StateHealthy {
		t.Fatal("chaos corruption must be silent: no metadata change, node stays healthy")
	}
	var notReplica string
	for id := range h.roots {
		if !slices.Contains(put.Object.Replicas, id) {
			notReplica = id
		}
	}
	if _, err := h.coord.CorruptReplica(ctx, "obj", notReplica); !errors.Is(err, ErrNotAReplica) {
		t.Fatalf("err = %v", err)
	}
	if _, err := h.coord.KillNode(ctx, "node-9"); !errors.Is(err, ErrUnknownNode) {
		t.Fatalf("err = %v", err)
	}

	kr, err := h.coord.KillNode(ctx, victim)
	if err != nil || !kr.ConfirmedUnavailable {
		t.Fatalf("kill %+v %v", kr, err)
	}
	if h.checker.State(victim) != storage.StateUnavailable {
		t.Fatal("killed node not unavailable")
	}
}

// Metadata may name nodes this coordinator no longer knows (reconfigured
// cluster). They must be treated as unavailable, never dereferenced.
func TestUnconfiguredNodeInMetadataIsUnavailable(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 3, 3)
	data := []byte("reconfigured")
	put, _ := h.coord.Put(ctx, "obj", bytes.NewReader(data), 0)
	_, err := h.meta.Modify(ctx, "obj", func(o metadata.Object) (metadata.Object, error) {
		o.Replicas = append([]string{"node-gone"}, o.Replicas[1:]...)
		return o, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	r, err := h.coord.Get(ctx, "obj", ReadOptions{PreferNode: "node-gone"})
	if err != nil || !bytes.Equal(readAll(t, r), data) || r.Attempts[0].Outcome != OutcomeUnavailable {
		t.Fatalf("get: %v %+v", err, r)
	}
	if st := h.assess("obj"); st.State != durability.Degraded {
		t.Fatalf("state %s", st.State)
	}
	if _, err := h.coord.Scrub(ctx, 2); err != nil {
		t.Fatal(err)
	}
	res, err := h.coord.Delete(ctx, "obj")
	if err != nil || !slices.Contains(res.Unreachable, "node-gone") || res.FullyPurged {
		t.Fatalf("delete %+v %v", res, err)
	}
	_ = put
}
