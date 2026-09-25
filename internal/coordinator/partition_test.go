package coordinator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"vault/internal/durability"
	"vault/internal/integrity"
	"vault/internal/metadata"
	"vault/internal/netchaos"
	"vault/internal/storage"
)

func q(n int) *Quorum { return &Quorum{N: n} }

// partition cuts the coordinator→node link and lets the real health probe
// (through the same transport) observe it.
func (h *harness) partition(id string) {
	h.t.Helper()
	if err := h.net.Partition(id, 0); err != nil {
		h.t.Fatal(err)
	}
	h.checker.ProbeAll(context.Background())
	if st := h.checker.State(id); st != storage.StateUnavailable {
		h.t.Fatalf("%s is %s after partition", id, st)
	}
}

func (h *harness) heal(id string) {
	h.t.Helper()
	if err := h.net.Heal(id); err != nil {
		h.t.Fatal(err)
	}
	h.checker.ProbeAll(context.Background())
	if st := h.checker.State(id); st != storage.StateHealthy {
		h.t.Fatalf("%s is %s after heal", id, st)
	}
}

func (h *harness) put(id string, data []byte, opts PutOptions) PutResult {
	h.t.Helper()
	res, err := h.coord.PutWithOptions(context.Background(), id, bytes.NewReader(data), opts)
	if err != nil {
		h.t.Fatalf("put %s: %v", id, err)
	}
	return res
}

// Partition != node failure: the partitioned node's process keeps serving
// anyone else, only the coordinator's link is cut — and every kind of
// coordinator request (probe, write, read, verify, delete) sees the cut.
// Latency delays every request on the link.
func TestPartitionCutsEveryCoordinatorRequestButNodeStaysAlive(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 3, 3)
	h.put("obj", payload(4096, 1), PutOptions{})

	h.partition("node-2")
	st := h.checker.Snapshot()[1]
	if !strings.Contains(st.LastError, "network partition (injected)") {
		t.Fatalf("probe error does not show the partition: %q", st.LastError)
	}
	// The node process is alive: a client that is not the coordinator
	// reaches it directly and it still holds its replica.
	resp, err := http.Get(h.servers["node-2"].URL + "/health")
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("node-2 itself should be up: %v", err)
	}
	resp.Body.Close()
	h.mustHold("node-2", "obj", 1, payload(4096, 1))

	n2 := h.coord.nodes["node-2"]
	if _, err := n2.VerifyReplica(ctx, "obj", 1); !errors.Is(err, storage.ErrUnavailable) {
		t.Fatalf("verify through a cut link: %v", err)
	}
	if err := n2.DeleteReplica(ctx, "obj", 1); !errors.Is(err, storage.ErrUnavailable) {
		t.Fatalf("delete through a cut link: %v", err)
	}
	if _, err := h.coord.Put(ctx, "new", bytes.NewReader([]byte("x")), 3); err == nil {
		t.Fatal("RF=3 write succeeded with a partitioned node")
	}
	r, err := h.coord.Get(ctx, "obj", ReadOptions{})
	if err != nil || r.ServedBy == "node-2" {
		t.Fatalf("read: %v", err)
	}
	readAll(t, r)
	h.mustHold("node-2", "obj", 1, payload(4096, 1)) // the cut delete never arrived

	h.heal("node-2")
	if err := h.net.SetLatency("node-3", 150*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if _, err := h.coord.nodes["node-3"].Health(ctx); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d < 150*time.Millisecond {
		t.Fatalf("latency not applied: %s", d)
	}
	if l, _ := h.net.Link("node-3"); l.LatencyMS != 150 || l.RequestsDelayed == 0 {
		t.Fatalf("link %+v", l)
	}
}

func TestTemporaryPartitionHealsByItself(t *testing.T) {
	h := newHarness(t, 3, 3)
	if err := h.net.Partition("node-1", 150*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	h.checker.ProbeAll(context.Background())
	if h.checker.State("node-1") != storage.StateUnavailable {
		t.Fatal("not partitioned")
	}
	time.Sleep(300 * time.Millisecond)
	h.checker.ProbeAll(context.Background())
	if h.checker.State("node-1") != storage.StateHealthy {
		t.Fatal("temporary partition did not heal")
	}
}

// W=2 of RF=3: a write with one node partitioned is acknowledged with two
// verified replicas and committed DEGRADED; with two partitioned it fails
// with a quorum error and commits nothing, leaving the previous version.
func TestWriteQuorum(t *testing.T) {
	ctx := context.Background()
	h := newHarnessOpts(t, 3, 3, harnessOpts{cfg: Config{WriteQuorum: Quorum{N: 2}}})
	v1 := h.put("obj", []byte("v1"), PutOptions{})
	if len(v1.Object.Replicas) != 3 || v1.WriteQuorum != 2 {
		t.Fatalf("v1 %+v", v1)
	}

	h.partition("node-3")
	v2 := h.put("obj", []byte("v2"), PutOptions{})
	if len(v2.Object.Replicas) != 2 || slices.Contains(v2.Object.Replicas, "node-3") {
		t.Fatalf("v2 replicas %v", v2.Object.Replicas)
	}
	if st := h.assess("obj").State; st != durability.Degraded {
		t.Fatalf("v2 state %s, want DEGRADED", st)
	}

	// Two nodes cut, health not yet aware of node-2's cut: the write is
	// attempted, reaches only node-1, and is rolled back.
	if err := h.net.Partition("node-2", 0); err != nil {
		t.Fatal(err)
	}
	_, err := h.coord.Put(ctx, "obj", bytes.NewReader([]byte("v3")), 0)
	var qe *QuorumError
	if !errors.As(err, &qe) || qe.Operation != "write" || qe.Required != 2 || qe.Achieved != 1 {
		t.Fatalf("want write quorum error, got %v", err)
	}
	if got := h.versionsOn("node-1", "obj"); !slices.Equal(got, []uint64{2}) {
		t.Fatalf("node-1 holds %v after a failed quorum write; want only v2", got)
	}
	if obj, _ := h.meta.GetObject(ctx, "obj"); obj.Version != 2 || obj.SHA256 != shaHex([]byte("v2")) {
		t.Fatalf("metadata changed by a failed quorum write: %+v", obj)
	}
	// Now health knows: fail fast, before any byte is written.
	h.checker.ProbeAll(ctx)
	if _, err := h.coord.Put(ctx, "fresh", bytes.NewReader([]byte("f")), 0); !errors.As(err, &qe) || qe.Achieved != 0 {
		t.Fatalf("fail-fast quorum error: %v", err)
	}
	if _, err := h.meta.GetRecord(ctx, "fresh"); !errors.Is(err, metadata.ErrNotFound) {
		t.Fatal("a failed write left metadata")
	}
	// The default quorum (all) keeps the P1 semantics and error.
	if _, err := h.coord.PutWithOptions(ctx, "obj", bytes.NewReader([]byte("x")), PutOptions{WriteQuorum: &QuorumAll}); err == nil || errors.As(err, &qe) {
		t.Fatalf("W=all must fail with the P1 error, got %v", err)
	}
	// An explicit quorum above RF is a client error.
	if _, err := h.coord.PutWithOptions(ctx, "obj", bytes.NewReader([]byte("x")), PutOptions{WriteQuorum: q(4)}); !errors.Is(err, ErrInvalidQuorum) {
		t.Fatalf("W=4 on RF=3: %v", err)
	}
	if !strings.Contains(h.logs.String(), `"event":"QUORUM_NOT_MET"`) {
		t.Fatal("no QUORUM_NOT_MET event")
	}
}

// R=2: a read needs two replicas holding the current version with the
// metadata checksum; a partitioned or stale replica never counts, and no
// bytes are returned without the quorum.
func TestReadQuorumAndStaleReplicaNeverCounts(t *testing.T) {
	ctx := context.Background()
	h := newHarnessOpts(t, 3, 3, harnessOpts{cfg: Config{ReadQuorum: Quorum{N: 2}}})
	h.put("obj", []byte("version one"), PutOptions{})
	old := h.save("node-3", "obj", 1)
	h.put("obj", []byte("version two"), PutOptions{})

	r, err := h.coord.Get(ctx, "obj", ReadOptions{})
	if err != nil || r.ReadQuorum != 2 || len(r.Confirmed) < 2 {
		t.Fatalf("R=2 read with all healthy: %+v %v", r, err)
	}
	readAll(t, r)

	// node-3 loses v2 and holds only v1 again (a node that came back from
	// a partition with old bytes): it is listed, but stale.
	h.remove("node-3", "obj", 2)
	h.restore("node-3", "obj", 1, old)
	if _, err := h.coord.Get(ctx, "obj", ReadOptions{ReadQuorum: q(3)}); !errors.Is(err, ErrQuorumNotMet) {
		t.Fatalf("R=3 with a stale replica must fail: %v", err)
	}
	if f, ok := mustGet(t, h, "obj").FailureFor("node-3"); !ok || f.Reason != metadata.ReasonStaleVersion || f.ObservedVersion != 1 {
		t.Fatalf("stale replica not recorded: %+v", f)
	}
	r, err = h.coord.Get(ctx, "obj", ReadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got := readAll(t, r); string(got) != "version two" || slices.Contains(r.Confirmed, "node-3") {
		t.Fatalf("R=2 read returned %q confirmed by %v", got, r.Confirmed)
	}

	// Two of three links cut: the read fails its quorum, returns nothing.
	if err := h.net.Partition("node-2", 0); err != nil {
		t.Fatal(err)
	}
	var qe *QuorumError
	if _, err := h.coord.Get(ctx, "obj", ReadOptions{}); !errors.As(err, &qe) || qe.Operation != "read" || qe.Achieved != 1 {
		t.Fatalf("want read quorum error, got %v", err)
	}
	// R=1 still reads from the one verified replica, and it is never v1.
	r, err = h.coord.Get(ctx, "obj", ReadOptions{ReadQuorum: q(1)})
	if err != nil || string(readAll(t, r)) != "version two" {
		t.Fatalf("R=1 read: %v", err)
	}
}

func mustGet(t *testing.T, h *harness, id string) metadata.Object {
	t.Helper()
	obj, err := h.meta.GetObject(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return obj
}

// Partition during write, ack lost: node-3 stores and verifies the bytes,
// then the link is cut before its answer arrives. The quorum write commits
// without node-3; after healing, reconciliation finds node-3's copy,
// verifies it (version + checksum) and adopts it — RF restored with no
// data transfer.
func TestPartitionDuringWriteAckLostThenAdoptedOnHeal(t *testing.T) {
	ctx := context.Background()
	h := newHarnessOpts(t, 3, 3, harnessOpts{cfg: Config{WriteQuorum: Quorum{N: 2}}})
	data := payload(64<<10, 7)
	if err := h.net.Arm("node-3", netchaos.Trigger{Point: netchaos.PointAck, Method: http.MethodPut}); err != nil {
		t.Fatal(err)
	}
	res := h.put("obj", data, PutOptions{})
	if slices.Contains(res.Object.Replicas, "node-3") || len(res.Object.Replicas) != 2 {
		t.Fatalf("replicas %v", res.Object.Replicas)
	}
	h.mustHold("node-3", "obj", 1, data) // delivered, just never acknowledged
	if l, _ := h.net.Link("node-3"); !l.Partitioned {
		t.Fatal("ack trigger did not start the partition")
	}
	h.checker.ProbeAll(ctx)

	h.heal("node-3")
	rep, err := h.coord.ReconcileNode(ctx, "node-3", "test")
	if err != nil || rep.Adopted != 1 || rep.AdoptedBytes != int64(len(data)) {
		t.Fatalf("reconcile %+v %v", rep, err)
	}
	obj := mustGet(t, h, "obj")
	if !slices.Contains(obj.Replicas, "node-3") || h.assess("obj").State != durability.Healthy {
		t.Fatalf("not adopted: %+v", obj)
	}
	if c, _ := h.coord.Consistency(ctx, "obj", true); !c.Consistent {
		t.Fatalf("not consistent: %+v", c)
	}
}

// Partition during write, upload cut: the link breaks after 1000 bytes of
// the upload; node-3 never commits anything, the all-replica write fails
// and rolls back, nothing is left anywhere.
func TestPartitionDuringUploadCommitsNothing(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 3, 3)
	if err := h.net.Arm("node-3", netchaos.Trigger{Point: netchaos.PointUpload, AfterBytes: 1000, Method: http.MethodPut}); err != nil {
		t.Fatal(err)
	}
	var re *ReplicationError
	if _, err := h.coord.Put(ctx, "obj", bytes.NewReader(payload(64<<10, 3)), 0); !errors.As(err, &re) || re.Achieved != 2 {
		t.Fatalf("want replication error with 2 achieved, got %v", err)
	}
	for id := range h.roots {
		if v := h.versionsOn(id, "obj"); len(v) != 0 {
			t.Fatalf("%s holds %v after a cut upload + rollback", id, v)
		}
	}
	if l, _ := h.net.Link("node-3"); !l.Partitioned || l.TransfersCut == 0 {
		t.Fatalf("link %+v", l)
	}
}

// Partition during a failed write whose rollback cannot reach the node:
// the node keeps an uncommitted newer version. Reconciliation removes it,
// but only after verifying a copy of the committed version elsewhere.
func TestUncommittedVersionFromPartitionedWriteIsRemovedOnHeal(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 3, 3)
	h.put("obj", []byte("committed v1"), PutOptions{})
	if err := h.net.Arm("node-3", netchaos.Trigger{Point: netchaos.PointAck, Method: http.MethodPut}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.coord.Put(ctx, "obj", bytes.NewReader([]byte("never committed v2")), 0); err == nil {
		t.Fatal("write with a lost ack on an all-replica policy must fail")
	}
	if got := h.versionsOn("node-3", "obj"); !slices.Equal(got, []uint64{1, 2}) {
		t.Fatalf("premise: node-3 holds %v, want v1 + uncommitted v2", got)
	}
	h.checker.ProbeAll(ctx)
	h.heal("node-3")
	rep, err := h.coord.ReconcileNode(ctx, "node-3", "test")
	if err != nil || rep.Removed[RemovalUncommitted] != 1 || rep.Verified != 1 {
		t.Fatalf("reconcile %+v %v", rep, err)
	}
	if got := h.versionsOn("node-3", "obj"); !slices.Equal(got, []uint64{1}) {
		t.Fatalf("node-3 holds %v, want v1", got)
	}
	r, err := h.coord.Get(ctx, "obj", ReadOptions{ReadQuorum: q(3)})
	if err != nil || string(readAll(t, r)) != "committed v1" {
		t.Fatalf("read: %v", err)
	}
}

// Partition during read: the link to the first replica breaks after 16 KiB
// of the download. The read falls back to a verified replica; with R=3 the
// cut replica cannot confirm, so no bytes are returned.
func TestPartitionDuringRead(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 3, 3)
	data := payload(256<<10, 9)
	obj := h.put("obj", data, PutOptions{}).Object
	first := obj.Replicas[0]
	if err := h.net.Arm(first, netchaos.Trigger{Point: netchaos.PointDownload, AfterBytes: 16 << 10, Method: http.MethodGet}); err != nil {
		t.Fatal(err)
	}
	r, err := h.coord.Get(ctx, "obj", ReadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got := readAll(t, r); !bytes.Equal(got, data) || r.ServedBy == first {
		t.Fatalf("served by %s, %d bytes", r.ServedBy, len(got))
	}
	if r.Attempts[0].NodeID != first || r.Attempts[0].Outcome == OutcomeVerified {
		t.Fatalf("attempts %+v", r.Attempts)
	}

	h.heal(first)
	if err := h.net.Arm(first, netchaos.Trigger{Point: netchaos.PointDownload, AfterBytes: 16 << 10, Method: http.MethodGet}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.coord.Get(ctx, "obj", ReadOptions{ReadQuorum: q(3)}); !errors.Is(err, ErrQuorumNotMet) {
		t.Fatalf("R=3 with a cut replica: %v", err)
	}
}

// Healing: while node-3 is partitioned one object is overwritten (W=2),
// one is deleted, one is untouched. When the link heals, reconciliation
// removes node-3's stale version (after verifying the newest one
// elsewhere) and the deleted object's bytes (never resurrected), verifies
// the untouched replica, and the existing repair restores RF with the
// newest version.
func TestHealingReconcilesNewestVersionAndTombstones(t *testing.T) {
	ctx := context.Background()
	h := newHarnessOpts(t, 3, 3, harnessOpts{cfg: Config{WriteQuorum: Quorum{N: 2}}})
	for _, id := range []string{"over", "gone", "same"} {
		h.put(id, []byte(id+" v1"), PutOptions{})
	}
	h.partition("node-3")
	h.put("over", []byte("over v2, written during the partition"), PutOptions{})
	if _, err := h.coord.Delete(ctx, "gone"); err != nil {
		t.Fatal(err)
	}
	// Reads during the partition: newest version, and the delete holds.
	if r, err := h.coord.Get(ctx, "over", ReadOptions{}); err != nil || !strings.HasPrefix(string(readAll(t, r)), "over v2") {
		t.Fatalf("read during partition: %v", err)
	}
	if _, err := h.coord.Get(ctx, "gone", ReadOptions{}); !errors.Is(err, metadata.ErrNotFound) {
		t.Fatalf("deleted object readable: %v", err)
	}
	if !slices.Equal(h.versionsOn("node-3", "over"), []uint64{1}) || !slices.Equal(h.versionsOn("node-3", "gone"), []uint64{1}) {
		t.Fatal("premise: node-3 should still hold the old bytes")
	}

	h.heal("node-3")
	rep, err := h.coord.ReconcileNode(ctx, "node-3", "test")
	if err != nil {
		t.Fatal(err)
	}
	if rep.Removed[RemovalStale] != 1 || rep.Removed[RemovalDeletedObject] != 1 || rep.Verified != 1 || rep.Kept != 0 {
		t.Fatalf("reconcile %+v", rep)
	}
	if len(h.versionsOn("node-3", "over")) != 0 || len(h.versionsOn("node-3", "gone")) != 0 {
		t.Fatal("stale / deleted bytes left on node-3")
	}
	// RF below policy → the existing repair restores it with the newest version.
	if st := h.assess("over").State; st != durability.Degraded {
		t.Fatalf("over is %s", st)
	}
	if _, err := h.coord.RepairObject(ctx, "over"); err != nil {
		t.Fatal(err)
	}
	h.mustHold("node-3", "over", 2, []byte("over v2, written during the partition"))
	for _, id := range []string{"over", "gone", "same"} {
		if c, err := h.coord.Consistency(ctx, id, true); err != nil || !c.Consistent {
			t.Fatalf("%s not consistent: %+v %v", id, c, err)
		}
	}
	if _, err := h.coord.Get(ctx, "gone", ReadOptions{}); !errors.Is(err, metadata.ErrNotFound) {
		t.Fatal("deleted object resurrected")
	}
	// A later write of the deleted ID is strictly newer than its tombstone.
	if res := h.put("gone", []byte("new life"), PutOptions{}); res.Object.Version != 3 {
		t.Fatalf("recreated at v%d", res.Object.Version)
	}
}

// callLog records node calls and metadata commits in order.
type callLog struct {
	mu  sync.Mutex
	ops []string
}

func (l *callLog) add(format string, a ...any) {
	l.mu.Lock()
	l.ops = append(l.ops, fmt.Sprintf(format, a...))
	l.mu.Unlock()
}

func (l *callLog) index(op string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Index(l.ops, op)
}

func (l *callLog) reset() {
	l.mu.Lock()
	l.ops = nil
	l.mu.Unlock()
}

type recordingNode struct {
	storage.StorageNode
	log *callLog
	// badVerify makes this node's re-hash report a wrong digest.
	badVerify *bool
}

func (n *recordingNode) PutReplica(ctx context.Context, id string, v uint64, want integrity.Digest, body io.Reader) (storage.ReplicaInfo, error) {
	info, err := n.StorageNode.PutReplica(ctx, id, v, want, body)
	if err == nil {
		n.log.add("put %s v%d", n.ID(), v)
	}
	return info, err
}

func (n *recordingNode) VerifyReplica(ctx context.Context, id string, v uint64) (storage.VerifyResult, error) {
	vr, err := n.StorageNode.VerifyReplica(ctx, id, v)
	if err == nil && n.badVerify != nil && *n.badVerify {
		vr.Actual.SHA256 = strings.Repeat("0", 64)
	}
	n.log.add("verify %s v%d", n.ID(), v)
	return vr, err
}

func (n *recordingNode) DeleteReplica(ctx context.Context, id string, v uint64) error {
	n.log.add("delete %s v%d", n.ID(), v)
	return n.StorageNode.DeleteReplica(ctx, id, v)
}

type recordingMeta struct {
	*metadata.FileStore
	log *callLog
}

func (m recordingMeta) Modify(ctx context.Context, id string, fn func(metadata.Object) (metadata.Object, error)) (metadata.Object, error) {
	obj, err := m.FileStore.Modify(ctx, id, fn)
	if err == nil {
		m.log.add("commit %v", obj.Replicas)
	}
	return obj, err
}

// The guarded replacement order, proven on the calls actually made:
// replacement written → its version and checksum verified → replacement
// committed → only then the old replica deleted. If the replacement fails
// verification nothing is committed and the old replica is never deleted;
// and an extra copy is never deleted while it may be the only valid one.
func TestGuardedReplacementVerifiesBeforeRemoval(t *testing.T) {
	ctx := context.Background()
	log := &callLog{}
	bad := map[string]*bool{}
	h := newHarnessOpts(t, 4, 3, harnessOpts{
		wrap: func(n storage.StorageNode) storage.StorageNode {
			b := false
			bad[n.ID()] = &b
			return &recordingNode{StorageNode: n, log: log, badVerify: &b}
		},
		meta: func(fs *metadata.FileStore) metadata.Store { return recordingMeta{fs, log} },
	})
	data := payload(32<<10, 5)
	obj := h.put("obj", data, PutOptions{}).Object
	var spare string
	for id := range h.roots {
		if !slices.Contains(obj.Replicas, id) {
			spare = id
		}
	}
	victim := obj.Replicas[1]
	h.corrupt(victim, "obj", 1)
	if _, err := h.coord.Scrub(ctx, 2); err != nil {
		t.Fatal(err)
	}

	// 1. The fresh replacement fails its verification: it is discarded,
	// never committed; repair falls back to rewriting the corrupted replica
	// in place (verified before commit too); the old replica is never
	// deleted.
	*bad[spare] = true
	log.reset()
	if _, err := h.coord.RepairObject(ctx, "obj"); err != nil {
		t.Fatal(err)
	}
	if slices.Contains(mustGet(t, h, "obj").Replicas, spare) {
		t.Fatalf("a replacement that failed verification was committed: %v", log.ops)
	}
	if log.index(fmt.Sprintf("delete %s v1", victim)) >= 0 {
		t.Fatalf("old replica deleted: %v", log.ops)
	}
	vs, ds := log.index(fmt.Sprintf("verify %s v1", spare)), log.index(fmt.Sprintf("delete %s v1", spare))
	pv, vv := log.index(fmt.Sprintf("put %s v1", victim)), log.index(fmt.Sprintf("verify %s v1", victim))
	c1 := slices.IndexFunc(log.ops, func(s string) bool { return strings.HasPrefix(s, "commit ") })
	if vs < 0 || ds < vs || pv < 0 || !(pv < vv && vv < c1) {
		t.Fatalf("order: %v", log.ops)
	}
	h.mustHold(victim, "obj", 1, data)

	// 2. Another replica is corrupted; this time the fresh replacement
	// verifies. Exact order: written → verified → committed → old deleted.
	victim = obj.Replicas[2]
	h.corrupt(victim, "obj", 1)
	if _, err := h.coord.Scrub(ctx, 2); err != nil {
		t.Fatal(err)
	}
	*bad[spare] = false
	log.reset()
	var deletedAfter []string
	h.coord.hooks.beforeReplicaDelete = func(_, node string, _ uint64) {
		log.mu.Lock()
		deletedAfter = slices.Clone(log.ops)
		log.mu.Unlock()
	}
	if _, err := h.coord.RepairObject(ctx, "obj"); err != nil {
		t.Fatal(err)
	}
	put := log.index(fmt.Sprintf("put %s v1", spare))
	verify := log.index(fmt.Sprintf("verify %s v1", spare))
	commit := slices.IndexFunc(log.ops, func(s string) bool { return strings.HasPrefix(s, "commit ") && strings.Contains(s, spare) })
	del := log.index(fmt.Sprintf("delete %s v1", victim))
	if put < 0 || verify < 0 || commit < 0 || del < 0 || !(put < verify && verify < commit && commit < del) {
		t.Fatalf("order put=%d verify=%d commit=%d delete=%d: %v", put, verify, commit, del, log.ops)
	}
	if !slices.Contains(deletedAfter, fmt.Sprintf("verify %s v1", spare)) {
		t.Fatalf("delete hook ran before verification: %v", deletedAfter)
	}
	h.mustHold(spare, "obj", 1, data)
	if _, err := h.replicaBytes(victim, "obj", 1); err == nil {
		t.Fatal("corrupted replica still on disk")
	}
	if !strings.Contains(h.logs.String(), `"event":"REPLICA_REMOVED"`) {
		t.Fatal("no REPLICA_REMOVED event")
	}

	// 3. Never the only verified copy: the victim node now holds a valid,
	// unlisted extra copy while every listed replica is corrupt. Removing
	// the extra copy is refused, and it stays on disk.
	if _, err := h.coord.nodes[victim].PutReplica(ctx, "obj", 1, integrity.Digest{SHA256: shaHex(data), Size: int64(len(data))}, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	cur := mustGet(t, h, "obj")
	for _, n := range cur.Replicas {
		h.corrupt(n, "obj", 1)
	}
	log.reset()
	unlock := h.coord.locks.Lock("obj")
	res, err := h.coord.removeReplicaLocked(ctx, cur, victim, 1, "test", nil)
	unlock()
	if !errors.Is(err, ErrRemovalRefused) || res.Class != RemovalExtra || len(res.VerifiedBy) != 0 {
		t.Fatalf("want refusal of the only valid copy, got %+v %v", res, err)
	}
	if log.index(fmt.Sprintf("delete %s v1", victim)) >= 0 {
		t.Fatal("only valid copy deleted")
	}
	h.mustHold(victim, "obj", 1, data)
	// A listed replica of the current version is never removed directly.
	unlock = h.coord.locks.Lock("obj")
	_, err = h.coord.removeReplicaLocked(ctx, cur, cur.Replicas[0], 1, "test", nil)
	unlock()
	if !errors.Is(err, ErrRemovalRefused) {
		t.Fatalf("listed replica removal: %v", err)
	}
}

// Storage metrics equal what is really on the nodes' disks.
func TestStorageMetricsMatchDisk(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 3, 3)
	sizes := map[string]int{"a": 1000, "b": 2000, "c": 3000}
	for id, n := range sizes {
		h.put(id, payload(n, byte(n)), PutOptions{})
	}
	h.put("a", payload(500, 1), PutOptions{}) // overwrite: v1 cleaned up
	if _, err := h.coord.Delete(ctx, "c"); err != nil {
		t.Fatal(err)
	}
	diskBytes := func(ids ...string) (int64, int) {
		var total int64
		var files int
		for _, id := range ids {
			filepath.WalkDir(filepath.Join(h.roots[id], "objects"), func(p string, d os.DirEntry, err error) error {
				if err == nil && !d.IsDir() && strings.HasSuffix(p, ".data") {
					fi, _ := d.Info()
					total += fi.Size()
					files++
				}
				return nil
			})
		}
		return total, files
	}
	h.checker.ProbeAll(ctx) // refresh the nodes' self-reported usage
	m, err := h.coord.StorageMetrics(ctx)
	if err != nil {
		t.Fatal(err)
	}
	disk, files := diskBytes("node-1", "node-2", "node-3")
	if !m.Complete || m.LiveObjects != 2 || m.Tombstones != 1 || m.LogicalBytes != 2500 {
		t.Fatalf("metrics %+v", m)
	}
	if *m.PhysicalBytes != disk || disk != 7500 || *m.PhysicalReplicas != files || files != 6 {
		t.Fatalf("physical %d/%d, disk %d/%d", *m.PhysicalBytes, *m.PhysicalReplicas, disk, files)
	}
	if *m.StorageOverhead != 3.0 || *m.PolicyOverhead != 3.0 || m.ListedReplicas != 6 || m.ExpectedReplicas != 6 {
		t.Fatalf("overhead %v/%v listed %d", *m.StorageOverhead, *m.PolicyOverhead, m.ListedReplicas)
	}
	if m.Breakdown.CurrentListed.Bytes != disk || m.Breakdown.Stale.Replicas != 0 {
		t.Fatalf("breakdown %+v", m.Breakdown)
	}
	for _, n := range m.Nodes {
		if *n.Bytes != 2500 || *n.ReportedUsedBytes != 2500 {
			t.Fatalf("node %+v", n)
		}
	}

	// A partitioned node: totals that need it are unknown, not guessed.
	h.partition("node-2")
	m, _ = h.coord.StorageMetrics(ctx)
	reach, _ := diskBytes("node-1", "node-3")
	if m.Complete || m.PhysicalBytes != nil || m.PhysicalReplicas != nil || m.StorageOverhead != nil ||
		!slices.Equal(m.UnreachableNodes, []string{"node-2"}) || m.PhysicalBytesReachable != reach {
		t.Fatalf("incomplete metrics %+v", m)
	}
	if m.LogicalBytes != 2500 {
		t.Fatal("logical bytes come from metadata and stay known")
	}
}
