package coordinator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"vault/internal/events"
	"vault/internal/health"
	"vault/internal/metadata"
	"vault/internal/netchaos"
	"vault/internal/node"
	"vault/internal/nodeclient"
	"vault/internal/placement"
	"vault/internal/storage"
)

// harness wires a coordinator to n real node servers (in-process HTTP,
// real on-disk stores). Multi-process behaviour is covered separately by
// test/integration.
type harness struct {
	t       *testing.T
	coord   *Coordinator
	meta    *metadata.FileStore
	checker *health.Checker
	roots   map[string]string
	servers map[string]*httptest.Server
	logs    *syncBuffer
	metaDir string
	net     *netchaos.Injector
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

func newHarness(t *testing.T, n, rf int) *harness { return newHarnessOpts(t, n, rf, harnessOpts{}) }

// harnessOpts adjusts a harness: coordinator config (quorums), wrappers
// around each node client and around the metadata store (to record calls).
type harnessOpts struct {
	cfg  Config
	wrap func(storage.StorageNode) storage.StorageNode
	meta func(*metadata.FileStore) metadata.Store
}

func newHarnessOpts(t *testing.T, n, rf int, o harnessOpts) *harness {
	t.Helper()
	h := &harness{t: t, roots: map[string]string{}, servers: map[string]*httptest.Server{}, logs: &syncBuffer{}}
	log := events.New(h.logs, "test")
	base := nodeclient.NewTransport()
	var ids []string
	for i := 1; i <= n; i++ {
		ids = append(ids, fmt.Sprintf("node-%d", i))
	}
	// Every coordinator→node request goes through the network fault
	// injector, exactly as in the api process with chaos enabled.
	h.net = netchaos.New(log, ids)
	var nodes []storage.StorageNode
	var probers []health.Prober
	for _, id := range ids {
		root := t.TempDir()
		st, err := node.OpenStore(root, id)
		if err != nil {
			t.Fatal(err)
		}
		ns := node.NewServer(st, events.Discard(), 64<<20)
		srv := httptest.NewServer(ns.Handler())
		ns.EnableChaos(func() { srv.CloseClientConnections(); srv.Close() })
		t.Cleanup(srv.Close)
		c, err := nodeclient.New(id, srv.URL, &http.Client{Transport: h.net.Transport(id, base)})
		if err != nil {
			t.Fatal(err)
		}
		h.roots[id], h.servers[id] = root, srv
		var sn storage.StorageNode = c
		if o.wrap != nil {
			sn = o.wrap(c)
		}
		nodes = append(nodes, sn)
		probers = append(probers, c)
	}
	h.metaDir = t.TempDir()
	meta, err := metadata.OpenFileStore(h.metaDir)
	if err != nil {
		t.Fatal(err)
	}
	h.meta = meta
	var ms metadata.Store = meta
	if o.meta != nil {
		ms = o.meta(meta)
	}
	h.checker = health.New(probers, time.Hour, time.Second, log)
	h.checker.ProbeAll(context.Background())
	cfg := o.cfg
	cfg.DefaultReplicationFactor, cfg.TempDir, cfg.MaxObjectSize = rf, t.TempDir(), 8<<20
	h.coord, err = New(cfg, nodes, ms, placement.Rendezvous{}, h.checker, log)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

// kill makes a node unreachable. With probe=false the health checker still
// believes it is healthy, modelling a crash between probes.
func (h *harness) kill(id string, probe bool) {
	h.servers[id].CloseClientConnections()
	h.servers[id].Close()
	if probe {
		h.checker.ProbeAll(context.Background())
	}
}

func (h *harness) replicaBytes(id, obj string, version uint64) ([]byte, error) {
	return os.ReadFile(node.ReplicaDataPath(h.roots[id], obj, version))
}

func (h *harness) corrupt(id, obj string, version uint64) {
	h.t.Helper()
	path := node.ReplicaDataPath(h.roots[id], obj, version)
	b, err := os.ReadFile(path)
	if err != nil {
		h.t.Fatal(err)
	}
	b[len(b)/2] ^= 0xFF
	if err := os.WriteFile(path, b, 0o644); err != nil {
		h.t.Fatal(err)
	}
}

func payload(n int, seed byte) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i)*31 + seed
	}
	return b
}

func shaHex(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

func readAll(t *testing.T, r *ReadResult) []byte {
	t.Helper()
	defer r.Body.Close()
	b, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestPutGetReplicatesToEveryNodeDisk(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 3, 3)
	data := payload(100_000, 1)

	res, err := h.coord.Put(ctx, "obj", bytes.NewReader(data), 0)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Created || res.Object.Version != 1 || res.Object.SHA256 != shaHex(data) || len(res.Object.Replicas) != 3 {
		t.Fatalf("unexpected result %+v", res)
	}
	for _, id := range []string{"node-1", "node-2", "node-3"} {
		b, err := h.replicaBytes(id, "obj", 1)
		if err != nil || !bytes.Equal(b, data) {
			t.Fatalf("%s: replica on disk missing or wrong (err=%v)", id, err)
		}
	}

	r, err := h.coord.Get(ctx, "obj", ReadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got := readAll(t, r); !bytes.Equal(got, data) {
		t.Fatal("read returned different bytes")
	}
}

func TestReplicationFactorIsHonoured(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 3, 3)
	res, err := h.coord.Put(ctx, "rf2", bytes.NewReader([]byte("two copies")), 2)
	if err != nil {
		t.Fatal(err)
	}
	held := 0
	for id := range h.roots {
		if _, err := h.replicaBytes(id, "rf2", 1); err == nil {
			held++
			if !slices.Contains(res.Object.Replicas, id) {
				t.Fatalf("%s holds bytes but is not in metadata", id)
			}
		}
	}
	if held != 2 || res.Object.ReplicationFactor != 2 {
		t.Fatalf("held=%d rf=%d, want 2/2", held, res.Object.ReplicationFactor)
	}

	for _, bad := range []int{-1, 4} {
		if _, err := h.coord.Put(ctx, "bad", bytes.NewReader(nil), bad); !errors.Is(err, ErrInvalidReplicationFactor) {
			t.Fatalf("rf=%d: err = %v", bad, err)
		}
	}
}

func TestInsufficientHealthyNodesIsNotSuccess(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 3, 3)
	h.kill("node-3", true)

	_, err := h.coord.Put(ctx, "obj", bytes.NewReader([]byte("x")), 0)
	var ie *placement.InsufficientNodesError
	if !errors.As(err, &ie) || ie.Required != 3 || ie.Available != 2 {
		t.Fatalf("err = %v, want insufficient 3/2", err)
	}
	if _, err := h.meta.GetObject(ctx, "obj"); !errors.Is(err, metadata.ErrNotFound) {
		t.Fatal("metadata committed despite failed policy")
	}
	for _, id := range []string{"node-1", "node-2"} {
		if _, err := h.replicaBytes(id, "obj", 1); err == nil {
			t.Fatalf("%s holds bytes for a failed write", id)
		}
	}
}

func TestFailedReplicaWriteRollsBackAndKeepsPreviousVersion(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 3, 3)
	v1 := []byte("version one")
	if _, err := h.coord.Put(ctx, "obj", bytes.NewReader(v1), 0); err != nil {
		t.Fatal(err)
	}
	// node-2 crashes, but the checker has not noticed yet.
	h.kill("node-2", false)

	_, err := h.coord.Put(ctx, "obj", bytes.NewReader([]byte("version two")), 0)
	var re *ReplicationError
	if !errors.As(err, &re) || re.Required != 3 || re.Achieved != 2 {
		t.Fatalf("err = %v, want ReplicationError 2/3", err)
	}
	for _, id := range []string{"node-1", "node-3"} {
		if _, err := h.replicaBytes(id, "obj", 2); err == nil {
			t.Fatalf("%s kept an uncommitted v2 replica", id)
		}
	}
	obj, _ := h.meta.GetObject(ctx, "obj")
	if obj.Version != 1 {
		t.Fatalf("metadata version = %d, want 1", obj.Version)
	}
	r, err := h.coord.Get(ctx, "obj", ReadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(readAll(t, r), v1) {
		t.Fatal("previous version not intact")
	}
	if h.checker.State("node-2") != storage.StateUnavailable {
		t.Fatal("failed write did not passively mark node-2 unavailable")
	}
}

func TestWriteUsesSpareWhenTargetFails(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 4, 3)
	plan, _ := placement.Rendezvous{}.Place("obj", []string{"node-1", "node-2", "node-3", "node-4"}, 3)
	h.kill(plan.Targets[0], false)

	res, err := h.coord.Put(ctx, "obj", bytes.NewReader([]byte("spare")), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Object.Replicas) != 3 || slices.Contains(res.Object.Replicas, plan.Targets[0]) ||
		!slices.Contains(res.Object.Replicas, plan.Spares[0]) {
		t.Fatalf("replicas = %v, dead target %s, spare %s", res.Object.Replicas, plan.Targets[0], plan.Spares[0])
	}
}

func TestCorruptReplicaIsRejectedAndRecorded(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 3, 3)
	data := payload(50_000, 7)
	res, err := h.coord.Put(ctx, "obj", bytes.NewReader(data), 0)
	if err != nil {
		t.Fatal(err)
	}
	victim := res.Object.Replicas[0] // first in read order
	h.corrupt(victim, "obj", 1)

	r, err := h.coord.Get(ctx, "obj", ReadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if r.ServedBy == victim {
		t.Fatal("corrupt replica served")
	}
	if !bytes.Equal(readAll(t, r), data) {
		t.Fatal("wrong bytes returned")
	}
	if r.Attempts[0].NodeID != victim || r.Attempts[0].Outcome != OutcomeChecksumMismatch {
		t.Fatalf("attempts = %+v", r.Attempts)
	}
	obj, _ := h.meta.GetObject(ctx, "obj")
	if len(obj.IntegrityFailures) != 1 || obj.IntegrityFailures[0].NodeID != victim ||
		obj.IntegrityFailures[0].Reason != metadata.ReasonChecksumMismatch {
		t.Fatalf("integrity failures = %+v", obj.IntegrityFailures)
	}
	if !bytes.Contains([]byte(h.logs.String()), []byte(`"event":"CHECKSUM_MISMATCH"`)) {
		t.Fatal("no CHECKSUM_MISMATCH event logged")
	}
}

func TestTruncatedAndMissingReplicasAreRejected(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 3, 3)
	data := payload(10_000, 3)
	res, _ := h.coord.Put(ctx, "obj", bytes.NewReader(data), 0)
	a, b := res.Object.Replicas[0], res.Object.Replicas[1]
	os.WriteFile(node.ReplicaDataPath(h.roots[a], "obj", 1), data[:100], 0o644)
	os.Remove(node.ReplicaDataPath(h.roots[b], "obj", 1))

	r, err := h.coord.Get(ctx, "obj", ReadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(readAll(t, r), data) {
		t.Fatal("wrong bytes")
	}
	if r.Attempts[0].Outcome != OutcomeSizeMismatch || r.Attempts[1].Outcome != OutcomeMissing {
		t.Fatalf("attempts = %+v", r.Attempts)
	}
}

func TestAllReplicasCorruptReturnsError(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 3, 3)
	res, _ := h.coord.Put(ctx, "obj", bytes.NewReader(payload(1000, 1)), 0)
	for _, id := range res.Object.Replicas {
		h.corrupt(id, "obj", 1)
	}
	_, err := h.coord.Get(ctx, "obj", ReadOptions{})
	var nv *NoVerifiedReplicaError
	if !errors.As(err, &nv) || len(nv.Attempts) != 3 {
		t.Fatalf("err = %v, want NoVerifiedReplicaError with 3 attempts", err)
	}
}

func TestReadPreference(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 3, 3)
	h.coord.Put(ctx, "obj", bytes.NewReader([]byte("pref")), 0)
	r, err := h.coord.Get(ctx, "obj", ReadOptions{PreferNode: "node-3"})
	if err != nil {
		t.Fatal(err)
	}
	r.Body.Close()
	if r.ServedBy != "node-3" {
		t.Fatalf("served by %s, want node-3", r.ServedBy)
	}
	if _, err := h.coord.Get(ctx, "obj", ReadOptions{PreferNode: "node-9"}); !errors.Is(err, ErrInvalidReadPreference) {
		t.Fatalf("err = %v", err)
	}
}

func TestReadSurvivesUnavailableReplica(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 3, 3)
	data := payload(4096, 9)
	res, _ := h.coord.Put(ctx, "obj", bytes.NewReader(data), 0)
	first := res.Object.Replicas[0]
	h.kill(first, false) // crash not yet seen by health checker

	r, err := h.coord.Get(ctx, "obj", ReadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(readAll(t, r), data) || r.ServedBy == first {
		t.Fatalf("served by %s", r.ServedBy)
	}
	if r.Attempts[0].Outcome != OutcomeUnavailable {
		t.Fatalf("attempts = %+v", r.Attempts)
	}
}

func TestOverwriteCreatesNewVersionAndRemovesOld(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 3, 3)
	h.coord.Put(ctx, "obj", bytes.NewReader([]byte("v1")), 0)
	res, err := h.coord.Put(ctx, "obj", bytes.NewReader([]byte("v2 bytes")), 0)
	if err != nil {
		t.Fatal(err)
	}
	if res.Created || res.Object.Version != 2 {
		t.Fatalf("result %+v", res)
	}
	for id := range h.roots {
		if _, err := h.replicaBytes(id, "obj", 1); err == nil {
			t.Fatalf("%s still holds superseded v1", id)
		}
	}
	r, _ := h.coord.Get(ctx, "obj", ReadOptions{})
	if string(readAll(t, r)) != "v2 bytes" {
		t.Fatal("did not read v2")
	}
}

func TestDelete(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 3, 3)
	h.coord.Put(ctx, "obj", bytes.NewReader([]byte("bye")), 0)
	h.coord.Put(ctx, "obj2", bytes.NewReader([]byte("bye2")), 0)

	res, err := h.coord.Delete(ctx, "obj")
	if err != nil || !res.FullyPurged || len(res.Deleted) != 3 {
		t.Fatalf("delete = %+v, %v", res, err)
	}
	for id := range h.roots {
		if _, err := h.replicaBytes(id, "obj", 1); err == nil {
			t.Fatalf("%s still holds deleted object", id)
		}
	}
	if _, err := h.coord.Get(ctx, "obj", ReadOptions{}); !errors.Is(err, metadata.ErrNotFound) {
		t.Fatalf("get after delete: %v", err)
	}
	if _, err := h.coord.Delete(ctx, "obj"); !errors.Is(err, metadata.ErrNotFound) {
		t.Fatalf("second delete: %v", err)
	}

	h.kill("node-1", false)
	res, err = h.coord.Delete(ctx, "obj2")
	if err != nil || res.FullyPurged || !slices.Equal(res.Unreachable, []string{"node-1"}) {
		t.Fatalf("delete with dead node = %+v, %v", res, err)
	}
}

func TestConcurrentOperations(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 3, 3)
	shared := payload(20_000, 5)
	if _, err := h.coord.Put(ctx, "shared", bytes.NewReader(shared), 0); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 200)
	for i := 0; i < 30; i++ {
		wg.Add(2)
		go func(i int) { // writer, distinct object
			defer wg.Done()
			d := payload(5000+i, byte(i))
			if _, err := h.coord.Put(ctx, fmt.Sprintf("obj-%d", i), bytes.NewReader(d), 0); err != nil {
				errs <- err
			}
		}(i)
		go func() { // reader of a stable object during writes
			defer wg.Done()
			r, err := h.coord.Get(ctx, "shared", ReadOptions{})
			if err != nil {
				errs <- err
				return
			}
			b, _ := io.ReadAll(r.Body)
			r.Body.Close()
			if !bytes.Equal(b, shared) {
				errs <- errors.New("shared object read mismatch")
			}
		}()
	}
	// Same-object writers are serialised and each gets a distinct version.
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := h.coord.Put(ctx, "hot", bytes.NewReader([]byte(fmt.Sprint(i))), 0); err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	hot, _ := h.meta.GetObject(ctx, "hot")
	if hot.Version != 10 {
		t.Fatalf("hot version = %d, want 10", hot.Version)
	}
}
