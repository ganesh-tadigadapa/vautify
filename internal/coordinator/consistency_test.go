package coordinator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"vault/internal/durability"
	"vault/internal/metadata"
	"vault/internal/node"
	"vault/internal/storage"
)

// replicaFiles captures a replica's data + record so a test can put them
// back later — modelling a node that kept old bytes (it was down, or its
// disk was restored from an old snapshot).
type replicaFiles struct{ data, rec []byte }

func (h *harness) save(nodeID, id string, v uint64) replicaFiles {
	h.t.Helper()
	p := node.ReplicaDataPath(h.roots[nodeID], id, v)
	d, err1 := os.ReadFile(p)
	r, err2 := os.ReadFile(strings.TrimSuffix(p, ".data") + ".json")
	if err1 != nil || err2 != nil {
		h.t.Fatalf("save %s v%d on %s: %v %v", id, v, nodeID, err1, err2)
	}
	return replicaFiles{d, r}
}

func (h *harness) restore(nodeID, id string, v uint64, f replicaFiles) {
	h.t.Helper()
	p := node.ReplicaDataPath(h.roots[nodeID], id, v)
	os.MkdirAll(p[:strings.LastIndex(p, "/")], 0o755)
	if os.WriteFile(p, f.data, 0o644) != nil || os.WriteFile(strings.TrimSuffix(p, ".data")+".json", f.rec, 0o644) != nil {
		h.t.Fatal("restore failed")
	}
}

func (h *harness) remove(nodeID, id string, v uint64) {
	p := node.ReplicaDataPath(h.roots[nodeID], id, v)
	os.Remove(p)
	os.Remove(strings.TrimSuffix(p, ".data") + ".json")
}

// versionsOn lists the versions of id physically present on a node.
func (h *harness) versionsOn(nodeID, id string) []uint64 {
	ents, _ := os.ReadDir(h.roots[nodeID] + "/objects/" + id)
	var out []uint64
	for _, e := range ents {
		if v, ok := strings.CutSuffix(e.Name(), ".data"); ok {
			n, _ := strconv.ParseUint(v, 10, 64)
			out = append(out, n)
		}
	}
	return out
}

func TestSequentialVersionsAreMonotonicAcrossDeletes(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 3, 3)
	for want := uint64(1); want <= 3; want++ {
		res, err := h.coord.Put(ctx, "obj", bytes.NewReader([]byte(fmt.Sprint("v", want))), 0)
		if err != nil || res.Object.Version != want || res.PreviousVersion != want-1 {
			t.Fatalf("put %d: %+v %v", want, res, err)
		}
	}
	del, err := h.coord.Delete(ctx, "obj")
	if err != nil || del.Version != 3 || del.TombstoneVersion != 4 {
		t.Fatalf("delete: %+v %v", del, err)
	}
	if _, err := h.coord.Get(ctx, "obj", ReadOptions{}); !errors.Is(err, metadata.ErrNotFound) {
		t.Fatalf("read after delete: %v", err)
	}
	if !strings.Contains(h.logs.String(), `"event":"TOMBSTONE_CREATED"`) {
		t.Fatal("no TOMBSTONE_CREATED event")
	}

	// Durable across a coordinator restart (fresh store on the same dir).
	reopened, err := metadata.OpenFileStore(h.metaDir)
	if err != nil {
		t.Fatal(err)
	}
	if rec, err := reopened.GetRecord(ctx, "obj"); err != nil || !rec.Deleted || rec.Version != 4 {
		t.Fatalf("tombstone after restart: %+v %v", rec, err)
	}

	res, err := h.coord.Put(ctx, "obj", bytes.NewReader([]byte("reborn")), 0)
	if err != nil || res.Object.Version != 5 || !res.Created || res.PreviousVersion != 4 {
		t.Fatalf("put after delete: %+v %v", res, err)
	}
	if n := strings.Count(h.logs.String(), `"event":"VERSION_CREATED"`); n != 4 {
		t.Fatalf("VERSION_CREATED events = %d, want 4", n)
	}
}

func TestCASPutAndDelete(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 3, 3)
	var ce *ConflictError

	if _, err := h.coord.PutIfVersion(ctx, "obj", bytes.NewReader([]byte("a")), 0, 0); err != nil {
		t.Fatalf("create-only on absent: %v", err)
	}
	if _, err := h.coord.PutIfVersion(ctx, "obj", bytes.NewReader([]byte("b")), 0, 0); !errors.As(err, &ce) || ce.CurrentVersion != 1 || ce.CurrentState != "live" {
		t.Fatalf("create-only on existing: %v", err)
	}
	if _, err := h.coord.PutIfVersion(ctx, "obj", bytes.NewReader([]byte("c")), 0, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := h.coord.PutIfVersion(ctx, "obj", bytes.NewReader([]byte("d")), 0, 1); !errors.As(err, &ce) || ce.ExpectedVersion != 1 || ce.CurrentVersion != 2 {
		t.Fatalf("stale CAS: %v", err)
	}
	if _, err := h.coord.DeleteIfVersion(ctx, "obj", 1); !errors.As(err, &ce) {
		t.Fatalf("stale CAS delete: %v", err)
	}
	// Failed CAS left nothing: metadata still v2, no v3 bytes anywhere.
	obj, _ := h.meta.GetObject(ctx, "obj")
	if obj.Version != 2 || obj.SHA256 != shaHex([]byte("c")) {
		t.Fatalf("metadata changed by failed CAS: %+v", obj)
	}
	for id := range h.roots {
		if !slices.Equal(h.versionsOn(id, "obj"), []uint64{2}) {
			t.Fatalf("%s holds %v, want only v2", id, h.versionsOn(id, "obj"))
		}
	}
	if !strings.Contains(h.logs.String(), `"event":"WRITE_CONFLICT"`) {
		t.Fatal("no WRITE_CONFLICT event")
	}
	del, err := h.coord.DeleteIfVersion(ctx, "obj", 2)
	if err != nil || del.TombstoneVersion != 3 {
		t.Fatalf("CAS delete: %+v %v", del, err)
	}
	if _, err := h.coord.PutIfVersion(ctx, "obj", bytes.NewReader([]byte("e")), 0, 2); !errors.As(err, &ce) || ce.CurrentState != "deleted" || ce.TombstoneVersion != 3 {
		t.Fatalf("CAS against tombstone: %v", err)
	}
	if res, err := h.coord.PutIfVersion(ctx, "obj", bytes.NewReader([]byte("f")), 0, 0); err != nil || res.Object.Version != 4 {
		t.Fatalf("create-only after delete: %+v %v", res, err)
	}
}

// Many writers with the same expected version: exactly one wins, the rest
// conflict, and no loser leaves bytes or metadata behind.
func TestConcurrentCASExactlyOneWinner(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 3, 3)
	h.coord.Put(ctx, "obj", bytes.NewReader([]byte("base")), 0)

	var wg sync.WaitGroup
	var mu sync.Mutex
	wins, conflicts := 0, 0
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := h.coord.PutIfVersion(ctx, "obj", bytes.NewReader([]byte(fmt.Sprint("writer-", i))), 0, 1)
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				wins++
			} else if errors.Is(err, ErrWriteConflict) {
				conflicts++
			} else {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	if wins != 1 || conflicts != 15 {
		t.Fatalf("wins=%d conflicts=%d", wins, conflicts)
	}
	obj, _ := h.meta.GetObject(ctx, "obj")
	if obj.Version != 2 {
		t.Fatalf("version %d", obj.Version)
	}
	for id := range h.roots {
		if !slices.Equal(h.versionsOn(id, "obj"), []uint64{2}) {
			t.Fatalf("%s holds %v, want only v2", id, h.versionsOn(id, "obj"))
		}
	}
}

// Read-modify-write increments with CAS retry never lose an update.
func TestConcurrentCASIncrementsLoseNothing(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 3, 3)
	h.coord.Put(ctx, "counter", bytes.NewReader([]byte("0")), 0)
	const clients = 12
	var wg sync.WaitGroup
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				r, err := h.coord.Get(ctx, "counter", ReadOptions{})
				if err != nil {
					t.Error(err)
					return
				}
				b, _ := io.ReadAll(r.Body)
				r.Body.Close()
				n, _ := strconv.Atoi(string(b))
				_, err = h.coord.PutIfVersion(ctx, "counter", strings.NewReader(strconv.Itoa(n+1)), 0, r.Object.Version)
				if err == nil {
					return
				}
				if !errors.Is(err, ErrWriteConflict) {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	r, _ := h.coord.Get(ctx, "counter", ReadOptions{})
	if got := string(readAll(t, r)); got != strconv.Itoa(clients) || r.Object.Version != 1+clients {
		t.Fatalf("counter = %s at v%d, want %d at v%d", got, r.Object.Version, clients, 1+clients)
	}
}

// A listed replica whose node holds only an older version (e.g. its disk
// was restored from an old snapshot) is STALE: never served, detected by
// scrub, and repaired to the current version.
func TestStaleListedReplicaIsDetectedAndRepaired(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 3, 3)
	h.coord.Put(ctx, "obj", bytes.NewReader([]byte("old bytes")), 0)
	stale := "node-2"
	old := h.save(stale, "obj", 1)
	v2 := []byte("new bytes, v2")
	h.coord.Put(ctx, "obj", bytes.NewReader(v2), 0)
	h.remove(stale, "obj", 2)
	h.restore(stale, "obj", 1, old)

	// Never served: a read directed at the stale node falls back.
	r, err := h.coord.Get(ctx, "obj", ReadOptions{PreferNode: stale})
	if err != nil || !bytes.Equal(readAll(t, r), v2) || r.ServedBy == stale {
		t.Fatalf("read: %v served by %s", err, r.ServedBy)
	}

	rep, err := h.coord.Scrub(ctx, 2)
	if err != nil || rep.Stale != 1 || rep.StaleVersionsPurged != 1 {
		t.Fatalf("scrub %+v %v", rep, err)
	}
	rs, _ := h.assess("obj").Replica(stale)
	if rs.State != durability.ReplicaStale || rs.NodeState != storage.StateHealthy {
		t.Fatalf("replica state %+v", rs)
	}
	for _, ev := range []string{"STALE_REPLICA_DETECTED", "STALE_REPLICA_REPAIRED"} {
		if !strings.Contains(h.logs.String(), `"event":"`+ev+`"`) {
			t.Fatalf("no %s event", ev)
		}
	}

	res, err := h.coord.RepairObject(ctx, "obj")
	if err != nil || !res.Restored || !slices.Equal(res.Targets, []string{stale}) || res.Source == stale {
		t.Fatalf("repair %+v %v", res, err)
	}
	h.mustHold(stale, "obj", 2, v2)
	if got := h.versionsOn(stale, "obj"); !slices.Equal(got, []uint64{2}) {
		t.Fatalf("stale node holds %v", got)
	}
	cr, err := h.coord.Consistency(ctx, "obj", true)
	if err != nil || !cr.Consistent || cr.CurrentReplicas != 3 {
		t.Fatalf("consistency %+v %v", cr, err)
	}
}

// Old bytes on a node that missed the delete never resurrect the object.
func TestTombstonePreventsResurrection(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 3, 3)
	put, _ := h.coord.Put(ctx, "obj", bytes.NewReader([]byte("doomed")), 0)
	returning := put.Object.Replicas[0]
	kept := h.save(returning, "obj", 1)
	if _, err := h.coord.Delete(ctx, "obj"); err != nil {
		t.Fatal(err)
	}
	h.restore(returning, "obj", 1, kept) // node "comes back" with its old copy

	if _, err := h.coord.Get(ctx, "obj", ReadOptions{}); !errors.Is(err, metadata.ErrNotFound) {
		t.Fatalf("deleted object readable: %v", err)
	}
	cr, _ := h.coord.Consistency(ctx, "obj", true)
	if cr.State != "deleted" || cr.Consistent || len(cr.Replicas) != 1 || cr.Replicas[0].State != ConsistencyStale || cr.Replicas[0].Authoritative {
		t.Fatalf("consistency %+v", cr)
	}
	if _, err := h.coord.RepairObject(ctx, "obj"); err == nil {
		t.Fatal("repair of a deleted object must not succeed")
	}
	rep, _ := h.coord.Scrub(ctx, 2)
	if rep.StaleVersionsPurged != 1 || len(h.versionsOn(returning, "obj")) != 0 {
		t.Fatalf("scrub did not purge pre-tombstone data: %+v", rep)
	}
	if cr, _ := h.coord.Consistency(ctx, "obj", true); !cr.Consistent {
		t.Fatalf("after purge: %+v", cr)
	}
	res, err := h.coord.Put(ctx, "obj", bytes.NewReader([]byte("new life")), 0)
	if err != nil || res.Object.Version != 3 {
		t.Fatalf("put after tombstone: %+v %v", res, err)
	}
	r, _ := h.coord.Get(ctx, "obj", ReadOptions{})
	if string(readAll(t, r)) != "new life" {
		t.Fatal("wrong bytes after re-create")
	}
}

// A coordinator crash between "copies verified" and "metadata committed"
// leaves durable metadata exactly as it was: the half-done repair is not
// visible to a restarted coordinator.
func TestCrashBeforeRepairCommitLeavesMetadataUntouched(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, 4, 3)
	put, _ := h.coord.Put(ctx, "obj", bytes.NewReader(payload(9000, 1)), 0)
	h.kill(put.Object.Replicas[0], true)

	inside, release := make(chan struct{}), make(chan struct{})
	h.coord.hooks.beforeRepairCommit = func(string) { close(inside); <-release }
	done := make(chan repairOutcome, 1)
	go func() {
		res, err := h.coord.RepairObject(ctx, "obj")
		done <- repairOutcome{res.Targets, err}
	}()
	<-inside

	// What a restarted coordinator would load right now.
	crashed, err := metadata.OpenFileStore(h.metaDir)
	if err != nil {
		t.Fatal(err)
	}
	onDisk, _ := crashed.GetObject(ctx, "obj")
	if !slices.Equal(onDisk.Replicas, put.Object.Replicas) || onDisk.LastRepairedAt != nil {
		t.Fatalf("partial repair visible on disk: %+v", onDisk)
	}
	close(release)
	out := <-done
	h.coord.hooks.beforeRepairCommit = nil
	if out.err != nil || len(out.targets) != 1 {
		t.Fatalf("repair after release: %+v", out)
	}
}

type repairOutcome struct {
	targets []string
	err     error
}
