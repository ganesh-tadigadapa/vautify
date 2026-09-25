package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"vault/internal/cluster"
	"vault/internal/coordinator"
	"vault/internal/durability"
	"vault/internal/storage"
)

func (e *env) consistency(id string) coordinator.ConsistencyReport {
	var c coordinator.ConsistencyReport
	e.getJSON("/objects/"+id+"/consistency", &c)
	return c
}

func (e *env) replicaOn(c coordinator.ConsistencyReport, nodeID string) (coordinator.ReplicaConsistency, bool) {
	for _, r := range c.Replicas {
		if r.NodeID == nodeID {
			return r, true
		}
	}
	return coordinator.ReplicaConsistency{}, false
}

func (e *env) versionsHeld(nodeID, id string) []uint64 {
	e.t.Helper()
	reps, err := e.nodeClient(nodeID).ListReplicas(context.Background(), id)
	if err != nil {
		e.t.Fatal(err)
	}
	var out []uint64
	for _, r := range reps {
		out = append(out, r.Version)
	}
	return out
}

func (e *env) putStatus(path string, data []byte) response {
	e.t.Helper()
	return e.do(http.MethodPut, path, data)
}

// P2 gap: metadata written by a larger cluster names node IDs this API is
// no longer configured with (the stale Docker volume case). Reads, repairs
// and deletes must treat them as unavailable — never crash — and repair
// must re-home the affected replicas onto configured nodes.
func TestUnknownNodeIDsInMetadataAfterReconfiguration(t *testing.T) {
	dir := t.TempDir()
	spec := func(nodes int) cluster.Spec {
		return cluster.Spec{
			Dir: dir, Nodes: nodes, ReplicationFactor: 3, ProbeInterval: 200 * time.Millisecond,
			AutoRepair: true, EnableChaos: true, SweepInterval: 300 * time.Millisecond,
		}
	}
	e5 := launch(t, spec(5))
	payloads := map[string][]byte{}
	for i := 0; i < 12; i++ {
		id := fmt.Sprintf("old-%02d", i)
		payloads[id] = randomBytes(20_000+i, int64(200+i))
		e5.put(id, payloads[id])
	}
	e5.c.Stop()
	// Same api + node-1..3 directories; node-4 and node-5 no longer exist.
	e := launch(t, spec(3))

	for id, want := range payloads {
		r := e.do(http.MethodGet, "/objects/"+id, nil)
		e.mustStatus(r, http.StatusOK, "GET "+id)
		if !bytes.Equal(r.Body, want) {
			t.Fatalf("%s: wrong bytes", id)
		}
		e.getJSON("/objects/"+id+"/consistency", new(coordinator.ConsistencyReport))
		e.getJSON("/objects/"+id+"/health", new(map[string]any))
	}
	e.post("/scrub/run", http.StatusOK, nil)
	e.waitAllHealthy(30 * time.Second)
	for id := range payloads {
		m := e.metadata(id)
		for _, n := range m.Replicas {
			if n == "node-4" || n == "node-5" {
				t.Fatalf("%s still relies on unconfigured %s", id, n)
			}
		}
	}
	e.mustStatus(e.do(http.MethodDelete, "/objects/old-00", nil), http.StatusOK, "DELETE")
	if !e.c.API.Running() {
		t.Fatal("api process died")
	}
	if b, _ := os.ReadFile(e.c.API.LogPath); bytes.Contains(b, []byte("panic")) {
		t.Fatal("api log contains a panic")
	}
}

// A node that was down during an overwrite returns holding the old
// version. It is never authoritative: not listed, never read, reported
// STALE, purged by scrub.
func TestReturningNodeWithOldVersionIsNeverAuthoritative(t *testing.T) {
	// Pins the Phase 3 scrub purge, so Phase 4's reconcile-on-recovery
	// (which would clean the returning node first) is off here.
	e := startSelfHealing(t, 4, healOpts{extra: []string{"--reconcile-on-recovery=false"}})
	all := nodeNames(4)
	id := nameOn(t, "ret", all, []string{"node-1", "node-2", "node-3"})
	e.put(id, []byte("version one"))

	if err := e.c.StopNode("node-2"); err != nil {
		t.Fatal(err)
	}
	e.waitNodeState("node-2", storage.StateUnavailable)
	v2 := []byte("version two, written while node-2 was down")
	m2 := e.put(id, v2)
	if m2.Version != 2 || slices.Contains(m2.Replicas, "node-2") {
		t.Fatalf("v2 metadata %+v", m2)
	}
	if err := e.c.StartNode(context.Background(), "node-2"); err != nil {
		t.Fatal(err)
	}
	e.waitNodeState("node-2", storage.StateHealthy)
	if held := e.versionsHeld("node-2", id); !slices.Equal(held, []uint64{1}) {
		t.Fatalf("premise: returning node should hold v1, holds %v", held)
	}

	r := e.do(http.MethodGet, "/objects/"+id, nil)
	if !bytes.Equal(r.Body, v2) || r.Header.Get(storage.HeaderVersion) != "2" {
		t.Fatalf("read returned %q v%s", r.Body, r.Header.Get(storage.HeaderVersion))
	}
	e.mustStatus(e.do(http.MethodGet, "/objects/"+id+"?prefer=node-2", nil), http.StatusBadRequest, "prefer stale node")

	c := e.consistency(id)
	rc, ok := e.replicaOn(c, "node-2")
	if !ok || rc.State != coordinator.ConsistencyStale || rc.Listed || rc.Authoritative || rc.Version != 1 || c.Consistent {
		t.Fatalf("consistency for node-2: %+v (report %+v)", rc, c)
	}
	if c.CurrentVersion != 2 || c.CurrentReplicas != 3 {
		t.Fatalf("report %+v", c)
	}

	var rep coordinator.ScrubReport
	e.post("/scrub/run", http.StatusOK, &rep)
	if rep.StaleVersionsPurged != 1 {
		t.Fatalf("scrub %+v", rep)
	}
	if held := e.versionsHeld("node-2", id); len(held) != 0 {
		t.Fatalf("stale v1 not purged: %v", held)
	}
	if len(e.eventsFor("STALE_REPLICA_DETECTED", id)) == 0 || len(e.eventsFor("STALE_REPLICA_REPAIRED", id)) == 0 {
		t.Fatal("missing stale replica events")
	}
	if c := e.consistency(id); !c.Consistent {
		t.Fatalf("not consistent after purge: %+v", c)
	}
}

// A node that missed a DELETE comes back with the old bytes. The tombstone
// wins: the object stays deleted, repair does not touch it, scrub purges
// the bytes, and a later PUT creates a strictly newer live version.
func TestTombstonePreventsResurrectionAcrossNodeAndAPIRestarts(t *testing.T) {
	// Pins the Phase 3 scrub purge, so Phase 4's reconcile-on-recovery
	// (which would clean the returning node first) is off here.
	e := startSelfHealing(t, 4, healOpts{extra: []string{"--reconcile-on-recovery=false"}})
	id := nameOn(t, "tomb", nodeNames(4), []string{"node-1", "node-2", "node-3"})
	e.put(id, []byte("to be deleted"))
	// Hold repair so node-2 is still a listed replica when it misses the delete.
	e.post("/repair/pause", http.StatusOK, nil)
	if err := e.c.StopNode("node-2"); err != nil {
		t.Fatal(err)
	}
	e.waitNodeState("node-2", storage.StateUnavailable)

	var del coordinator.DeleteResult
	r := e.do(http.MethodDelete, "/objects/"+id, nil)
	e.mustStatus(r, http.StatusOK, "DELETE")
	json.Unmarshal(r.Body, &del)
	if del.TombstoneVersion != 2 || del.FullyPurged || !slices.Equal(del.Unreachable, []string{"node-2"}) {
		t.Fatalf("delete %+v", del)
	}
	if len(e.eventsFor("TOMBSTONE_CREATED", id)) != 1 {
		t.Fatal("no TOMBSTONE_CREATED")
	}
	e.post("/repair/resume", http.StatusOK, nil)

	if err := e.c.StartNode(context.Background(), "node-2"); err != nil {
		t.Fatal(err)
	}
	e.waitNodeState("node-2", storage.StateHealthy)
	if held := e.versionsHeld("node-2", id); !slices.Equal(held, []uint64{1}) {
		t.Fatalf("premise: node-2 should still hold v1, holds %v", held)
	}
	assertGone := func(when string) {
		e.mustStatus(e.do(http.MethodGet, "/objects/"+id, nil), http.StatusNotFound, "GET "+when)
		e.mustStatus(e.do(http.MethodHead, "/objects/"+id, nil), http.StatusNotFound, "HEAD "+when)
		e.mustStatus(e.do(http.MethodGet, "/objects/"+id+"/metadata", nil), http.StatusNotFound, "metadata "+when)
	}
	assertGone("after node return")
	e.post("/repair/sweep", http.StatusOK, nil)
	e.post("/objects/"+id+"/repair", http.StatusNotFound, nil)
	// A repair queued before the delete may still run, but it must be
	// superseded by the tombstone and write nothing.
	for _, h := range historyFor(e.history(), id) {
		if h.Outcome != "superseded" || len(h.Result.Targets) != 0 || h.Result.BytesTransferred != 0 {
			t.Fatalf("a deleted object was repaired: %+v", h)
		}
	}
	c := e.consistency(id)
	if rc, _ := e.replicaOn(c, "node-2"); c.State != "deleted" || rc.State != coordinator.ConsistencyStale || rc.Authoritative {
		t.Fatalf("consistency %+v", c)
	}

	// The tombstone survives an API crash.
	if err := e.c.KillAPI(); err != nil {
		t.Fatal(err)
	}
	if err := e.c.StartAPI(context.Background()); err != nil {
		t.Fatal(err)
	}
	e.c.WaitHealthyNodes(context.Background(), 4, 10*time.Second)
	assertGone("after api restart")

	e.post("/scrub/run", http.StatusOK, nil)
	if held := e.versionsHeld("node-2", id); len(held) != 0 {
		t.Fatalf("pre-tombstone bytes not purged: %v", held)
	}
	if c := e.consistency(id); !c.Consistent {
		t.Fatalf("deleted object not consistent after purge: %+v", c)
	}

	r = e.putStatus("/objects/"+id, []byte("brand new life"))
	e.mustStatus(r, http.StatusCreated, "PUT after tombstone")
	if m := e.metadata(id); m.Version != 3 {
		t.Fatalf("new life version %d, want 3", m.Version)
	}
	if r := e.do(http.MethodGet, "/objects/"+id, nil); string(r.Body) != "brand new life" {
		t.Fatalf("GET %q", r.Body)
	}
}

// Kill the coordinator (SIGKILL) while a repair has written and verified
// its copy but not committed: after restart, metadata shows no trace of the
// partial repair; with repair re-enabled, it completes properly.
func TestCoordinatorCrashDuringRepairCommitsNothing(t *testing.T) {
	e := startSelfHealing(t, 4, healOpts{extra: []string{"--test-repair-commit-delay=60s"}})
	all := nodeNames(4)
	id := nameOn(t, "crash", all, []string{"node-1", "node-2", "node-3"})
	data := randomBytes(300_000, 300)
	before := e.put(id, data)

	e.kill("node-2")
	var target string
	e.waitFor("repair copy verified, commit pending", 15*time.Second, func() bool {
		evs := e.eventsFor("REPAIR_CHECKSUM_VERIFIED", id)
		if len(evs) == 1 {
			target, _ = evs[0].Fields["target"].(string)
		}
		return target != ""
	})
	if target != "node-4" {
		t.Fatalf("premise: target should be the only fresh node, got %q", target)
	}
	if held := e.versionsHeld(target, id); !slices.Equal(held, []uint64{1}) {
		t.Fatalf("premise: copy should be on %s, holds %v", target, held)
	}

	if err := e.c.KillAPI(); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := e.c.StartAPI(ctx, "--auto-repair=false", "--test-repair-commit-delay=0s"); err != nil {
		t.Fatal(err)
	}
	e.c.WaitHealthyNodes(ctx, 3, 10*time.Second)

	m := e.metadata(id)
	if !slices.Equal(m.Replicas, before.Replicas) || m.LastRepairedAt != nil || slices.Contains(m.Replicas, target) {
		t.Fatalf("partial repair became metadata: %+v", m)
	}
	c := e.consistency(id)
	if rc, _ := e.replicaOn(c, target); rc.Listed || rc.Authoritative {
		t.Fatalf("uncommitted copy on %s treated as authoritative: %+v", target, rc)
	}
	if h := e.health(id); h.State != durability.Degraded {
		t.Fatalf("state %s, want DEGRADED", h.State)
	}

	if err := e.c.RestartAPI(ctx, "--test-repair-commit-delay=0s"); err != nil {
		t.Fatal(err)
	}
	e.c.WaitHealthyNodes(ctx, 3, 10*time.Second)
	e.waitAllHealthy(20 * time.Second)
	m = e.metadata(id)
	if !slices.Contains(m.Replicas, target) || slices.Contains(m.Replicas, "node-2") {
		t.Fatalf("after completed repair: %+v", m)
	}
	if r := e.do(http.MethodGet, "/objects/"+id, nil); !bytes.Equal(r.Body, data) {
		t.Fatal("wrong bytes")
	}
}

// Deterministic mid-repair races (via the commit-delay fault injection):
// a newer PUT, or a DELETE, lands while the repair's copy awaits commit.
// The repair must be superseded and install nothing.
func TestWriteOrDeleteDuringRepairSupersedesIt(t *testing.T) {
	for _, op := range []string{"put", "delete"} {
		t.Run(op, func(t *testing.T) {
			e := startSelfHealing(t, 4, healOpts{extra: []string{"--test-repair-commit-delay=2s"}})
			id := nameOn(t, "sup-"+op, nodeNames(4), []string{"node-1", "node-2", "node-3"})
			e.put(id, []byte("v1 being repaired"))
			e.kill("node-2")
			e.waitFor("copy verified", 15*time.Second, func() bool { return len(e.eventsFor("REPAIR_CHECKSUM_VERIFIED", id)) == 1 })

			v2 := []byte("v2 written mid-repair")
			if op == "put" {
				e.mustStatus(e.putStatus("/objects/"+id, v2), http.StatusOK, "PUT mid-repair")
			} else {
				e.mustStatus(e.do(http.MethodDelete, "/objects/"+id, nil), http.StatusOK, "DELETE mid-repair")
			}
			e.waitFor("REPAIR_SUPERSEDED", 15*time.Second, func() bool { return len(e.eventsFor("REPAIR_SUPERSEDED", id)) == 1 })

			if held := e.versionsHeld("node-4", id); slices.Contains(held, 1) {
				t.Fatalf("stale v1 copy left installed on node-4: %v", held)
			}
			if op == "put" {
				m := e.metadata(id)
				if m.Version != 2 || m.SHA256 != sha(v2) {
					t.Fatalf("metadata %+v", m)
				}
				if r := e.do(http.MethodGet, "/objects/"+id, nil); !bytes.Equal(r.Body, v2) {
					t.Fatal("read did not return v2")
				}
			} else {
				e.mustStatus(e.do(http.MethodGet, "/objects/"+id, nil), http.StatusNotFound, "GET after delete")
				if c := e.consistency(id); c.State != "deleted" || c.CurrentVersion != 2 {
					t.Fatalf("consistency %+v", c)
				}
			}
			if hs := historyFor(e.history(), id); len(hs) != 1 || hs[0].Outcome != "superseded" {
				t.Fatalf("history %+v", hs)
			}
		})
	}
}

// P2 regression: a DEGRADED object queued first, a CRITICAL object queued
// strictly later — the CRITICAL one must start first.
func TestPriorityDegradedQueuedFirstCriticalStartsFirst(t *testing.T) {
	e := startSelfHealing(t, 6, healOpts{workers: 1})
	all := nodeNames(6)
	degraded := nameOn(t, "p-degraded", all, []string{"node-1", "node-2", "node-3"})
	critical := nameOn(t, "p-critical", all, []string{"node-1", "node-4", "node-5"})
	e.put(degraded, randomBytes(100_000, 400))
	e.put(critical, randomBytes(100_000, 401))

	e.post("/repair/pause", http.StatusOK, nil)
	e.kill("node-2")
	e.waitFor("degraded object queued", 10*time.Second, func() bool {
		q := e.queue().Queued
		return len(q) == 1 && q[0].ObjectID == degraded
	})
	time.Sleep(20 * time.Millisecond)
	e.kill("node-4")
	e.kill("node-5")
	e.waitFor("critical object queued", 10*time.Second, func() bool {
		q := e.queue().Queued
		return len(q) == 2 && q[0].Priority == durability.Critical
	})
	q := e.queue().Queued
	if q[0].ObjectID != critical || q[1].ObjectID != degraded || !q[1].QueuedAt.Before(q[0].QueuedAt) {
		t.Fatalf("queue %+v (degraded must have been queued strictly first)", q)
	}

	e.post("/repair/resume", http.StatusOK, nil)
	e.waitAllHealthy(20 * time.Second)
	h := e.history()
	if len(h) < 2 || h[0].ObjectID != critical || !h[0].StartedAt.Before(h[1].StartedAt) {
		t.Fatalf("start order %+v", h)
	}
}

func TestCASOverHTTP(t *testing.T) {
	e := startCluster(t, 3, 3)
	e.mustStatus(e.putStatus("/objects/doc?expected_version=0", []byte("a")), http.StatusCreated, "create-only")
	r := e.putStatus("/objects/doc?expected_version=0", []byte("b"))
	e.mustStatus(r, http.StatusConflict, "create-only on existing")
	var body struct {
		Error struct {
			Code    string
			Details coordinator.ConflictError
		}
	}
	json.Unmarshal(r.Body, &body)
	if body.Error.Code != "write_conflict" || body.Error.Details.CurrentVersion != 1 || body.Error.Details.CurrentState != "live" {
		t.Fatalf("conflict body %s", r.Body)
	}
	req, _ := http.NewRequest(http.MethodPut, e.c.API.URL+"/objects/doc", strings.NewReader("c"))
	req.Header.Set(storage.HeaderExpectedVersion, "1")
	resp, _ := e.hc.Do(req)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("header CAS: %d", resp.StatusCode)
	}
	e.mustStatus(e.do(http.MethodDelete, "/objects/doc?expected_version=1", nil), http.StatusConflict, "stale CAS delete")
	e.mustStatus(e.putStatus("/objects/doc?expected_version=abc", []byte("x")), http.StatusBadRequest, "bad expected_version")
	e.mustStatus(e.do(http.MethodDelete, "/objects/doc?expected_version=2", nil), http.StatusOK, "CAS delete")
	if len(e.logEvents(e.c.API, "WRITE_CONFLICT")) != 2 {
		t.Fatal("expected 2 WRITE_CONFLICT events")
	}

	// Lost-update test over real HTTP: every CAS increment survives.
	e.mustStatus(e.putStatus("/objects/counter", []byte("0")), http.StatusCreated, "counter")
	const clients = 10
	var wg sync.WaitGroup
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for attempt := 0; attempt < 200; attempt++ {
				resp, err := e.hc.Get(e.c.API.URL + "/objects/counter")
				if err != nil {
					t.Error(err)
					return
				}
				var buf bytes.Buffer
				buf.ReadFrom(resp.Body)
				resp.Body.Close()
				n, _ := strconv.Atoi(buf.String())
				req, _ := http.NewRequest(http.MethodPut,
					e.c.API.URL+"/objects/counter?expected_version="+resp.Header.Get(storage.HeaderVersion),
					strings.NewReader(strconv.Itoa(n+1)))
				pr, err := e.hc.Do(req)
				if err != nil {
					t.Error(err)
					return
				}
				pr.Body.Close()
				if pr.StatusCode == http.StatusOK {
					return
				}
				if pr.StatusCode != http.StatusConflict {
					t.Errorf("CAS increment: HTTP %d", pr.StatusCode)
					return
				}
			}
			t.Error("CAS increment never succeeded")
		}()
	}
	wg.Wait()
	r = e.do(http.MethodGet, "/objects/counter", nil)
	if string(r.Body) != strconv.Itoa(clients) || r.Header.Get(storage.HeaderVersion) != strconv.Itoa(1+clients) {
		t.Fatalf("counter %q at v%s, want %d at v%d", r.Body, r.Header.Get(storage.HeaderVersion), clients, 1+clients)
	}
	for _, n := range e.c.Nodes {
		if held := e.versionsHeld(n.Name, "counter"); !slices.Equal(held, []uint64{1 + clients}) {
			t.Fatalf("%s holds %v: failed CAS attempts left bytes", n.Name, held)
		}
	}
}
