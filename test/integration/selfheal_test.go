package integration

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"vault/internal/coordinator"
	"vault/internal/durability"
	"vault/internal/placement"
	"vault/internal/repair"
	"vault/internal/storage"
)

// HARD GATE A: a real node process dies → objects degrade → automatic
// repair from verified sources onto real replacement nodes → RF restored.
// Also checks blast radius and repair metrics against what really happened.
func TestSelfHealNodeFailureRestoresRF(t *testing.T) {
	e := startSelfHealing(t, 4, healOpts{})
	all := nodeNames(4)

	type obj struct {
		id   string
		data []byte
	}
	objs := []obj{{id: nameOn(t, "onnode2", all, []string{"node-1", "node-2", "node-3"})}}
	for i := 0; i < 7; i++ {
		objs = append(objs, obj{id: fmt.Sprintf("file-%d", i)})
	}
	original := map[string][]string{}
	var affected []string
	var affectedBytes int64
	for i := range objs {
		objs[i].data = randomBytes(64<<10+i*1000, int64(10+i))
		m := e.put(objs[i].id, objs[i].data)
		original[m.ID] = m.Replicas
		if slices.Contains(m.Replicas, "node-2") {
			affected = append(affected, m.ID)
			affectedBytes += m.Size
		}
	}

	e.kill("node-2")
	e.waitAllHealthy(20 * time.Second)

	for _, o := range objs {
		m := e.metadata(o.id)
		if len(m.Replicas) != 3 || slices.Contains(m.Replicas, "node-2") {
			t.Fatalf("%s replicas after repair: %v", o.id, m.Replicas)
		}
		for _, n := range m.Replicas {
			b, err := os.ReadFile(e.replicaPath(n, o.id, m.Version))
			if err != nil || sha(b) != m.SHA256 {
				t.Fatalf("%s: replica on %s missing or wrong on disk (err=%v)", o.id, n, err)
			}
		}
		r := e.do(http.MethodGet, "/objects/"+o.id, nil)
		e.mustStatus(r, http.StatusOK, "GET "+o.id)
		if !bytes.Equal(r.Body, o.data) {
			t.Fatalf("%s: wrong bytes after repair", o.id)
		}
		if h := e.health(o.id); h.State != durability.Healthy || h.HealthyReplicas != 3 {
			t.Fatalf("%s health %+v", o.id, h.ObjectHealth)
		}
	}

	hist := e.history()
	for _, id := range affected {
		hs := historyFor(hist, id)
		if len(hs) != 1 || hs[0].Outcome != "restored" {
			t.Fatalf("%s history %+v", id, hs)
		}
		r := hs[0].Result
		if r.HealthyBefore != 2 || r.HealthyAfter != 3 || r.Source == "node-2" || !slices.Contains(original[id], r.Source) {
			t.Fatalf("%s result %+v", id, r)
		}
		if len(r.Targets) != 1 || slices.Contains(original[id], r.Targets[0]) {
			t.Fatalf("%s target %v not a fresh node (original %v)", id, r.Targets, original[id])
		}
		if len(e.eventsFor("OBJECT_DEGRADED", id)) == 0 || len(e.eventsFor("REPAIR_CHECKSUM_VERIFIED", id)) != 1 ||
			len(e.eventsFor("REPAIR_METADATA_COMMITTED", id)) != 1 || len(e.eventsFor("OBJECT_RESTORED", id)) == 0 {
			t.Fatalf("%s: missing lifecycle events", id)
		}
	}

	// Blast radius computed at failure time matches metadata.
	var br []map[string]any
	for _, ev := range e.events("BLAST_RADIUS_CALCULATED") {
		if ev.Fields["node_id"] == "node-2" {
			br = append(br, ev.Fields)
		}
	}
	if len(br) == 0 || int(br[0]["affected_objects"].(float64)) != len(affected) || int(br[0]["degraded"].(float64)) != len(affected) {
		t.Fatalf("blast radius events %+v, affected %d", br, len(affected))
	}

	// Metrics are the sum of real work.
	m := e.metrics().Repair
	if m.Completed != int64(len(affected)) || m.ReplicasCreated != int64(len(affected)) ||
		m.BytesTransferred != affectedBytes || m.RecoveryTime.Count != int64(len(affected)) || m.Failed != 0 || m.QueueDepth != 0 {
		t.Fatalf("metrics %+v (affected %d, bytes %d)", m, len(affected), affectedBytes)
	}
}

// HARD GATE B: silent corruption on a live node → scrub detects it →
// replica CORRUPTED (node still HEALTHY) → repair from a verified source →
// verified replacement → HEALTHY. The corrupted replica is first in
// metadata order, so it would be the first source candidate if it were not
// excluded.
func TestSelfHealSilentCorruptionScrubRepair(t *testing.T) {
	e := startSelfHealing(t, 4, healOpts{})
	data := randomBytes(1<<20, 20)
	obj := e.put("ledger.db", data)
	victim := obj.Replicas[0]
	pid := e.c.Node(victim).PID()

	path := e.replicaPath(victim, "ledger.db", obj.Version)
	b, _ := os.ReadFile(path)
	for i := 5000; i < 5016; i++ {
		b[i] ^= 0xFF
	}
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
	if h := e.health("ledger.db"); h.State != durability.Healthy {
		t.Fatalf("before scrub Vault cannot know; state = %s", h.State)
	}

	e.post("/repair/pause", http.StatusOK, nil)
	var rep coordinator.ScrubReport
	e.post("/scrub/run", http.StatusOK, &rep)
	if rep.Corrupted != 1 || rep.Verified != 2 || rep.Findings[0].NodeID != victim || rep.Findings[0].Outcome != "corrupted" {
		t.Fatalf("scrub report %+v", rep)
	}
	h := e.health("ledger.db")
	vr, _ := h.Replica(victim)
	if h.State != durability.Degraded || vr.State != durability.ReplicaCorrupted || vr.NodeState != storage.StateHealthy {
		t.Fatalf("after scrub: %s, victim %+v", h.State, vr)
	}
	if h.Repair == nil || h.Repair.Status != repair.StatusQueued {
		t.Fatalf("not queued: %+v", h.Repair)
	}
	e.post("/repair/resume", http.StatusOK, nil)
	e.waitAllHealthy(20 * time.Second)

	hs := historyFor(e.history(), "ledger.db")
	if len(hs) != 1 || hs[0].Outcome != "restored" || hs[0].Result.Source == victim || len(hs[0].Result.Targets) != 1 {
		t.Fatalf("history %+v", hs)
	}
	target := hs[0].Result.Targets[0]
	for _, ev := range e.eventsFor("REPAIR_SOURCE_SELECTED", "ledger.db") {
		if ev.Fields["source"] == victim {
			t.Fatal("corrupted replica selected as repair source")
		}
	}
	tb, err := os.ReadFile(e.replicaPath(target, "ledger.db", obj.Version))
	if err != nil || sha(tb) != obj.SHA256 {
		t.Fatalf("replacement on %s not verified on disk (err=%v)", target, err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("corrupted replica bytes were not removed from the victim")
	}
	m := e.metadata("ledger.db")
	if slices.Contains(m.Replicas, victim) || !slices.Contains(m.Replicas, target) || len(m.IntegrityFailures) != 0 {
		t.Fatalf("metadata %+v", m)
	}
	if e.c.Node(victim).PID() != pid {
		t.Fatal("victim node process changed; it must have stayed alive throughout")
	}

	// The lifecycle happened in order.
	order := []string{"SCRUB_CORRUPTION_DETECTED", "REPAIR_QUEUED", "REPAIR_STARTED", "REPAIR_SOURCE_SELECTED",
		"REPAIR_REPLICA_WRITTEN", "REPAIR_CHECKSUM_VERIFIED", "REPAIR_METADATA_COMMITTED", "REPAIR_COMPLETED", "OBJECT_RESTORED"}
	var last uint64
	for _, name := range order {
		s := seqOf(t, e.eventsFor(name, "ledger.db"), name)
		if s <= last {
			t.Fatalf("%s (seq %d) happened before the previous step (seq %d)", name, s, last)
		}
		last = s
	}
	r := e.do(http.MethodGet, "/objects/ledger.db", nil)
	if !bytes.Equal(r.Body, data) {
		t.Fatal("wrong bytes after repair")
	}
}

// With no spare node, a corrupted replica on a live node is rewritten in
// place from a verified source.
func TestSelfHealCorruptionInPlaceOnThreeNodes(t *testing.T) {
	e := startSelfHealing(t, 3, healOpts{})
	data := randomBytes(200_000, 21)
	obj := e.put("small.bin", data)
	victim := obj.Replicas[1]
	var cr storage.CorruptResult
	e.post("/chaos/corrupt-replica/small.bin/"+victim, http.StatusOK, &cr)
	e.post("/scrub/run", http.StatusOK, nil)
	e.waitAllHealthy(20 * time.Second)

	hs := historyFor(e.history(), "small.bin")
	if len(hs) != 1 || !slices.Equal(hs[0].Result.Targets, []string{victim}) || hs[0].Result.Source == victim {
		t.Fatalf("history %+v", hs)
	}
	vr, err := e.nodeClient(victim).VerifyReplica(context.Background(), "small.bin", obj.Version)
	if err != nil || !vr.Valid || vr.Actual.SHA256 != obj.SHA256 {
		t.Fatalf("victim replica after in-place repair: %+v %v", vr, err)
	}
	if m := e.metadata("small.bin"); !slices.Equal(m.Replicas, obj.Replicas) || len(m.IntegrityFailures) != 0 {
		t.Fatalf("metadata %+v", m)
	}
}

// Corruption found by an ordinary client read also triggers repair.
func TestSelfHealRepairsCorruptionFoundByRead(t *testing.T) {
	e := startSelfHealing(t, 4, healOpts{})
	data := randomBytes(100_000, 22)
	obj := e.put("read.bin", data)
	e.post("/chaos/corrupt-replica/read.bin/"+obj.Replicas[0], http.StatusOK, nil)
	r := e.do(http.MethodGet, "/objects/read.bin", nil)
	e.mustStatus(r, http.StatusOK, "GET")
	if rejected(r.Header)[obj.Replicas[0]] != "checksum_mismatch" || !bytes.Equal(r.Body, data) {
		t.Fatalf("read: %v", r.Header)
	}
	e.waitAllHealthy(20 * time.Second)
	if hs := historyFor(e.history(), "read.bin"); len(hs) != 1 || hs[0].Outcome != "restored" {
		t.Fatalf("history %+v", hs)
	}
}

// HARD GATE C: an object with one surviving replica is repaired before one
// with two — even though the two-replica object was queued first and sorts
// first by ID, so FIFO or ID order would pick it.
func TestSelfHealPriorityCriticalBeforeDegraded(t *testing.T) {
	e := startSelfHealing(t, 5, healOpts{workers: 1})
	all := nodeNames(5)
	critical := nameOn(t, "zzz-critical", all, []string{"node-1", "node-2", "node-3"})
	degraded := nameOn(t, "aaa-degraded", all, []string{"node-3", "node-4", "node-5"})
	protected := nameOn(t, "mmm-protected", all, []string{"node-1", "node-4", "node-5"})
	for i, id := range []string{critical, degraded, protected} {
		e.put(id, randomBytes(300_000, int64(30+i)))
	}

	e.post("/repair/pause", http.StatusOK, nil)
	e.kill("node-3") // both objects lose one replica: both DEGRADED, queued together
	e.waitFor("both queued", 10*time.Second, func() bool { return e.queue().Depth == 2 })
	e.kill("node-2") // the critical object loses a second replica
	e.waitFor("escalation to CRITICAL", 10*time.Second, func() bool {
		q := e.queue().Queued
		return len(q) == 2 && q[0].Priority == durability.Critical
	})

	q := e.queue().Queued
	if q[0].ObjectID != critical || q[1].ObjectID != degraded || q[1].Priority != durability.Degraded {
		t.Fatalf("queue order %+v", q)
	}
	mustContain(t, q[0].Reason, "Only 1 verified replica remains for RF=3")
	if q[1].QueuedAt.After(q[0].QueuedAt) {
		t.Fatalf("test premise: degraded object should have been queued no later (FIFO would pick it): %v vs %v", q[1].QueuedAt, q[0].QueuedAt)
	}
	if h := e.health(critical); h.State != durability.Critical || h.HealthyReplicas != 1 {
		t.Fatalf("critical object %+v", h.ObjectHealth)
	}
	if h := e.health(degraded); h.State != durability.Degraded || h.HealthyReplicas != 2 {
		t.Fatalf("degraded object %+v", h.ObjectHealth)
	}
	if h := e.health(protected); h.State != durability.Healthy {
		t.Fatalf("protected object %+v", h.ObjectHealth)
	}

	e.post("/repair/resume", http.StatusOK, nil)
	e.waitAllHealthy(20 * time.Second)

	hist := e.history()
	if len(hist) < 2 || hist[0].ObjectID != critical || hist[0].Priority != durability.Critical || hist[1].ObjectID != degraded {
		t.Fatalf("repair order %+v", hist)
	}
	if !hist[0].StartedAt.Before(hist[1].StartedAt) {
		t.Fatal("critical repair did not start first")
	}
	c := hist[0].Result
	if c.Source != "node-1" || len(c.Targets) != 2 || c.HealthyBefore != 1 || c.HealthyAfter != 3 {
		t.Fatalf("critical repair %+v", c)
	}
	for _, id := range []string{critical, degraded} {
		m := e.metadata(id)
		got := slices.Sorted(slices.Values(m.Replicas))
		if !slices.Equal(got, []string{"node-1", "node-4", "node-5"}) {
			t.Fatalf("%s replicas %v", id, m.Replicas)
		}
	}
}

// Blast-radius numbers equal an independent computation from metadata.
func TestSelfHealBlastRadiusMatchesMetadata(t *testing.T) {
	e := startSelfHealing(t, 5, healOpts{})
	for i := 0; i < 20; i++ {
		e.put(fmt.Sprintf("br-%02d", i), randomBytes(2000+i, int64(40+i)))
	}
	e.post("/repair/pause", http.StatusOK, nil)
	e.kill("node-2")
	e.kill("node-4")
	down := []string{"node-2", "node-4"}

	var list struct {
		Objects []struct {
			ID       string
			Replicas []string
			Size     int64
		}
	}
	e.getJSON("/objects", &list)
	var want durability.BlastRadius
	var total durability.Counts
	for _, o := range list.Objects {
		healthy := 0
		for _, n := range o.Replicas {
			if !slices.Contains(down, n) {
				healthy++
			}
		}
		st := durability.Classify(healthy, 3)
		total.Add(st)
		if !slices.Contains(o.Replicas, "node-2") {
			continue
		}
		want.AffectedObjects++
		want.AffectedBytes += o.Size
		switch st {
		case durability.Healthy:
			want.Protected++
		case durability.Degraded:
			want.Degraded++
		case durability.Critical:
			want.Critical++
		case durability.Lost:
			want.Lost++
		}
	}
	var got durability.BlastRadius
	e.getJSON("/nodes/node-2/blast-radius", &got)
	if got.AffectedObjects != want.AffectedObjects || got.AffectedBytes != want.AffectedBytes || got.Protected != want.Protected ||
		got.Degraded != want.Degraded || got.Critical != want.Critical || got.Lost != want.Lost || len(got.Objects) != want.AffectedObjects {
		t.Fatalf("blast radius %+v, independently computed %+v", got, want)
	}
	if got.Critical == 0 || got.Degraded == 0 {
		t.Fatalf("test premise: want a mix of critical and degraded, got %+v", want)
	}
	if st := e.status(); st.Objects != total || st.NodesHealthy != 3 {
		t.Fatalf("cluster status objects %+v, want %+v", st.Objects, total)
	}
	if m := e.metrics().Objects; m != total {
		t.Fatalf("metrics objects %+v, want %+v", m, total)
	}
	e.mustStatus(e.do(http.MethodGet, "/nodes/node-9/blast-radius", nil), http.StatusNotFound, "unknown node blast radius")
}

// HARD GATE F: duplicate repair requests never produce a second repair.
func TestSelfHealDuplicateRepairRequestsDeduplicated(t *testing.T) {
	e := startSelfHealing(t, 4, healOpts{workers: 4})
	id := nameOn(t, "dup", nodeNames(4), []string{"node-1", "node-2", "node-3"})
	e.put(id, randomBytes(16<<20, 50))
	e.post("/repair/pause", http.StatusOK, nil)
	e.kill("node-2")
	e.waitFor("queued", 10*time.Second, func() bool { return e.queue().Depth == 1 })

	hammer := func(n int) {
		var wg sync.WaitGroup
		for i := 0; i < n; i++ {
			wg.Add(2)
			go func() { defer wg.Done(); e.hc.Post(e.c.API.URL+"/objects/"+id+"/repair", "", nil) }()
			go func() { defer wg.Done(); e.hc.Post(e.c.API.URL+"/repair/sweep", "", nil) }()
		}
		wg.Wait()
	}
	hammer(20)
	if s := e.queue(); s.Depth != 1 || len(s.Active) != 0 {
		t.Fatalf("queue after duplicates: %+v", s)
	}
	e.post("/repair/resume", http.StatusOK, nil)
	hammer(20) // while the 16 MiB repair runs
	e.waitAllHealthy(30 * time.Second)

	if n := len(e.eventsFor("REPAIR_STARTED", id)); n != 1 {
		t.Fatalf("REPAIR_STARTED %d times, want 1", n)
	}
	if hs := historyFor(e.history(), id); len(hs) != 1 || hs[0].Outcome != "restored" {
		t.Fatalf("history %+v", hs)
	}
	if d := e.metrics().Repair.Deduplicated; d < 40 {
		t.Fatalf("deduplicated_total = %d, want >= 40", d)
	}
}

// Repairs of different objects run concurrently; one object never has
// overlapping repairs.
func TestSelfHealRepairsDifferentObjectsConcurrently(t *testing.T) {
	e := startSelfHealing(t, 5, healOpts{workers: 4})
	all := nodeNames(5)
	var ids []string
	for i := 0; len(ids) < 12; i++ {
		id := fmt.Sprintf("par-%d", i)
		if slices.Contains(placementOf(id, all), "node-2") {
			ids = append(ids, id)
			e.put(id, randomBytes(4<<20, int64(60+i)))
		}
	}
	e.post("/repair/pause", http.StatusOK, nil)
	e.kill("node-2")
	e.waitFor("all queued", 10*time.Second, func() bool { return e.queue().Depth == len(ids) })
	e.post("/repair/resume", http.StatusOK, nil)
	e.waitAllHealthy(30 * time.Second)

	hist := e.history()
	// Peak number of repairs in flight at once (sweep line over intervals).
	type edge struct {
		at    time.Time
		delta int
	}
	var edges []edge
	for _, h := range hist {
		edges = append(edges, edge{h.StartedAt, +1}, edge{h.CompletedAt, -1})
	}
	slices.SortFunc(edges, func(a, b edge) int {
		if c := a.at.Compare(b.at); c != 0 {
			return c
		}
		return a.delta - b.delta // ends before starts at the same instant
	})
	peak, cur := 0, 0
	for _, x := range edges {
		cur += x.delta
		peak = max(peak, cur)
	}
	if peak < 2 || peak > 4 {
		t.Fatalf("peak concurrent repairs %d, want 2..4 (workers=4)", peak)
	}
	for _, id := range ids {
		if hs := historyFor(hist, id); len(hs) != 1 {
			t.Fatalf("%s repaired %d times", id, len(hs))
		}
	}
}

func placementOf(id string, all []string) []string {
	return placement.Rank(id, all)[:3]
}

// HARD GATE H (multi-process): objects overwritten repeatedly while their
// repairs run never end up with stale bytes installed.
func TestSelfHealConcurrentRepairAndOverwrite(t *testing.T) {
	e := startSelfHealing(t, 4, healOpts{workers: 4})
	all := nodeNames(4)
	var ids []string
	for i := 0; len(ids) < 8; i++ {
		id := fmt.Sprintf("race-%d", i)
		if slices.Contains(placementOf(id, all), "node-2") {
			ids = append(ids, id)
			e.put(id, randomBytes(1<<20, int64(70+i)))
		}
	}
	e.kill("node-2")

	const rounds = 4
	final := make([][]byte, len(ids))
	var wg sync.WaitGroup
	for i, id := range ids {
		wg.Add(1)
		go func(i int, id string) {
			defer wg.Done()
			for r := 0; r < rounds; r++ {
				d := randomBytes(1<<20+r, int64(1000*i+r))
				req, _ := http.NewRequest(http.MethodPut, e.c.API.URL+"/objects/"+id, bytes.NewReader(d))
				resp, err := e.hc.Do(req)
				if err != nil {
					t.Error(err)
					return
				}
				resp.Body.Close()
				if resp.StatusCode != http.StatusOK {
					t.Errorf("%s round %d: HTTP %d", id, r, resp.StatusCode)
					return
				}
				final[i] = d
			}
		}(i, id)
	}
	wg.Wait()
	e.waitAllHealthy(30 * time.Second)

	ctx := context.Background()
	for i, id := range ids {
		m := e.metadata(id)
		if m.Version != 1+rounds || m.SHA256 != sha(final[i]) {
			t.Fatalf("%s: metadata v%d sha %s…, want v%d of final payload", id, m.Version, m.SHA256[:8], 1+rounds)
		}
		for _, n := range m.Replicas {
			vr, err := e.nodeClient(n).VerifyReplica(ctx, id, m.Version)
			if err != nil || vr.Actual.SHA256 != m.SHA256 {
				t.Fatalf("%s: replica on %s is not the current version (%v)", id, n, err)
			}
		}
		r := e.do(http.MethodGet, "/objects/"+id, nil)
		if !bytes.Equal(r.Body, final[i]) {
			t.Fatalf("%s: read returned stale bytes", id)
		}
	}
}

// With no eligible target, repair reports failure honestly; the object
// stays DEGRADED; nothing claims recovery. When the node returns, the
// object is healthy again without any fabricated repair.
func TestSelfHealInsufficientCapacityStaysDegraded(t *testing.T) {
	e := startSelfHealing(t, 3, healOpts{extra: []string{"--repair-max-attempts=2", "--repair-backoff=50ms"}})
	obj := e.put("stuck.bin", randomBytes(10_000, 80))
	e.kill("node-2")
	e.waitFor("repair parked as failed", 15*time.Second, func() bool { return len(e.queue().Failed) == 1 })

	f := e.queue().Failed[0]
	mustContain(t, f.Task.LastError, "no eligible healthy target")
	if h := e.health("stuck.bin"); h.State != durability.Degraded || h.HealthyReplicas != 2 {
		t.Fatalf("health %+v", h.ObjectHealth)
	}
	m := e.metrics().Repair
	if m.Completed != 0 || m.Failed != 1 || m.ReplicasCreated != 0 || m.BytesTransferred != 0 {
		t.Fatalf("metrics %+v", m)
	}
	if len(e.eventsFor("REPAIR_FAILED", "stuck.bin")) != 1 {
		t.Fatal("no REPAIR_FAILED event")
	}
	if meta := e.metadata("stuck.bin"); !slices.Equal(meta.Replicas, obj.Replicas) || meta.LastRepairedAt != nil {
		t.Fatalf("metadata changed without repair: %+v", meta)
	}

	if err := e.c.StartNode(context.Background(), "node-2"); err != nil {
		t.Fatal(err)
	}
	e.waitAllHealthy(15 * time.Second)
	if m := e.metrics().Repair; m.Completed != 0 {
		t.Fatalf("recovery by node return was counted as a repair: %+v", m)
	}
}

// Chaos endpoints perform real actions on real processes and files.
func TestChaosEndpointsPerformRealActions(t *testing.T) {
	e := startSelfHealing(t, 3, healOpts{})
	e.post("/repair/pause", http.StatusOK, nil) // observe raw effects only
	data := randomBytes(50_000, 90)
	obj := e.put("chaos.bin", data)
	victim := obj.Replicas[0]
	pid := e.c.Node(victim).PID()
	path := e.replicaPath(victim, "chaos.bin", obj.Version)
	sidecar := strings.TrimSuffix(path, ".data") + ".json"
	sideBefore, _ := os.ReadFile(sidecar)

	var cr storage.CorruptResult
	e.post("/chaos/corrupt-replica/chaos.bin/"+victim, http.StatusOK, &cr)
	onDisk, _ := os.ReadFile(path)
	if cr.Before.SHA256 != obj.SHA256 || cr.After.SHA256 != sha(onDisk) || sha(onDisk) == obj.SHA256 || len(onDisk) != len(data) {
		t.Fatalf("corrupt result %+v", cr)
	}
	if sideAfter, _ := os.ReadFile(sidecar); !bytes.Equal(sideBefore, sideAfter) {
		t.Fatal("chaos corruption touched the node's checksum record; it must be silent")
	}
	if e.c.Node(victim).PID() != pid || e.nodeState(victim) != storage.StateHealthy {
		t.Fatal("corrupted node must stay alive and healthy")
	}
	if m := e.metadata("chaos.bin"); len(m.IntegrityFailures) != 0 {
		t.Fatal("chaos corruption must not be recorded by the coordinator")
	}
	e.mustStatus(e.do(http.MethodPost, "/chaos/corrupt-replica/chaos.bin/node-9", nil), http.StatusNotFound, "unknown node")
	e.mustStatus(e.do(http.MethodPost, "/chaos/corrupt-replica/missing/"+victim, nil), http.StatusNotFound, "unknown object")
	e.mustStatus(e.do(http.MethodPost, "/chaos/kill-node/node-9", nil), http.StatusNotFound, "kill unknown node")

	killed := obj.Replicas[1]
	e.kill(killed)
	if e.nodeState(killed) != storage.StateUnavailable {
		t.Fatal("killed node not UNAVAILABLE")
	}
}

func TestChaosDisabledByDefault(t *testing.T) {
	e := startCluster(t, 3, 3) // Phase 1 configuration
	e.mustStatus(e.do(http.MethodPost, "/chaos/kill-node/node-1", nil), http.StatusForbidden, "api chaos")
	resp, err := e.hc.Post(e.c.Node("node-1").URL+"/admin/terminate", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden || !e.c.Node("node-1").Running() {
		t.Fatalf("node admin terminate without --enable-chaos: HTTP %d", resp.StatusCode)
	}
}
