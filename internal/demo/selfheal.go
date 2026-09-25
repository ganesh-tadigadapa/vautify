package demo

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"slices"
	"sort"
	"strings"
	"time"

	"vault/internal/cluster"
	"vault/internal/coordinator"
	"vault/internal/durability"
	"vault/internal/metadata"
	"vault/internal/node"
	"vault/internal/repair"
	"vault/internal/selfheal"
	"vault/internal/storage"
)

// The self-healing demos drive Vault exclusively through its HTTP API —
// the same endpoints a UI would use — against real node processes.

type client struct {
	hc   *http.Client
	base string
}

func (c client) getJSON(ctx context.Context, path string, v any) error {
	return doJSON(ctx, c.hc, http.MethodGet, c.base+path, nil, http.StatusOK, v)
}

func (c client) postJSON(ctx context.Context, path string, v any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+path, nil)
	if err != nil {
		return err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("POST %s: HTTP %d: %s", path, resp.StatusCode, strings.TrimSpace(string(b)))
	}
	if v == nil {
		return nil
	}
	return json.Unmarshal(b, v)
}

func (c client) put(ctx context.Context, id string, data []byte) (metadata.Object, error) {
	var pr struct {
		Object metadata.Object `json:"object"`
	}
	err := doJSON(ctx, c.hc, http.MethodPut, c.base+"/objects/"+id, bytes.NewReader(data), http.StatusCreated, &pr)
	return pr.Object, err
}

func (c client) waitFor(ctx context.Context, what string, timeout time.Duration, cond func() (bool, error)) error {
	deadline := time.Now().Add(timeout)
	for {
		ok, err := cond()
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for %s", what)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func (c client) waitHealed(ctx context.Context, timeout time.Duration) (selfheal.ClusterStatus, error) {
	var st selfheal.ClusterStatus
	err := c.waitFor(ctx, "all objects HEALTHY and repair idle", timeout, func() (bool, error) {
		if err := c.getJSON(ctx, "/cluster/status", &st); err != nil {
			return false, err
		}
		return st.Objects.Healthy == st.Objects.Total && st.Repair.QueueDepth == 0 && st.Repair.Active == 0, nil
	})
	return st, err
}

func shaHex(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

func humanBytes(n int64) string {
	switch {
	case n >= 1<<20:
		return fmt.Sprintf("%.1f MiB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.0f KiB", float64(n)/(1<<10))
	}
	return fmt.Sprintf("%d B", n)
}

func (p printer) state(s durability.State) string {
	switch s {
	case durability.Critical, durability.Lost:
		return p.c("31", string(s))
	case durability.Degraded:
		return p.c("33", string(s))
	}
	return p.c("32", string(s))
}

// RunSelfHeal: 5 nodes, RF=3, two nodes killed, blast radius, risk-ordered
// queue, verified repairs, restored replication, real recovery metrics.
func RunSelfHeal(ctx context.Context, out io.Writer, opts Options) error {
	p := printer{w: out, color: opts.Color}
	size := opts.Size
	if size == 0 {
		size = 512 << 10
	}
	const objects = 24
	failed := []string{"node-2", "node-4"}

	p.f("%s\n", p.bold("VAULT SELF-HEAL DEMO — two node failures, risk-prioritised repair"))
	p.f("%s\n", p.dim("real processes · real HTTP · real files · every number below is measured"))

	cl, err := cluster.Start(ctx, cluster.Spec{
		Binary: opts.Binary, Dir: opts.Dir, Nodes: 5, ReplicationFactor: 3,
		AutoRepair: true, EnableChaos: true, RepairWorkers: 2, SweepInterval: 300 * time.Millisecond,
	})
	if err != nil {
		return err
	}
	defer cl.Stop()
	c := client{hc: &http.Client{Timeout: 60 * time.Second}, base: cl.API.URL}

	var st selfheal.ClusterStatus
	if err := c.getJSON(ctx, "/cluster/status", &st); err != nil {
		return err
	}
	p.section("Cluster")
	p.line("%d / %d nodes healthy %s   RF=3   repair workers: %d", st.NodesHealthy, st.NodesTotal, p.ok(), st.Repair.Workers)

	// Upload -------------------------------------------------------------
	rng := rand.New(rand.NewSource(7))
	payloads := map[string][]byte{}
	var ids []string
	for i := 0; i < objects; i++ {
		id := fmt.Sprintf("dataset-%02d", i)
		b := make([]byte, size)
		rng.Read(b)
		if _, err := c.put(ctx, id, b); err != nil {
			return fmt.Errorf("upload %s: %w", id, err)
		}
		payloads[id], ids = b, append(ids, id)
	}
	if err := c.getJSON(ctx, "/cluster/status", &st); err != nil {
		return err
	}
	p.section(fmt.Sprintf("Upload: %d objects × %s, RF=3 → %d replicas on disk", objects, humanBytes(int64(size)), objects*3))
	var per []string
	for _, n := range st.Nodes {
		per = append(per, fmt.Sprintf("%s:%d", n.ID, n.Replicas))
	}
	p.line("replicas per node  %s", strings.Join(per, "  "))

	// Fail ---------------------------------------------------------------
	if err := c.postJSON(ctx, "/repair/pause", nil); err != nil {
		return err
	}
	p.section("Inject: NODE-2 + NODE-4 FAILURE")
	p.line("%s", p.dim("(repair paused so the queue can be inspected before it drains)"))
	for _, n := range failed {
		pid := cl.Node(n).PID()
		var kr coordinator.KillResult
		if err := c.postJSON(ctx, "/chaos/kill-node/"+n, &kr); err != nil {
			return err
		}
		if err := c.waitFor(ctx, n+" exit", 5*time.Second, func() (bool, error) { return !cl.Node(n).Running(), nil }); err != nil {
			return err
		}
		p.line("%s → process %d exited %s  unreachable after %d ms", n, pid, p.bad(), kr.ConfirmationMS)
	}
	var q repair.Snapshot
	if err := c.waitFor(ctx, "impact classified and queued", 10*time.Second, func() (bool, error) {
		if err := c.getJSON(ctx, "/cluster/status", &st); err != nil {
			return false, err
		}
		if err := c.getJSON(ctx, "/repair/queue", &q); err != nil {
			return false, err
		}
		return st.NodesHealthy == 3 && q.Depth == st.Objects.Degraded+st.Objects.Critical, nil
	}); err != nil {
		return err
	}

	// Impact -------------------------------------------------------------
	p.section("Impact (blast radius computed from metadata)")
	affected := map[string]bool{}
	for _, n := range failed {
		var br durability.BlastRadius
		if err := c.getJSON(ctx, "/nodes/"+n+"/blast-radius", &br); err != nil {
			return err
		}
		for _, o := range br.Objects {
			affected[o.ObjectID] = true
		}
		p.line("%s held replicas of %d objects (%s)", n, br.AffectedObjects, humanBytes(br.AffectedBytes))
	}
	o := st.Objects
	p.line("%s objects affected · %s %s · %s %s · %d still protected · %d lost",
		p.bold(fmt.Sprint(len(affected))), p.bold(fmt.Sprint(o.Critical)), p.state(durability.Critical),
		p.bold(fmt.Sprint(o.Degraded)), p.state(durability.Degraded), o.Healthy, o.Lost)

	// Queue --------------------------------------------------------------
	p.section("Repair queue (risk-prioritised, not FIFO)")
	for _, i := range elide(len(q.Queued), 5) {
		if i < 0 {
			p.line("%s", p.dim(fmt.Sprintf("    … %d more", len(q.Queued)-6)))
			continue
		}
		t := q.Queued[i]
		p.line("%2d. [%s] %s  %s", i+1, p.state(t.Priority), t.ObjectID, p.dim(t.Reason))
	}
	lastCritical, firstDegraded := -1, len(q.Queued)
	for i, t := range q.Queued {
		if t.Priority == durability.Critical {
			lastCritical = i
		} else if i < firstDegraded {
			firstDegraded = i
		}
	}
	if lastCritical > firstDegraded {
		return fmt.Errorf("queue is not risk-ordered: %+v", q.Queued)
	}
	p.line("%s all %d CRITICAL objects are ahead of all %d DEGRADED objects", p.ok(), o.Critical, o.Degraded)

	// Repair -------------------------------------------------------------
	if err := c.postJSON(ctx, "/repair/resume", nil); err != nil {
		return err
	}
	if st, err = c.waitHealed(ctx, 60*time.Second); err != nil {
		return err
	}
	var hist struct{ History []repair.HistoryEntry }
	if err := c.getJSON(ctx, "/repair/history", &hist); err != nil {
		return err
	}
	h := hist.History
	sort.SliceStable(h, func(i, j int) bool { return h[i].StartedAt.Before(h[j].StartedAt) })
	p.section("Repair (in the order it ran)")
	for _, i := range elide(len(h), 6) {
		if i < 0 {
			p.line("%s", p.dim(fmt.Sprintf("… %d more", len(h)-7)))
			continue
		}
		e := h[i]
		mark := p.ok()
		if e.Outcome != "restored" {
			mark = p.bad()
		}
		p.line("[%s] %s  %s → %s  checksum verified %s  %s  %d ms",
			p.state(e.Priority), e.ObjectID, e.Result.Source, strings.Join(e.Result.Targets, ", "),
			mark, humanBytes(e.Result.BytesTransferred), e.DurationMS)
	}
	lastCriticalStart, firstDegradedStart := time.Time{}, time.Time{}
	for _, e := range h {
		if e.Priority == durability.Critical && e.StartedAt.After(lastCriticalStart) {
			lastCriticalStart = e.StartedAt
		}
		if e.Priority == durability.Degraded && (firstDegradedStart.IsZero() || e.StartedAt.Before(firstDegradedStart)) {
			firstDegradedStart = e.StartedAt
		}
	}
	if !firstDegradedStart.IsZero() && lastCriticalStart.After(firstDegradedStart) {
		return fmt.Errorf("a DEGRADED repair started before a CRITICAL one")
	}
	p.line("%s every CRITICAL repair started before any DEGRADED repair", p.ok())

	// Verify -------------------------------------------------------------
	p.section("Verification")
	readOK, replicasOK := 0, 0
	for _, id := range ids {
		body, _, err := get(ctx, c.hc, c.base+"/objects/"+id)
		if err != nil {
			return err
		}
		if shaHex(body) != shaHex(payloads[id]) {
			return fmt.Errorf("%s read back wrong bytes", id)
		}
		readOK++
		var m metadata.Object
		if err := c.getJSON(ctx, "/objects/"+id+"/metadata", &m); err != nil {
			return err
		}
		for _, n := range m.Replicas {
			if slices.Contains(failed, n) {
				return fmt.Errorf("%s still relies on failed %s", id, n)
			}
			b, err := os.ReadFile(node.ReplicaDataPath(cl.Node(n).DataDir, id, m.Version))
			if err != nil || shaHex(b) != m.SHA256 {
				return fmt.Errorf("%s: replica on %s not verified on disk", id, n)
			}
			replicasOK++
		}
	}
	p.line("%d / %d objects read back byte-identical, sha256 verified %s", readOK, objects, p.ok())
	p.line("%d / %d replicas present and verified on live nodes' disks %s", replicasOK, objects*3, p.ok())

	// Result -------------------------------------------------------------
	var mt struct {
		Repair  repair.Metrics    `json:"repair"`
		Objects durability.Counts `json:"objects"`
	}
	if err := c.getJSON(ctx, "/repair/metrics", &mt); err != nil {
		return err
	}
	r := mt.Repair
	p.section("Result")
	p.line("replication restored: %d / %d objects at 3/3 %s", mt.Objects.Healthy, mt.Objects.Total, p.ok())
	p.line("repairs: %d restored · %d failed · %d retries · %d replicas created · %s transferred",
		r.Completed, r.Failed, r.Retries, r.ReplicasCreated, humanBytes(r.BytesTransferred))
	p.line("repair time per object: avg %d ms · max %d ms", r.RepairDuration.AvgMS, r.RepairDuration.MaxMS)
	p.line("recovery time (degradation detected → restored): avg %d ms · max %d ms %s",
		r.RecoveryTime.AvgMS, r.RecoveryTime.MaxMS, p.dim("(includes the inspection pause above)"))

	p.section("Not done in this stage")
	p.line("%s node-2 and node-4 stay down; their old replica files are not reconciled if they return", p.dim("•"))
	p.line("%s metadata lives in one coordinator — no consensus, no partition handling", p.dim("•"))
	p.f("\n")
	return nil
}

// RunCorruptionRepair: silent corruption on a live node, found by the
// scrubber, repaired from a verified source, replacement verified.
func RunCorruptionRepair(ctx context.Context, out io.Writer, opts Options) error {
	p := printer{w: out, color: opts.Color}
	size := opts.Size
	if size == 0 {
		size = 1 << 20
	}
	const id = "ledger.db"
	p.f("%s\n", p.bold("VAULT SILENT-CORRUPTION REPAIR DEMO"))
	p.f("%s\n", p.dim("real processes · real HTTP · real files · nothing simulated"))

	cl, err := cluster.Start(ctx, cluster.Spec{
		Binary: opts.Binary, Dir: opts.Dir, Nodes: 4, ReplicationFactor: 3,
		AutoRepair: true, EnableChaos: true, SweepInterval: 300 * time.Millisecond,
	})
	if err != nil {
		return err
	}
	defer cl.Stop()
	c := client{hc: &http.Client{Timeout: 60 * time.Second}, base: cl.API.URL}

	data := make([]byte, size)
	rand.New(rand.NewSource(11)).Read(data)
	obj, err := c.put(ctx, id, data)
	if err != nil {
		return err
	}
	p.section(fmt.Sprintf("Upload: %s (%s) → %s", id, humanBytes(int64(size)), strings.Join(obj.Replicas, ", ")))
	victim := obj.Replicas[0]
	pid := cl.Node(victim).PID()

	// Inject -------------------------------------------------------------
	var cr storage.CorruptResult
	if err := c.postJSON(ctx, "/chaos/corrupt-replica/"+id+"/"+victim, &cr); err != nil {
		return err
	}
	p.section("Inject: silent corruption")
	p.line("%s → %d bytes flipped at offset %d on disk; sha256 %s… → %s…",
		victim, cr.Length, cr.Offset, cr.Before.SHA256[:12], cr.After.SHA256[:12])
	var nodes struct{ Nodes []struct{ ID, State string } }
	if err := c.getJSON(ctx, "/nodes", &nodes); err != nil {
		return err
	}
	for _, n := range nodes.Nodes {
		if n.ID == victim {
			p.line("NODE ALIVE %s  %s process %d, health %s", p.ok(), victim, cl.Node(victim).PID(), n.State)
		}
	}
	var hv selfheal.ObjectHealthView
	if err := c.getJSON(ctx, "/objects/"+id+"/health", &hv); err != nil {
		return err
	}
	p.line("Vault's view before scrub: %s %d/3  %s", p.state(hv.State), hv.HealthyReplicas, p.dim("(silent: nobody has read the bad bytes yet)"))

	// Scrub --------------------------------------------------------------
	if err := c.postJSON(ctx, "/repair/pause", nil); err != nil {
		return err
	}
	var sr coordinator.ScrubReport
	if err := c.postJSON(ctx, "/scrub/run", &sr); err != nil {
		return err
	}
	p.section("Scrub (each node re-hashes its own files; coordinator compares with metadata)")
	p.line("%d replicas checked · %d verified · %d corrupted · %d ms", sr.ReplicasChecked, sr.Verified, sr.Corrupted, sr.DurationMS)
	for _, f := range sr.Findings {
		p.line("%s → CHECKSUM MISMATCH %s  expected %s… found %s…", f.NodeID, p.bad(), f.ExpectedSHA256[:12], f.ObservedSHA256[:12])
	}
	if sr.Corrupted != 1 {
		return fmt.Errorf("scrub found %d corrupted replicas, want 1", sr.Corrupted)
	}
	if err := c.getJSON(ctx, "/objects/"+id+"/health", &hv); err != nil {
		return err
	}
	vr, _ := hv.Replica(victim)
	p.line("replica on %s: %s   node: %s   object: %s %d/3",
		victim, p.c("31", string(vr.State)), vr.NodeState, p.state(hv.State), hv.HealthyReplicas)
	if hv.Repair != nil {
		p.line("repair queued: priority %s — %s", p.state(hv.Repair.Priority), p.dim(hv.Repair.Reason))
	}
	if vr.State != durability.ReplicaCorrupted || vr.NodeState != storage.StateHealthy {
		return fmt.Errorf("expected CORRUPTED replica on a HEALTHY node, got %+v", vr)
	}

	// Repair -------------------------------------------------------------
	if err := c.postJSON(ctx, "/repair/resume", nil); err != nil {
		return err
	}
	if _, err := c.waitHealed(ctx, 30*time.Second); err != nil {
		return err
	}
	var hist struct{ History []repair.HistoryEntry }
	if err := c.getJSON(ctx, "/repair/history", &hist); err != nil {
		return err
	}
	if len(hist.History) != 1 {
		return fmt.Errorf("expected one repair, got %d", len(hist.History))
	}
	e := hist.History[0]
	if e.Result.Source == victim {
		return fmt.Errorf("repair used the corrupted replica as its source")
	}
	target := e.Result.Targets[0]
	p.section("Repair")
	p.line("source  %s (bytes re-verified against metadata sha256 before use) %s", e.Result.Source, p.ok())
	p.line("target  %s (healthy node without a replica)", target)
	tb, err := os.ReadFile(node.ReplicaDataPath(cl.Node(target).DataDir, id, obj.Version))
	if err != nil || shaHex(tb) != obj.SHA256 {
		return fmt.Errorf("replacement on %s not verified on disk", target)
	}
	p.line("CHECKSUM VERIFIED %s  %s verified on write and re-hashed from disk during repair", p.ok(), target)
	p.line("independent check: sha256 of %s's file on disk %s… == metadata %s", target, shaHex(tb)[:12], p.ok())
	if _, err := os.Stat(node.ReplicaDataPath(cl.Node(victim).DataDir, id, obj.Version)); os.IsNotExist(err) {
		p.line("corrupted bytes removed from %s %s", victim, p.ok())
	}

	// Restored -----------------------------------------------------------
	if err := c.getJSON(ctx, "/objects/"+id+"/health", &hv); err != nil {
		return err
	}
	body, _, err := get(ctx, c.hc, c.base+"/objects/"+id)
	if err != nil {
		return err
	}
	if shaHex(body) != obj.SHA256 || hv.State != durability.Healthy {
		return fmt.Errorf("object not restored")
	}
	p.section("RESTORED")
	p.line("%s %d/3  replicas %s  %s", p.state(hv.State), hv.HealthyReplicas, strings.Join(replicaIDs(hv.ObjectHealth), ", "), p.ok())
	p.line("client read sha256 %s… == original %s", shaHex(body)[:12], p.ok())
	p.line("recovery time (scrub detection → restored): %d ms %s", e.RecoveryMS, p.dim("(includes the inspection pause)"))
	p.line("%s (the corrupted node) was never restarted: same pid %d", victim, pid)
	if cl.Node(victim).PID() != pid {
		return fmt.Errorf("victim process changed")
	}
	p.f("\n")
	return nil
}

func replicaIDs(h durability.ObjectHealth) []string {
	var out []string
	for _, r := range h.Replicas {
		out = append(out, r.NodeID)
	}
	return out
}

// elide returns the indexes to print for a list of n items: the first head,
// a -1 marker for the elided middle, and the last item. Short lists are
// returned whole.
func elide(n, head int) []int {
	var out []int
	if n <= head+2 {
		for i := 0; i < n; i++ {
			out = append(out, i)
		}
		return out
	}
	for i := 0; i < head; i++ {
		out = append(out, i)
	}
	return append(out, -1, n-1)
}
