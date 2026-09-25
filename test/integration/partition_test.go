package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"io/fs"
	"net/http"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"vault/internal/api"
	"vault/internal/coordinator"
	"vault/internal/storage"
)

func (e *env) diagnosis() map[string]api.NodeDiagnosis {
	var body struct{ Nodes []api.NodeDiagnosis }
	e.getJSON("/cluster/diagnosis", &body)
	out := map[string]api.NodeDiagnosis{}
	for _, n := range body.Nodes {
		out[n.NodeID] = n
	}
	return out
}

func (e *env) storageMetrics() api.StorageMetricsResponse {
	var m api.StorageMetricsResponse
	e.getJSON("/storage/metrics", &m)
	return m
}

// dataFiles sums the replica files actually on the nodes' disks.
func (e *env) dataFiles(nodes ...string) (int64, int) {
	var total int64
	var n int
	for _, id := range nodes {
		filepath.WalkDir(filepath.Join(e.c.Node(id).DataDir, "objects"), func(p string, d fs.DirEntry, err error) error {
			if err == nil && !d.IsDir() && strings.HasSuffix(p, ".data") {
				fi, _ := d.Info()
				total += fi.Size()
				n++
			}
			return nil
		})
	}
	return total, n
}

func errorCode(t *testing.T, r response) string {
	t.Helper()
	var body struct{ Error struct{ Code string } }
	json.Unmarshal(r.Body, &body)
	return body.Error.Code
}

// The Phase 4 gate over real processes: partition a node through the chaos
// endpoint (the node process keeps running), write and read with W=2/R=2,
// delete during the partition, lose quorum with a second partition, heal,
// and watch reconciliation + repair return the cluster to a consistent
// state with the newest versions and no resurrected data. Storage metrics
// are compared with the files on disk throughout.
func TestPartitionQuorumHealingOverHTTP(t *testing.T) {
	e := startSelfHealing(t, 3, healOpts{extra: []string{"--write-quorum=2", "--read-quorum=2"}})
	v1, v2 := randomBytes(64<<10, 1), randomBytes(64<<10, 2)
	e.put("over", v1)
	e.put("gone", randomBytes(32<<10, 3))
	e.put("same", randomBytes(16<<10, 4))
	m := e.storageMetrics()
	if disk, files := e.dataFiles("node-1", "node-2", "node-3"); !m.Complete || *m.PhysicalBytes != disk || *m.PhysicalReplicas != files ||
		m.LogicalBytes != (64+32+16)<<10 || *m.StorageOverhead != 3.0 {
		t.Fatalf("baseline metrics %+v (disk %d/%d)", m.StorageMetrics, disk, files)
	}

	// Partition node-3: unreachable for the coordinator, alive for everyone else.
	e.post("/chaos/partition/node-3", http.StatusOK, nil)
	e.waitNodeState("node-3", storage.StateUnavailable)
	if !e.c.Node("node-3").Running() {
		t.Fatal("a partition must not stop the node process")
	}
	if _, err := e.nodeClient("node-3").Health(context.Background()); err != nil {
		t.Fatalf("node-3 answers a direct probe during the partition: %v", err)
	}
	if d := e.diagnosis()["node-3"]; d.Diagnosis != api.DiagnosisNetworkPartition || d.Health != storage.StateUnavailable {
		t.Fatalf("diagnosis %+v", d)
	}

	// W=2: acknowledged with two verified replicas, reported degraded.
	r := e.do(http.MethodPut, "/objects/over", v2)
	e.mustStatus(r, http.StatusOK, "quorum write during partition")
	var pr api.PutResponse
	json.Unmarshal(r.Body, &pr)
	if pr.Replication.Achieved != 2 || pr.Replication.Status != "degraded" || pr.Replication.WriteQuorum != 2 ||
		slices.Contains(pr.Object.Replicas, "node-3") {
		t.Fatalf("put response %s", r.Body)
	}
	e.mustStatus(e.do(http.MethodDelete, "/objects/gone", nil), http.StatusOK, "delete during partition")
	r = e.do(http.MethodGet, "/objects/over", nil)
	e.mustStatus(r, http.StatusOK, "R=2 read during partition")
	if !bytes.Equal(r.Body, v2) || r.Header.Get(storage.HeaderReadQuorum) != "2" ||
		strings.Contains(r.Header.Get(storage.HeaderConfirmedBy), "node-3") || len(strings.Split(r.Header.Get(storage.HeaderConfirmedBy), ",")) != 2 {
		t.Fatalf("read: v%s confirmed by %q", r.Header.Get(storage.HeaderVersion), r.Header.Get(storage.HeaderConfirmedBy))
	}
	e.mustStatus(e.do(http.MethodGet, "/objects/gone", nil), http.StatusNotFound, "deleted object during partition")

	// A second partition: neither quorum can be met; nothing is reported
	// as success, nothing changes.
	e.post("/chaos/partition/node-2", http.StatusOK, nil)
	e.waitNodeState("node-2", storage.StateUnavailable)
	r = e.do(http.MethodPut, "/objects/over", randomBytes(1024, 9))
	e.mustStatus(r, http.StatusServiceUnavailable, "write without quorum")
	if c := errorCode(t, r); c != "quorum_not_met" {
		t.Fatalf("write error %s", r.Body)
	}
	r = e.do(http.MethodGet, "/objects/over", nil)
	e.mustStatus(r, http.StatusServiceUnavailable, "read without quorum")
	if c := errorCode(t, r); c != "quorum_not_met" || bytes.Contains(r.Body, v2[:64]) {
		t.Fatalf("read error %s", r.Body)
	}
	if md := e.metadata("over"); md.Version != 2 || md.SHA256 != sha(v2) {
		t.Fatalf("metadata changed by failed quorum write: %+v", md)
	}
	m = e.storageMetrics()
	if m.Complete || m.PhysicalBytes != nil || m.StorageOverhead != nil || !slices.Equal(m.UnreachableNodes, []string{"node-2", "node-3"}) {
		t.Fatalf("incomplete metrics must not be invented: %+v", m.StorageMetrics)
	}
	if reach, _ := e.dataFiles("node-1"); m.PhysicalBytesReachable != reach {
		t.Fatalf("reachable bytes %d, disk %d", m.PhysicalBytesReachable, reach)
	}

	// Heal: reconcile on recovery, then repair restores RF.
	e.post("/chaos/heal", http.StatusOK, nil)
	e.waitNodeState("node-2", storage.StateHealthy)
	e.waitNodeState("node-3", storage.StateHealthy)
	e.waitAllHealthy(30 * time.Second)
	e.waitFor("node-3 converged to the newest version, deleted data removed", 15*time.Second, func() bool {
		return slices.Equal(e.versionsHeld("node-3", "over"), []uint64{2}) && len(e.versionsHeld("node-3", "gone")) == 0
	})
	e.waitFor("reconcile on recovery of node-2 and node-3", 15*time.Second, func() bool {
		reconciled := map[string]bool{}
		for _, ev := range e.events("RECONCILE_COMPLETED") {
			if ev.Fields["trigger"] == "node_recovered" {
				reconciled[ev.Fields["node_id"].(string)] = true
			}
		}
		return reconciled["node-2"] && reconciled["node-3"]
	})
	for _, id := range []string{"over", "gone", "same"} {
		if c := e.consistency(id); !c.Consistent {
			t.Fatalf("%s not consistent after healing: %+v", id, c)
		}
	}
	e.mustStatus(e.do(http.MethodGet, "/objects/gone", nil), http.StatusNotFound, "deleted object after healing")
	r = e.do(http.MethodGet, "/objects/over?read_quorum=3", nil)
	e.mustStatus(r, http.StatusOK, "R=3 read after healing")
	if !bytes.Equal(r.Body, v2) {
		t.Fatal("healed read is not the newest version")
	}

	m = e.storageMetrics()
	disk, files := e.dataFiles("node-1", "node-2", "node-3")
	if !m.Complete || *m.PhysicalBytes != disk || *m.PhysicalReplicas != files || m.LogicalBytes != (64+16)<<10 ||
		*m.StorageOverhead != 3.0 || m.Breakdown.Stale.Replicas != 0 || m.Breakdown.DeletedObject.Replicas != 0 {
		t.Fatalf("healed metrics %+v (disk %d/%d)", m.StorageMetrics, disk, files)
	}
	if m.Repair == nil || m.Repair.BytesWritten != int64(len(v2)) || m.Repair.ReplicasCreated != 1 {
		t.Fatalf("repair bytes %+v, want one %d-byte replica", m.Repair, len(v2))
	}
	if m.Removals.ByClass[coordinator.RemovalStale] < 1 || m.Removals.ByClass[coordinator.RemovalDeletedObject] < 1 {
		t.Fatalf("guarded removals %+v", m.Removals)
	}
	if d := e.diagnosis()["node-3"]; d.Diagnosis != api.DiagnosisHealthy {
		t.Fatalf("after heal %+v", d)
	}
}

// NODE FAILURE != NETWORK PARTITION != CORRUPTED DATA, over real processes.
func TestNodeFailureVsPartitionVsCorruption(t *testing.T) {
	e := startSelfHealing(t, 4, healOpts{})
	e.post("/repair/pause", http.StatusOK, nil)
	id := nameOn(t, "diag", nodeNames(4), []string{"node-1", "node-2", "node-3"})
	e.put(id, randomBytes(32<<10, 5))

	e.post("/chaos/corrupt-replica/"+id+"/node-1", http.StatusOK, nil)
	e.post("/scrub/run", http.StatusOK, nil)
	e.post("/chaos/partition/node-2", http.StatusOK, nil)
	e.waitNodeState("node-2", storage.StateUnavailable)
	e.kill("node-3")
	e.waitNodeState("node-3", storage.StateUnavailable)

	d := e.diagnosis()
	if n := d["node-1"]; n.Diagnosis != api.DiagnosisCorruptedData || n.Health != storage.StateHealthy || n.CorruptedReplicas != 1 {
		t.Fatalf("node-1 (corrupted data) %+v", n)
	}
	if n := d["node-2"]; n.Diagnosis != api.DiagnosisNetworkPartition || !e.c.Node("node-2").Running() {
		t.Fatalf("node-2 (partition) %+v", n)
	}
	if _, err := e.nodeClient("node-2").Health(context.Background()); err != nil {
		t.Fatalf("partitioned node-2 must still answer directly: %v", err)
	}
	if n := d["node-3"]; n.Diagnosis != api.DiagnosisNodeUnreachable || e.c.Node("node-3").Running() {
		t.Fatalf("node-3 (failure) %+v", n)
	}
	if n := d["node-4"]; n.Diagnosis != api.DiagnosisHealthy {
		t.Fatalf("node-4 %+v", n)
	}
}

// Safe replacement over real processes: the corrupted replica on node-2 is
// removed only after its replacement on node-4 was written, version- and
// checksum-verified, and committed — in that event order.
func TestGuardedReplacementOrderOverHTTP(t *testing.T) {
	e := startSelfHealing(t, 4, healOpts{})
	id := nameOn(t, "safe", nodeNames(4), []string{"node-1", "node-2", "node-3"})
	data := randomBytes(48<<10, 6)
	e.put(id, data)
	e.post("/chaos/corrupt-replica/"+id+"/node-2", http.StatusOK, nil)
	e.post("/scrub/run", http.StatusOK, nil)
	e.waitAllHealthy(20 * time.Second)

	find := func(name string, match func(map[string]any) bool) uint64 {
		for _, ev := range e.eventsFor(name, id) {
			if match(ev.Fields) {
				return ev.Seq
			}
		}
		t.Fatalf("no matching %s event", name)
		return 0
	}
	written := find("REPAIR_REPLICA_WRITTEN", func(f map[string]any) bool { return f["target"] == "node-4" })
	verified := find("REPAIR_CHECKSUM_VERIFIED", func(f map[string]any) bool {
		return f["target"] == "node-4" && f["version_verified"] == true
	})
	committed := find("REPAIR_METADATA_COMMITTED", func(map[string]any) bool { return true })
	removed := find("REPLICA_REMOVED", func(f map[string]any) bool {
		vb, _ := f["verified_by"].([]any)
		return f["node_id"] == "node-2" && f["class"] == coordinator.RemovalInvalid && slices.Contains(vb, any("node-4"))
	})
	if !(written < verified && verified < committed && committed < removed) {
		t.Fatalf("order written=%d verified=%d committed=%d removed=%d", written, verified, committed, removed)
	}
	if held := e.versionsHeld("node-2", id); len(held) != 0 {
		t.Fatalf("corrupted replica still on node-2: %v", held)
	}
	if md := e.metadata(id); !slices.Contains(md.Replicas, "node-4") || slices.Contains(md.Replicas, "node-2") {
		t.Fatalf("replicas %v", md.Replicas)
	}
	if r := e.do(http.MethodGet, "/objects/"+id+"?read_quorum=3", nil); r.Status != http.StatusOK || !bytes.Equal(r.Body, data) {
		t.Fatalf("R=3 read after replacement: %d", r.Status)
	}
}
