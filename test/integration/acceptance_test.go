package integration

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"os"
	"slices"
	"strconv"
	"testing"
	"time"

	"vault/internal/storage"
)

// HARD GATE 1: replication factor N means the bytes physically exist in N
// distinct node processes' storage directories.
func TestGateRealReplicationAcrossProcesses(t *testing.T) {
	e := startCluster(t, 3, 3)
	ctx := context.Background()

	pids := map[int]bool{}
	for _, n := range e.c.Nodes {
		if !n.Running() || pids[n.PID()] {
			t.Fatalf("%s is not a distinct running process (pid %d)", n.Name, n.PID())
		}
		pids[n.PID()] = true
	}

	data := randomBytes(3<<20, 1)
	obj := e.put("replicated.bin", data)
	if obj.SHA256 != sha(data) || obj.Size != int64(len(data)) || obj.ReplicationFactor != 3 {
		t.Fatalf("metadata %+v", obj)
	}
	if got := slices.Sorted(slices.Values(obj.Replicas)); !slices.Equal(got, []string{"node-1", "node-2", "node-3"}) {
		t.Fatalf("replicas = %v", obj.Replicas)
	}
	for _, id := range obj.Replicas {
		onDisk, err := os.ReadFile(e.replicaPath(id, "replicated.bin", obj.Version))
		if err != nil {
			t.Fatalf("%s: no replica file on disk: %v", id, err)
		}
		if !bytes.Equal(onDisk, data) {
			t.Fatalf("%s: on-disk bytes differ", id)
		}
		vr, err := e.nodeClient(id).VerifyReplica(ctx, "replicated.bin", obj.Version)
		if err != nil || !vr.Valid {
			t.Fatalf("%s: node-side verify = %+v, %v", id, vr, err)
		}
	}

	// Configurable RF: RF=2 puts bytes on exactly two node directories.
	r := e.do(http.MethodPut, "/objects/two.bin?replication_factor=2", []byte("two copies"))
	e.mustStatus(r, http.StatusCreated, "PUT rf=2")
	two := e.metadata("two.bin")
	holders := 0
	for _, n := range e.c.Nodes {
		if _, err := os.Stat(e.replicaPath(n.Name, "two.bin", two.Version)); err == nil {
			holders++
			if !slices.Contains(two.Replicas, n.Name) {
				t.Fatalf("%s holds bytes but is not a recorded replica", n.Name)
			}
		}
	}
	if holders != 2 || two.ReplicationFactor != 2 {
		t.Fatalf("rf=2 object on %d nodes (rf %d)", holders, two.ReplicationFactor)
	}

	// An RF the cluster can never meet is rejected, not downgraded.
	r = e.do(http.MethodPut, "/objects/four.bin?replication_factor=4", []byte("x"))
	e.mustStatus(r, http.StatusBadRequest, "PUT rf=4 on 3 nodes")
}

// HARD GATE 2: a replica that is corrupted on disk while its node stays
// alive and healthy is detected by checksum, rejected, and never served.
func TestGateCorruptedButAliveReplicaIsRejected(t *testing.T) {
	e := startCluster(t, 3, 3)
	ctx := context.Background()
	data := randomBytes(1<<20, 2)

	// 1. write to three nodes
	obj := e.put("victim.bin", data)
	// 2. verify all three replicas
	for _, id := range []string{"node-1", "node-2", "node-3"} {
		vr, err := e.nodeClient(id).VerifyReplica(ctx, "victim.bin", obj.Version)
		if err != nil || !vr.Valid {
			t.Fatalf("%s: replica not valid before corruption: %+v %v", id, vr, err)
		}
	}
	// 3. corrupt node-2's bytes on disk (same size, different content)
	path := e.replicaPath("node-2", "victim.bin", obj.Version)
	b, _ := os.ReadFile(path)
	for i := 1000; i < 1016; i++ {
		b[i] ^= 0xFF
	}
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
	// 4. node-2 stays alive and healthy
	if !e.c.Node("node-2").Running() {
		t.Fatal("node-2 process is not running")
	}
	if _, err := e.nodeClient("node-2").Health(ctx); err != nil {
		t.Fatalf("node-2 /health: %v", err)
	}
	if st := e.nodeState("node-2"); st != storage.StateHealthy {
		t.Fatalf("API sees node-2 as %s; corruption must not look like a crash", st)
	}

	// 5. GET, directing the read at node-2 first
	r := e.do(http.MethodGet, "/objects/victim.bin?prefer=node-2", nil)
	e.mustStatus(r, http.StatusOK, "GET victim.bin")

	// 6. node-2 produced a checksum mismatch; 7. it was rejected
	if got := rejected(r.Header)["node-2"]; got != "checksum_mismatch" {
		t.Fatalf("node-2 outcome = %q, want checksum_mismatch (header %q)", got, r.Header.Get(storage.HeaderRejectedReplicas))
	}
	served := r.Header.Get(storage.HeaderServedBy)
	if served == "node-2" || served == "" {
		t.Fatalf("served by %q", served)
	}
	// 8. the valid object came from another replica
	if !bytes.Equal(r.Body, data) || sha(r.Body) != obj.SHA256 {
		t.Fatal("GET returned bytes that do not match the original")
	}

	// The event is in the API's structured log, attributed to node-2.
	found := false
	for _, ev := range e.logEvents(e.c.API, "CHECKSUM_MISMATCH") {
		if ev["node_id"] == "node-2" && ev["object_id"] == "victim.bin" && ev["actual_sha256"] != obj.SHA256 {
			found = true
		}
	}
	if !found {
		t.Fatal("no CHECKSUM_MISMATCH event for node-2 in api log")
	}
	// And it is recorded against the object's metadata.
	m := e.metadata("victim.bin")
	if len(m.IntegrityFailures) != 1 || m.IntegrityFailures[0].NodeID != "node-2" || m.IntegrityFailures[0].Reason != "checksum_mismatch" {
		t.Fatalf("integrity_failures = %+v", m.IntegrityFailures)
	}
	// Node-side verification agrees the replica is bad. Stage 1 does not
	// repair it.
	if vr, _ := e.nodeClient("node-2").VerifyReplica(ctx, "victim.bin", obj.Version); vr.Valid {
		t.Fatal("node-2 replica unexpectedly valid")
	}
	if !e.c.Node("node-2").Running() {
		t.Fatal("node-2 died")
	}

	// A plain GET with no preference also routes around corruption when the
	// corrupted replica is the one normally read first.
	data2 := randomBytes(200_000, 3)
	obj2 := e.put("primary.bin", data2)
	first := obj2.Replicas[0]
	p2 := e.replicaPath(first, "primary.bin", obj2.Version)
	b2, _ := os.ReadFile(p2)
	b2[0] ^= 0x01
	os.WriteFile(p2, b2, 0o644)
	r = e.do(http.MethodGet, "/objects/primary.bin", nil)
	e.mustStatus(r, http.StatusOK, "GET primary.bin")
	if rejected(r.Header)[first] != "checksum_mismatch" || r.Header.Get(storage.HeaderServedBy) == first || !bytes.Equal(r.Body, data2) {
		t.Fatalf("plain GET: served by %s, rejected %v", r.Header.Get(storage.HeaderServedBy), rejected(r.Header))
	}
}

// HARD GATE 3: losing one node process does not make an object unavailable.
func TestGateSingleNodeFailure(t *testing.T) {
	e := startCluster(t, 3, 3)
	data := randomBytes(2<<20, 4)
	obj := e.put("survivor.bin", data)

	if err := e.c.StopNode("node-2"); err != nil {
		t.Fatal(err)
	}
	if e.c.Node("node-2").Running() {
		t.Fatal("node-2 still running")
	}

	// Immediately (before the prober notices) and pointed at the dead node.
	r := e.do(http.MethodGet, "/objects/survivor.bin?prefer=node-2", nil)
	e.mustStatus(r, http.StatusOK, "GET with node-2 down")
	if rejected(r.Header)["node-2"] != "node_unavailable" {
		t.Fatalf("node-2 outcome = %v", rejected(r.Header))
	}
	served := r.Header.Get(storage.HeaderServedBy)
	if served != "node-1" && served != "node-3" {
		t.Fatalf("served by %q", served)
	}
	if sha(r.Body) != obj.SHA256 || !bytes.Equal(r.Body, data) {
		t.Fatal("checksum does not verify after node failure")
	}

	// The coordinator now knows node-2 is down; plain reads keep working.
	e.waitNodeState("node-2", storage.StateUnavailable)
	r = e.do(http.MethodGet, "/objects/survivor.bin", nil)
	e.mustStatus(r, http.StatusOK, "plain GET with node-2 down")
	if !bytes.Equal(r.Body, data) {
		t.Fatal("wrong bytes")
	}
	if len(e.logEvents(e.c.API, "NODE_UNAVAILABLE")) == 0 {
		t.Fatal("no NODE_UNAVAILABLE event")
	}

	// With only two healthy nodes, RF=3 must NOT report success …
	r = e.do(http.MethodPut, "/objects/new.bin", []byte("needs three"))
	e.mustStatus(r, http.StatusServiceUnavailable, "PUT rf=3 with 2 healthy nodes")
	if !bytes.Contains(r.Body, []byte("insufficient_healthy_nodes")) {
		t.Fatalf("body: %s", r.Body)
	}
	e.mustStatus(e.do(http.MethodGet, "/objects/new.bin/metadata", nil), http.StatusNotFound, "metadata of failed write")
	for _, id := range []string{"node-1", "node-3"} {
		if _, err := os.Stat(e.replicaPath(id, "new.bin", 1)); err == nil {
			t.Fatalf("%s holds bytes of a write that was refused", id)
		}
	}
	// … while an explicitly requested RF=2 is satisfiable and succeeds.
	e.mustStatus(e.do(http.MethodPut, "/objects/new.bin?replication_factor=2", []byte("two is fine")), http.StatusCreated, "PUT rf=2")

	// Recovery is noticed.
	if err := e.c.StartNode(context.Background(), "node-2"); err != nil {
		t.Fatal(err)
	}
	e.waitNodeState("node-2", storage.StateHealthy)
	if len(e.logEvents(e.c.API, "NODE_RECOVERED")) == 0 {
		t.Fatal("no NODE_RECOVERED event")
	}
}

// HARD GATE 4: a restarted node serves the objects it stored before.
func TestGateNodeRestartPreservesData(t *testing.T) {
	e := startCluster(t, 3, 3)
	ctx := context.Background()
	data := randomBytes(1<<20, 5)
	obj := e.put("durable.bin", data)

	// SIGKILL: no chance to flush anything at shutdown.
	oldPID := e.c.Node("node-1").PID()
	if err := e.c.KillNode("node-1"); err != nil {
		t.Fatal(err)
	}
	if err := e.c.StartNode(ctx, "node-1"); err != nil {
		t.Fatal(err)
	}
	if e.c.Node("node-1").PID() == oldPID {
		t.Fatal("node-1 was not restarted")
	}
	e.waitNodeState("node-1", storage.StateHealthy)

	// Straight from the restarted node …
	rc, err := e.nodeClient("node-1").GetReplica(ctx, "durable.bin", obj.Version)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if sha(got) != obj.SHA256 {
		t.Fatal("restarted node returned different bytes")
	}
	// … and through the API, forced onto node-1.
	r := e.do(http.MethodGet, "/objects/durable.bin?prefer=node-1", nil)
	e.mustStatus(r, http.StatusOK, "GET after restart")
	if r.Header.Get(storage.HeaderServedBy) != "node-1" || sha(r.Body) != obj.SHA256 {
		t.Fatalf("served by %s", r.Header.Get(storage.HeaderServedBy))
	}
	h, err := e.nodeClient("node-1").Health(ctx)
	if err != nil || h.Capacity.ReplicaCount != 1 || h.Capacity.UsedBytes != int64(len(data)) {
		t.Fatalf("capacity after restart = %+v, %v", h.Capacity, err)
	}

	// Metadata survives an API restart too.
	if err := e.c.RestartAPI(ctx); err != nil {
		t.Fatal(err)
	}
	if err := e.c.WaitHealthyNodes(ctx, 3, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	r = e.do(http.MethodGet, "/objects/durable.bin", nil)
	e.mustStatus(r, http.StatusOK, "GET after API restart")
	if !bytes.Equal(r.Body, data) {
		t.Fatal("wrong bytes after API restart")
	}
}

func TestDeleteRemovesReplicaFiles(t *testing.T) {
	e := startCluster(t, 3, 3)
	obj := e.put("gone.bin", []byte("delete me"))
	r := e.do(http.MethodDelete, "/objects/gone.bin", nil)
	e.mustStatus(r, http.StatusOK, "DELETE")
	if !bytes.Contains(r.Body, []byte(`"fully_purged": true`)) {
		t.Fatalf("body: %s", r.Body)
	}
	for _, id := range obj.Replicas {
		if _, err := os.Stat(e.replicaPath(id, "gone.bin", obj.Version)); !os.IsNotExist(err) {
			t.Fatalf("%s still has the file (err=%v)", id, err)
		}
	}
	e.mustStatus(e.do(http.MethodGet, "/objects/gone.bin", nil), http.StatusNotFound, "GET deleted")
	e.mustStatus(e.do(http.MethodHead, "/objects/gone.bin", nil), http.StatusNotFound, "HEAD deleted")
	e.mustStatus(e.do(http.MethodDelete, "/objects/gone.bin", nil), http.StatusNotFound, "DELETE again")
	if len(e.logEvents(e.c.API, "OBJECT_DELETE")) != 1 {
		t.Fatal("expected one OBJECT_DELETE event")
	}
}

func TestOverwriteVersionsAndCleansUp(t *testing.T) {
	e := startCluster(t, 3, 3)
	v1 := e.put("doc.txt", []byte("first"))
	r := e.do(http.MethodPut, "/objects/doc.txt", []byte("second version"))
	e.mustStatus(r, http.StatusOK, "overwrite")
	v2 := e.metadata("doc.txt")
	if v2.Version != v1.Version+1 || !v2.CreatedAt.Equal(v1.CreatedAt) || !v2.UpdatedAt.After(v1.UpdatedAt) {
		t.Fatalf("v1 %+v v2 %+v", v1, v2)
	}
	for _, id := range v1.Replicas {
		if _, err := os.Stat(e.replicaPath(id, "doc.txt", v1.Version)); !os.IsNotExist(err) {
			t.Fatalf("%s still holds superseded version", id)
		}
	}
	r = e.do(http.MethodGet, "/objects/doc.txt", nil)
	if string(r.Body) != "second version" || r.Header.Get(storage.HeaderVersion) != strconv.FormatUint(v2.Version, 10) {
		t.Fatalf("got %q v%s", r.Body, r.Header.Get(storage.HeaderVersion))
	}
}
