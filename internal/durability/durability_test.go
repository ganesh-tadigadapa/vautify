package durability

import (
	"strings"
	"testing"
	"time"

	"vault/internal/metadata"
	"vault/internal/storage"
)

func states(down ...string) NodeStateFunc {
	return func(id string) storage.NodeState {
		for _, d := range down {
			if d == id {
				return storage.StateUnavailable
			}
		}
		return storage.StateHealthy
	}
}

func obj(id string, rf int, replicas ...string) metadata.Object {
	return metadata.Object{ID: id, Version: 1, Size: 10, SHA256: "x", ReplicationFactor: rf, Replicas: replicas}
}

func TestClassify(t *testing.T) {
	cases := []struct {
		healthy, rf int
		want        State
	}{
		{3, 3, Healthy}, {2, 3, Degraded}, {1, 3, Critical}, {0, 3, Lost},
		{4, 3, Healthy}, {1, 1, Healthy}, {0, 1, Lost}, {1, 2, Critical},
		{4, 5, Degraded}, {2, 5, Degraded}, {1, 5, Critical},
	}
	for _, c := range cases {
		if got := Classify(c.healthy, c.rf); got != c.want {
			t.Errorf("Classify(%d,%d) = %s, want %s", c.healthy, c.rf, got, c.want)
		}
	}
}

func TestAssessSeparatesNodeAndReplicaState(t *testing.T) {
	o := obj("a", 3, "node-1", "node-2", "node-3")
	o.IntegrityFailures = []metadata.IntegrityFailure{
		{NodeID: "node-2", Version: 1, Reason: metadata.ReasonChecksumMismatch, LastDetectedAt: time.Now()},
		{NodeID: "node-3", Version: 0, Reason: metadata.ReasonChecksumMismatch}, // older version: ignored
	}
	h := Assess(o, states())
	if h.State != Degraded || h.HealthyReplicas != 2 {
		t.Fatalf("state %s healthy %d", h.State, h.HealthyReplicas)
	}
	r, _ := h.Replica("node-2")
	if r.NodeState != storage.StateHealthy || r.State != ReplicaCorrupted {
		t.Fatalf("node-2 = %+v; want alive node with CORRUPTED replica", r)
	}
	if r3, _ := h.Replica("node-3"); r3.State != ReplicaCurrent {
		t.Fatalf("stale-version failure counted: %+v", r3)
	}

	h = Assess(o, states("node-1", "node-3"))
	if h.State != Lost || !strings.HasPrefix(h.Reason, "No verified replica") {
		t.Fatalf("got %s %q", h.State, h.Reason)
	}
	h = Assess(obj("b", 3, "node-1", "node-2", "node-3"), states("node-2", "node-3"))
	if h.State != Critical || h.Reason != "Only 1 verified replica remains for RF=3 (node-2 UNAVAILABLE, node-3 UNAVAILABLE)." {
		t.Fatalf("got %s %q", h.State, h.Reason)
	}
	if !h.NeedsRepair() || Assess(o, states("node-1", "node-3")).NeedsRepair() {
		t.Fatal("NeedsRepair wrong")
	}
}

func TestAssessMissingSlots(t *testing.T) {
	h := Assess(obj("a", 3, "node-1", "node-2"), states())
	if h.State != Degraded || !strings.Contains(h.Reason, "1 replica slot(s) unassigned") {
		t.Fatalf("%s %q", h.State, h.Reason)
	}
}

func TestBlastRadius(t *testing.T) {
	objs := []metadata.Object{
		obj("a", 3, "node-1", "node-2", "node-3"), // node-3 already down -> critical
		obj("b", 3, "node-2", "node-4", "node-5"), // degraded
		obj("c", 2, "node-2", "node-4"),           // critical
		obj("d", 1, "node-2"),                     // lost
		obj("e", 3, "node-1", "node-4", "node-5"), // not affected
		obj("f", 2, "node-1", "node-2", "node-4"), // over-replicated: protected
	}
	br := ComputeBlastRadius(objs, "node-2", states("node-3"), time.Now())
	if br.AffectedObjects != 5 || br.Protected != 1 || br.Degraded != 1 || br.Critical != 2 || br.Lost != 1 {
		t.Fatalf("%+v", br)
	}
	if br.Objects[0].ObjectID != "d" || br.Objects[1].State != Critical {
		t.Fatalf("not sorted by severity: %+v", br.Objects)
	}
	if br.NodeState != storage.StateHealthy {
		t.Fatal("hypothetical blast radius must report the node's real state")
	}
}
