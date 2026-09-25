package integration

import (
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"vault/internal/cluster"
	"vault/internal/durability"
	"vault/internal/events"
	"vault/internal/placement"
	"vault/internal/repair"
	"vault/internal/selfheal"
)

type healOpts struct {
	workers int
	extra   []string
}

// startSelfHealing launches a cluster with automatic repair and chaos
// endpoints enabled (periodic scrub off: tests trigger scrubs explicitly).
func startSelfHealing(t *testing.T, nodes int, o healOpts) *env {
	t.Helper()
	return launch(t, cluster.Spec{
		Nodes: nodes, ReplicationFactor: 3, ProbeInterval: 200 * time.Millisecond,
		AutoRepair: true, EnableChaos: true, RepairWorkers: o.workers,
		SweepInterval: 300 * time.Millisecond, ExtraAPIArgs: o.extra,
	})
}

func nodeNames(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("node-%d", i+1)
	}
	return out
}

// nameOn finds an object ID with the given prefix that rendezvous placement
// puts on exactly the given nodes (all nodes healthy at write time).
func nameOn(t *testing.T, prefix string, all, want []string) string {
	t.Helper()
	for i := 0; i < 100000; i++ {
		id := fmt.Sprintf("%s-%d", prefix, i)
		got := slices.Clone(placement.Rank(id, all)[:len(want)])
		slices.Sort(got)
		w := slices.Clone(want)
		slices.Sort(w)
		if slices.Equal(got, w) {
			return id
		}
	}
	t.Fatalf("no id with prefix %s lands on %v", prefix, want)
	return ""
}

func (e *env) getJSON(path string, v any) {
	e.t.Helper()
	r := e.do(http.MethodGet, path, nil)
	e.mustStatus(r, http.StatusOK, "GET "+path)
	if err := json.Unmarshal(r.Body, v); err != nil {
		e.t.Fatalf("GET %s: %v: %s", path, err, r.Body)
	}
}

func (e *env) post(path string, want int, v any) {
	e.t.Helper()
	r := e.do(http.MethodPost, path, nil)
	e.mustStatus(r, want, "POST "+path)
	if v != nil {
		if err := json.Unmarshal(r.Body, v); err != nil {
			e.t.Fatalf("POST %s: %v: %s", path, err, r.Body)
		}
	}
}

func (e *env) health(id string) selfheal.ObjectHealthView {
	var v selfheal.ObjectHealthView
	e.getJSON("/objects/"+id+"/health", &v)
	return v
}

func (e *env) queue() repair.Snapshot {
	var s repair.Snapshot
	e.getJSON("/repair/queue", &s)
	return s
}

type metricsBody struct {
	Repair  repair.Metrics    `json:"repair"`
	Objects durability.Counts `json:"objects"`
}

func (e *env) metrics() metricsBody {
	var m metricsBody
	e.getJSON("/repair/metrics", &m)
	return m
}

func (e *env) history() []repair.HistoryEntry {
	var h struct{ History []repair.HistoryEntry }
	e.getJSON("/repair/history", &h)
	return h.History
}

func (e *env) status() selfheal.ClusterStatus {
	var s selfheal.ClusterStatus
	e.getJSON("/cluster/status", &s)
	return s
}

func (e *env) events(name string) []events.Event {
	var body struct{ Events []events.Event }
	e.getJSON("/events?event="+name, &body)
	return body.Events
}

func (e *env) eventsFor(name, objectID string) []events.Event {
	var out []events.Event
	for _, ev := range e.events(name) {
		if ev.Fields["object_id"] == objectID {
			out = append(out, ev)
		}
	}
	return out
}

func (e *env) waitFor(what string, timeout time.Duration, cond func() bool) {
	e.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	e.t.Fatalf("timed out after %s waiting for %s", timeout, what)
}

// waitAllHealthy waits until every object is HEALTHY and the repair queue
// is empty with nothing running.
func (e *env) waitAllHealthy(timeout time.Duration) {
	e.t.Helper()
	e.waitFor("all objects HEALTHY and repair idle", timeout, func() bool {
		st := e.status()
		return st.Objects.Healthy == st.Objects.Total && st.Repair.QueueDepth == 0 && st.Repair.Active == 0
	})
}

func (e *env) kill(nodeID string) {
	e.t.Helper()
	var kr struct {
		ConfirmedUnavailable bool `json:"confirmed_unavailable"`
	}
	e.post("/chaos/kill-node/"+nodeID, http.StatusOK, &kr)
	if !kr.ConfirmedUnavailable {
		e.t.Fatalf("kill of %s not confirmed", nodeID)
	}
	e.waitFor(nodeID+" process exit", 5*time.Second, func() bool { return !e.c.Node(nodeID).Running() })
}

func historyFor(h []repair.HistoryEntry, id string) []repair.HistoryEntry {
	var out []repair.HistoryEntry
	for _, x := range h {
		if x.ObjectID == id {
			out = append(out, x)
		}
	}
	return out
}

func seqOf(t *testing.T, evs []events.Event, what string) uint64 {
	t.Helper()
	if len(evs) == 0 {
		t.Fatalf("no %s event", what)
	}
	return evs[0].Seq
}

func mustContain(t *testing.T, s, sub string) {
	t.Helper()
	if !strings.Contains(s, sub) {
		t.Fatalf("%q does not contain %q", s, sub)
	}
}
