package api

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"vault/internal/coordinator"
	"vault/internal/durability"
	"vault/internal/httpx"
	"vault/internal/netchaos"
	"vault/internal/repair"
	"vault/internal/storage"
)

// registerResilience adds the Phase 4 endpoints: network fault injection,
// failure diagnosis, reconciliation and storage metrics.
func (s *Server) registerResilience(mux *http.ServeMux) {
	mux.HandleFunc("GET /cluster/diagnosis", s.handleDiagnosis)
	mux.HandleFunc("GET /storage/metrics", s.handleStorageMetrics)
	mux.HandleFunc("POST /reconcile", s.withSelfHeal(s.handleReconcileAll))
	mux.HandleFunc("POST /nodes/{id}/reconcile", s.withSelfHeal(s.handleReconcileNode))

	mux.HandleFunc("GET /chaos/network", s.withNetwork(s.handleNetwork))
	mux.HandleFunc("POST /chaos/partition/{nodeId}", s.withNetwork(s.handlePartition))
	mux.HandleFunc("POST /chaos/latency/{nodeId}", s.withNetwork(s.handleLatency))
	mux.HandleFunc("POST /chaos/heal/{nodeId}", s.withNetwork(s.handleHeal))
	mux.HandleFunc("POST /chaos/heal", s.withNetwork(s.handleHealAll))
}

func (s *Server) withNetwork(h http.HandlerFunc) http.HandlerFunc {
	return s.withChaos(func(w http.ResponseWriter, r *http.Request) {
		if s.opts.Network == nil {
			httpx.WriteError(w, http.StatusServiceUnavailable, "network_chaos_unavailable", "no network fault injector is configured")
			return
		}
		h(w, r)
	})
}

// Failure diagnoses. A node the coordinator cannot reach is either down or
// cut off by the network; the coordinator alone cannot tell which. It can
// when the cut is one it injected. Corrupted data is a third, different
// failure: the node answers, its bytes are wrong.
const (
	DiagnosisHealthy          = "HEALTHY"
	DiagnosisSlowLink         = "SLOW_LINK"
	DiagnosisNetworkPartition = "NETWORK_PARTITION"
	DiagnosisNodeUnreachable  = "NODE_UNREACHABLE"
	DiagnosisCorruptedData    = "CORRUPTED_DATA"
)

// NodeDiagnosis classifies what is wrong with one node, if anything.
type NodeDiagnosis struct {
	NodeID    string               `json:"node_id"`
	Health    storage.NodeState    `json:"health"`
	LastError string               `json:"last_error,omitempty"`
	Link      *netchaos.LinkStatus `json:"link,omitempty"`
	// Replica findings recorded against this node for current versions.
	CorruptedReplicas int    `json:"corrupted_replicas"`
	StaleReplicas     int    `json:"stale_replicas"`
	MissingReplicas   int    `json:"missing_replicas"`
	Diagnosis         string `json:"diagnosis"`
	Explanation       string `json:"explanation"`
}

func (s *Server) diagnose(r *http.Request) ([]NodeDiagnosis, error) {
	objs, err := s.meta.ListObjects(r.Context())
	if err != nil {
		return nil, err
	}
	nodes := s.coord.Nodes()
	state := map[string]storage.NodeState{}
	for _, n := range nodes {
		state[n.ID] = n.State
	}
	stateOf := func(id string) storage.NodeState { return state[id] }
	type counts struct{ corrupted, stale, missing int }
	per := map[string]*counts{}
	for _, o := range objs {
		for _, rep := range durability.Assess(o, stateOf).Replicas {
			c := per[rep.NodeID]
			if c == nil {
				c = &counts{}
				per[rep.NodeID] = c
			}
			switch rep.State {
			case durability.ReplicaCorrupted:
				c.corrupted++
			case durability.ReplicaStale:
				c.stale++
			case durability.ReplicaMissing:
				c.missing++
			}
		}
	}
	out := make([]NodeDiagnosis, 0, len(nodes))
	for _, n := range nodes {
		d := NodeDiagnosis{NodeID: n.ID, Health: n.State, LastError: n.LastError}
		if c := per[n.ID]; c != nil {
			d.CorruptedReplicas, d.StaleReplicas, d.MissingReplicas = c.corrupted, c.stale, c.missing
		}
		if s.opts.Network != nil {
			if l, ok := s.opts.Network.Link(n.ID); ok {
				d.Link = &l
			}
		}
		switch {
		case d.Link != nil && d.Link.Partitioned:
			d.Diagnosis = DiagnosisNetworkPartition
			d.Explanation = "the coordinator→node link is cut (injected partition). The node process is not " +
				"affected and keeps its data; the coordinator treats it exactly like an unreachable node, and " +
				"reconciles it against metadata when the link heals"
		case n.State != storage.StateHealthy:
			d.Diagnosis = DiagnosisNodeUnreachable
			d.Explanation = "the node does not answer (no injected partition): its process or host is down, " +
				"or a real network fault that the coordinator cannot tell apart from one"
		case d.CorruptedReplicas > 0:
			d.Diagnosis = DiagnosisCorruptedData
			d.Explanation = fmt.Sprintf("the node answers, but %d replica(s) on it failed checksum verification; "+
				"reads reject them and repair replaces them", d.CorruptedReplicas)
		case d.Link != nil && d.Link.LatencyMS > 0:
			d.Diagnosis = DiagnosisSlowLink
			d.Explanation = fmt.Sprintf("reachable, with %dms injected latency on every request", d.Link.LatencyMS)
		default:
			d.Diagnosis = DiagnosisHealthy
			d.Explanation = "reachable, no recorded replica problems"
		}
		out = append(out, d)
	}
	return out, nil
}

func (s *Server) handleDiagnosis(w http.ResponseWriter, r *http.Request) {
	d, err := s.diagnose(r)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"nodes": d, "network_injection": s.opts.Network != nil})
}

func (s *Server) handleNetwork(w http.ResponseWriter, r *http.Request) {
	d, err := s.diagnose(r)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"links": s.opts.Network.Status(), "diagnosis": d})
}

// handlePartition cuts a link now (?duration=10s heals it by itself), or
// with ?trigger=upload|download|ack[&after_bytes=N][&method=PUT] arms a cut
// in the middle of the next matching replica transfer.
func (s *Server) handlePartition(w http.ResponseWriter, r *http.Request) {
	id, q := r.PathValue("nodeId"), r.URL.Query()
	if point := q.Get("trigger"); point != "" {
		t := netchaos.Trigger{Point: point, Method: q.Get("method")}
		if v := q.Get("after_bytes"); v != "" {
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				httpx.WriteError(w, http.StatusBadRequest, "invalid_after_bytes", "after_bytes must be an integer")
				return
			}
			t.AfterBytes = n
		}
		if err := s.opts.Network.Arm(id, t); err != nil {
			s.writeChaosErr(w, r, err)
			return
		}
	} else {
		var d time.Duration
		if v := q.Get("duration"); v != "" {
			var err error
			if d, err = time.ParseDuration(v); err != nil || d < 0 {
				httpx.WriteError(w, http.StatusBadRequest, "invalid_duration", "duration must be a Go duration such as 10s")
				return
			}
		}
		if err := s.opts.Network.Partition(id, d); err != nil {
			s.writeChaosErr(w, r, err)
			return
		}
	}
	l, _ := s.opts.Network.Link(id)
	httpx.WriteJSON(w, http.StatusOK, l)
}

func (s *Server) handleLatency(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("nodeId")
	ms, err := strconv.Atoi(r.URL.Query().Get("ms"))
	if err != nil || ms < 0 {
		httpx.WriteError(w, http.StatusBadRequest, "invalid_latency", "ms must be a non-negative integer")
		return
	}
	if err := s.opts.Network.SetLatency(id, time.Duration(ms)*time.Millisecond); err != nil {
		s.writeChaosErr(w, r, err)
		return
	}
	l, _ := s.opts.Network.Link(id)
	httpx.WriteJSON(w, http.StatusOK, l)
}

func (s *Server) handleHeal(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("nodeId")
	if err := s.opts.Network.Heal(id); err != nil {
		s.writeChaosErr(w, r, err)
		return
	}
	l, _ := s.opts.Network.Link(id)
	httpx.WriteJSON(w, http.StatusOK, l)
}

func (s *Server) handleHealAll(w http.ResponseWriter, r *http.Request) {
	s.opts.Network.HealAll()
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"links": s.opts.Network.Status()})
}

func (s *Server) writeChaosErr(w http.ResponseWriter, r *http.Request, err error) {
	if _, isNode := s.opts.Network.Link(r.PathValue("nodeId")); !isNode {
		httpx.WriteError(w, http.StatusNotFound, "unknown_node", err.Error())
		return
	}
	httpx.WriteError(w, http.StatusBadRequest, "invalid_fault", err.Error())
}

func (s *Server) handleReconcileNode(w http.ResponseWriter, r *http.Request) {
	rep, err := s.opts.SelfHeal.Reconcile(r.Context(), r.PathValue("id"))
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, rep)
}

func (s *Server) handleReconcileAll(w http.ResponseWriter, r *http.Request) {
	reps, err := s.opts.SelfHeal.ReconcileAll(r.Context())
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"nodes": reps})
}

// RepairBytes is the repair traffic since the API started.
type RepairBytes struct {
	// BytesWritten: bytes written to repair targets (each repaired replica
	// counts its object size once; the verified source read through the
	// coordinator's spool costs the same amount again).
	BytesWritten    int64 `json:"bytes_written"`
	ReplicasCreated int64 `json:"replicas_created"`
	Repaired        int64 `json:"objects_repaired"`
}

// StorageMetricsResponse is GET /storage/metrics.
type StorageMetricsResponse struct {
	coordinator.StorageMetrics
	Repair *RepairBytes `json:"repair,omitempty"`
}

func (s *Server) handleStorageMetrics(w http.ResponseWriter, r *http.Request) {
	m, err := s.coord.StorageMetrics(r.Context())
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	resp := StorageMetricsResponse{StorageMetrics: m}
	if s.opts.SelfHeal != nil {
		rm := s.opts.SelfHeal.Repairs().Metrics()
		resp.Repair = repairBytes(rm)
	}
	httpx.WriteJSON(w, http.StatusOK, resp)
}

func repairBytes(m repair.Metrics) *RepairBytes {
	return &RepairBytes{BytesWritten: m.BytesTransferred, ReplicasCreated: m.ReplicasCreated, Repaired: m.Completed}
}
