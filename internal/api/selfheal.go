package api

import (
	"net/http"
	"strconv"

	"vault/internal/httpx"
	"vault/internal/objectid"
)

// registerSelfHeal adds the Phase 2 endpoints. Every handler is a thin
// wrapper over the same controller/coordinator methods the CLI and tests
// use; there is no separate implementation for the UI.
func (s *Server) registerSelfHeal(mux *http.ServeMux) {
	mux.HandleFunc("GET /cluster/status", s.withSelfHeal(s.handleClusterStatus))
	mux.HandleFunc("GET /nodes/{id}/blast-radius", s.withSelfHeal(s.handleBlastRadius))
	mux.HandleFunc("GET /objects/{id}/health", s.withSelfHeal(s.handleObjectHealth))
	mux.HandleFunc("POST /objects/{id}/repair", s.withSelfHeal(s.handleRequestRepair))
	mux.HandleFunc("POST /scrub/run", s.withSelfHeal(s.handleScrub))
	mux.HandleFunc("GET /repair/queue", s.withSelfHeal(s.handleRepairQueue))
	mux.HandleFunc("GET /repair/metrics", s.withSelfHeal(s.handleRepairMetrics))
	mux.HandleFunc("GET /repair/history", s.withSelfHeal(s.handleRepairHistory))
	mux.HandleFunc("POST /repair/pause", s.withSelfHeal(s.handleRepairPause))
	mux.HandleFunc("POST /repair/resume", s.withSelfHeal(s.handleRepairResume))
	mux.HandleFunc("POST /repair/sweep", s.withSelfHeal(s.handleSweep))
	mux.HandleFunc("GET /events", s.handleEvents)
	mux.HandleFunc("POST /chaos/kill-node/{nodeId}", s.withChaos(s.handleKillNode))
	mux.HandleFunc("POST /chaos/corrupt-replica/{objectId}/{nodeId}", s.withChaos(s.handleCorruptReplica))
}

func (s *Server) withSelfHeal(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.opts.SelfHeal == nil {
			httpx.WriteError(w, http.StatusServiceUnavailable, "self_heal_disabled", "self-healing subsystem is not configured")
			return
		}
		h(w, r)
	}
}

func (s *Server) withChaos(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.opts.EnableChaos {
			httpx.WriteError(w, http.StatusForbidden, "chaos_disabled", "api was started without --enable-chaos")
			return
		}
		h(w, r)
	}
}

func (s *Server) handleClusterStatus(w http.ResponseWriter, r *http.Request) {
	st, err := s.opts.SelfHeal.Status(r.Context())
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, st)
}

func (s *Server) handleBlastRadius(w http.ResponseWriter, r *http.Request) {
	br, err := s.opts.SelfHeal.BlastRadius(r.Context(), r.PathValue("id"))
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, br)
}

func (s *Server) handleObjectHealth(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := objectid.Validate(id); err != nil {
		s.writeErr(w, r, err)
		return
	}
	v, err := s.opts.SelfHeal.ObjectHealth(r.Context(), id)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, v)
}

func (s *Server) handleRequestRepair(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := objectid.Validate(id); err != nil {
		s.writeErr(w, r, err)
		return
	}
	h, outcome, err := s.opts.SelfHeal.RequestRepair(r.Context(), id)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusAccepted, map[string]any{"object": h, "enqueue": outcome})
}

func (s *Server) handleScrub(w http.ResponseWriter, r *http.Request) {
	rep, err := s.opts.SelfHeal.Scrub(r.Context())
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, rep)
}

func (s *Server) handleRepairQueue(w http.ResponseWriter, r *http.Request) {
	httpx.WriteJSON(w, http.StatusOK, s.opts.SelfHeal.Repairs().Snapshot())
}

func (s *Server) handleRepairMetrics(w http.ResponseWriter, r *http.Request) {
	st, err := s.opts.SelfHeal.Status(r.Context())
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"repair":  s.opts.SelfHeal.Repairs().Metrics(),
		"objects": st.Objects,
	})
}

func (s *Server) handleRepairHistory(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"history": s.opts.SelfHeal.Repairs().History(limit)})
}

func (s *Server) handleRepairPause(w http.ResponseWriter, r *http.Request) {
	s.opts.SelfHeal.Repairs().Pause()
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"paused": true})
}

func (s *Server) handleRepairResume(w http.ResponseWriter, r *http.Request) {
	s.opts.SelfHeal.Repairs().Resume()
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"paused": false})
}

func (s *Server) handleSweep(w http.ResponseWriter, r *http.Request) {
	rep, err := s.opts.SelfHeal.Sweep(r.Context())
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, rep)
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	if s.opts.Events == nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, "events_disabled", "event recorder is not configured")
		return
	}
	since, _ := strconv.ParseUint(r.URL.Query().Get("since"), 10, 64)
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	evs := s.opts.Events.Since(since, limit)
	if name := r.URL.Query().Get("event"); name != "" {
		filtered := evs[:0]
		for _, e := range evs {
			if string(e.Event) == name {
				filtered = append(filtered, e)
			}
		}
		evs = filtered
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"events": evs})
}

func (s *Server) handleKillNode(w http.ResponseWriter, r *http.Request) {
	res, err := s.coord.KillNode(r.Context(), r.PathValue("nodeId"))
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, res)
}

func (s *Server) handleCorruptReplica(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("objectId")
	if err := objectid.Validate(id); err != nil {
		s.writeErr(w, r, err)
		return
	}
	res, err := s.coord.CorruptReplica(r.Context(), id, r.PathValue("nodeId"))
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, res)
}
