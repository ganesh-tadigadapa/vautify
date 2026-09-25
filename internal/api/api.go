// Package api exposes the coordinator as Vault's client HTTP API.
package api

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"vault/internal/coordinator"
	"vault/internal/events"
	"vault/internal/httpx"
	"vault/internal/integrity"
	"vault/internal/metadata"
	"vault/internal/netchaos"
	"vault/internal/objectid"
	"vault/internal/placement"
	"vault/internal/selfheal"
	"vault/internal/storage"
)

// Server is the client-facing API.
type Server struct {
	coord *coordinator.Coordinator
	meta  metadata.Store
	log   *events.Logger
	opts  Options
}

// Options enables the self-healing, observability and chaos endpoints.
type Options struct {
	// SelfHeal serves /cluster, /repair, /scrub and object health.
	SelfHeal *selfheal.Controller
	// Events serves the recent event timeline at /events.
	Events *events.Recorder
	// EnableChaos serves the /chaos endpoints. Off by default.
	EnableChaos bool
	// Network is the coordinator→node fault injector behind the partition
	// and latency chaos endpoints (nil when chaos is disabled).
	Network *netchaos.Injector
}

// New returns an API server backed by coord. meta is used for listing.
func New(coord *coordinator.Coordinator, meta metadata.Store, log *events.Logger, opts Options) *Server {
	return &Server{coord: coord, meta: meta, log: log, opts: opts}
}

// Handler returns the API routes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", s.handleDashboard)
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("GET /nodes", s.handleNodes)
	mux.HandleFunc("GET /objects", s.handleList)
	mux.HandleFunc("PUT /objects/{id}", s.handlePut)
	mux.HandleFunc("GET /objects/{id}", s.handleGet)
	mux.HandleFunc("HEAD /objects/{id}", s.handleHead)
	mux.HandleFunc("DELETE /objects/{id}", s.handleDelete)
	mux.HandleFunc("GET /objects/{id}/metadata", s.handleMetadata)
	mux.HandleFunc("GET /objects/{id}/consistency", s.handleConsistency)
	s.registerSelfHeal(mux)
	s.registerResilience(mux)
	return mux
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	nodes := s.coord.Nodes()
	healthy := 0
	for _, n := range nodes {
		if n.State == storage.StateHealthy {
			healthy++
		}
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"status":                     "ok",
		"role":                       "api",
		"nodes_total":                len(nodes),
		"nodes_healthy":              healthy,
		"default_replication_factor": s.coord.Config().DefaultReplicationFactor,
		"max_object_size_bytes":      s.coord.Config().MaxObjectSize,
		"self_heal_enabled":          s.opts.SelfHeal != nil,
		"chaos_enabled":              s.opts.EnableChaos,
		"write_quorum":               s.coord.Config().WriteQuorum.String(),
		"read_quorum":                s.coord.Config().ReadQuorum.String(),
	})
}

func (s *Server) handleNodes(w http.ResponseWriter, r *http.Request) {
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"nodes": s.coord.Nodes()})
}

func (s *Server) handleList(w http.ResponseWriter, r *http.Request) {
	objs, err := s.meta.ListObjects(r.Context())
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"objects": objs, "count": len(objs)})
}

// ReplicationStatus summarises whether a write met its policy.
type ReplicationStatus struct {
	Required int `json:"required"`
	Achieved int `json:"achieved"`
	// Status is "satisfied" (RF replicas) or "degraded" (the write quorum
	// was met with fewer than RF; repair restores RF).
	Status      string `json:"status"`
	WriteQuorum int    `json:"write_quorum"`
}

// PutResponse is the JSON body of a successful PUT.
type PutResponse struct {
	Object                metadata.Object              `json:"object"`
	Created               bool                         `json:"created"`
	PreviousVersion       uint64                       `json:"previous_version"`
	Replication           ReplicationStatus            `json:"replication"`
	Attempts              []coordinator.ReplicaAttempt `json:"attempts"`
	RetainedStaleReplicas []string                     `json:"retained_stale_replicas,omitempty"`
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	rf := 0
	if v := r.URL.Query().Get("replication_factor"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			httpx.WriteError(w, http.StatusBadRequest, "invalid_replication_factor", "replication_factor must be an integer")
			return
		}
		rf = n
	}
	if max := s.coord.Config().MaxObjectSize; r.ContentLength > max {
		httpx.WriteErrorDetails(w, http.StatusRequestEntityTooLarge, "object_too_large",
			"object exceeds the maximum size", map[string]int64{"max_bytes": max})
		return
	}

	expected, ok := expectedVersion(w, r)
	if !ok {
		return
	}
	wq, ok := quorumParam(w, r, "write_quorum")
	if !ok {
		return
	}
	res, err := s.coord.PutWithOptions(r.Context(), id, r.Body, coordinator.PutOptions{
		ReplicationFactor: rf, WriteQuorum: wq, ExpectedVersion: expected,
	})
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	status := http.StatusOK
	if res.Created {
		status = http.StatusCreated
	}
	repl := ReplicationStatus{
		Required: res.Object.ReplicationFactor, Achieved: len(res.Object.Replicas),
		Status: "satisfied", WriteQuorum: res.WriteQuorum,
	}
	if repl.Achieved < repl.Required {
		repl.Status = "degraded"
	}
	httpx.WriteJSON(w, status, PutResponse{
		Object:                res.Object,
		Created:               res.Created,
		PreviousVersion:       res.PreviousVersion,
		Replication:           repl,
		Attempts:              res.Attempts,
		RetainedStaleReplicas: res.RetainedStaleReplicas,
	})
}

func setObjectHeaders(h http.Header, obj metadata.Object) {
	h.Set("Content-Type", "application/octet-stream")
	h.Set("Content-Length", strconv.FormatInt(obj.Size, 10))
	h.Set("ETag", `"`+obj.SHA256+`"`)
	h.Set("Last-Modified", obj.UpdatedAt.Format(http.TimeFormat))
	h.Set(storage.HeaderSHA256, obj.SHA256)
	h.Set(storage.HeaderVersion, strconv.FormatUint(obj.Version, 10))
	h.Set(storage.HeaderReplicationFactor, strconv.Itoa(obj.ReplicationFactor))
	h.Set(storage.HeaderReplicas, strings.Join(obj.Replicas, ","))
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := objectid.Validate(id); err != nil {
		s.writeErr(w, r, err)
		return
	}
	rq, ok := quorumParam(w, r, "read_quorum")
	if !ok {
		return
	}
	res, err := s.coord.Get(r.Context(), id, coordinator.ReadOptions{PreferNode: r.URL.Query().Get("prefer"), ReadQuorum: rq})
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	defer res.Body.Close()

	h := w.Header()
	setObjectHeaders(h, res.Object)
	h.Set(storage.HeaderServedBy, res.ServedBy)
	h.Set(storage.HeaderReadQuorum, strconv.Itoa(res.ReadQuorum))
	h.Set(storage.HeaderConfirmedBy, strings.Join(res.Confirmed, ","))
	if rej := res.RejectedReplicas(); len(rej) > 0 {
		h.Set(storage.HeaderRejectedReplicas, strings.Join(rej, ","))
	}
	w.WriteHeader(http.StatusOK)
	if _, err := io.Copy(w, res.Body); err != nil {
		s.log.Error(r.Context(), "client download interrupted", "object_id", id, "error", err.Error())
	}
}

func (s *Server) handleHead(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := objectid.Validate(id); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	obj, err := s.coord.Stat(r.Context(), id)
	if errors.Is(err, metadata.ErrNotFound) {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	setObjectHeaders(w.Header(), obj)
	w.WriteHeader(http.StatusOK)
}

// quorumParam parses an optional ?write_quorum= / ?read_quorum= override
// ("all", "majority", "one" or a count). It writes a 400 on bad input.
func quorumParam(w http.ResponseWriter, r *http.Request, name string) (*coordinator.Quorum, bool) {
	v := r.URL.Query().Get(name)
	if v == "" {
		return nil, true
	}
	q, err := coordinator.ParseQuorum(v)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, "invalid_quorum", name+": "+err.Error())
		return nil, false
	}
	return &q, true
}

// expectedVersion parses the optional CAS condition from ?expected_version=
// or the X-Vault-Expected-Version header. It writes a 400 on bad input.
func expectedVersion(w http.ResponseWriter, r *http.Request) (*uint64, bool) {
	v := r.URL.Query().Get("expected_version")
	if v == "" {
		v = r.Header.Get(storage.HeaderExpectedVersion)
	}
	if v == "" {
		return nil, true
	}
	n, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, "invalid_expected_version", "expected_version must be a non-negative integer")
		return nil, false
	}
	return &n, true
}

func (s *Server) handleConsistency(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := objectid.Validate(id); err != nil {
		s.writeErr(w, r, err)
		return
	}
	rep, err := s.coord.Consistency(r.Context(), id, r.URL.Query().Get("verify") != "false")
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, rep)
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	expected, ok := expectedVersion(w, r)
	if !ok {
		return
	}
	var res coordinator.DeleteResult
	var err error
	if expected != nil {
		res, err = s.coord.DeleteIfVersion(r.Context(), r.PathValue("id"), *expected)
	} else {
		res, err = s.coord.Delete(r.Context(), r.PathValue("id"))
	}
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, res)
}

func (s *Server) handleMetadata(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := objectid.Validate(id); err != nil {
		s.writeErr(w, r, err)
		return
	}
	obj, err := s.coord.Stat(r.Context(), id)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, obj)
}

// writeErr maps domain errors to HTTP responses.
func (s *Server) writeErr(w http.ResponseWriter, r *http.Request, err error) {
	var (
		conflict     *coordinator.ConflictError
		quorum       *coordinator.QuorumError
		insufficient *placement.InsufficientNodesError
		replication  *coordinator.ReplicationError
		noVerified   *coordinator.NoVerifiedReplicaError
		maxBytes     *http.MaxBytesError
	)
	switch {
	case errors.Is(err, objectid.ErrInvalid):
		httpx.WriteError(w, http.StatusBadRequest, "invalid_object_id", err.Error())
	case errors.Is(err, coordinator.ErrInvalidReplicationFactor):
		httpx.WriteError(w, http.StatusBadRequest, "invalid_replication_factor", err.Error())
	case errors.Is(err, coordinator.ErrInvalidReadPreference):
		httpx.WriteError(w, http.StatusBadRequest, "invalid_read_preference", err.Error())
	case errors.As(err, &conflict):
		httpx.WriteErrorDetails(w, http.StatusConflict, "write_conflict", err.Error(), conflict)
	case errors.As(err, &quorum):
		httpx.WriteErrorDetails(w, http.StatusServiceUnavailable, "quorum_not_met", err.Error(), quorum)
	case errors.Is(err, coordinator.ErrInvalidQuorum):
		httpx.WriteError(w, http.StatusBadRequest, "invalid_quorum", err.Error())
	case errors.Is(err, netchaos.ErrUnknownNode):
		httpx.WriteError(w, http.StatusNotFound, "unknown_node", err.Error())
	case errors.Is(err, metadata.ErrNotFound):
		httpx.WriteError(w, http.StatusNotFound, "object_not_found", "object does not exist")
	case errors.As(err, &insufficient):
		httpx.WriteErrorDetails(w, http.StatusServiceUnavailable, "insufficient_healthy_nodes", err.Error(),
			map[string]int{"required": insufficient.Required, "available": insufficient.Available})
	case errors.As(err, &replication):
		httpx.WriteErrorDetails(w, http.StatusServiceUnavailable, "replication_policy_not_satisfied", err.Error(), replication)
	case errors.As(err, &noVerified):
		httpx.WriteErrorDetails(w, http.StatusServiceUnavailable, "no_verified_replica", err.Error(), noVerified)
	case errors.Is(err, integrity.ErrTooLarge), errors.As(err, &maxBytes):
		httpx.WriteErrorDetails(w, http.StatusRequestEntityTooLarge, "object_too_large", "object exceeds the maximum size",
			map[string]int64{"max_bytes": s.coord.Config().MaxObjectSize})
	case errors.Is(err, coordinator.ErrUnknownNode):
		httpx.WriteError(w, http.StatusNotFound, "unknown_node", err.Error())
	case errors.Is(err, coordinator.ErrNotAReplica):
		httpx.WriteError(w, http.StatusConflict, "not_a_replica", err.Error())
	case errors.Is(err, storage.ErrChaosDisabled):
		httpx.WriteError(w, http.StatusForbidden, "chaos_disabled_on_node", err.Error())
	case errors.Is(err, storage.ErrUnavailable):
		httpx.WriteError(w, http.StatusBadGateway, "node_unavailable", err.Error())
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		httpx.WriteError(w, http.StatusServiceUnavailable, "request_cancelled", err.Error())
	default:
		s.log.Error(r.Context(), "request failed", "method", r.Method, "path", r.URL.Path, "error", err.Error())
		httpx.WriteError(w, http.StatusInternalServerError, "internal_error", err.Error())
	}
}
