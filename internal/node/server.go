package node

import (
	"errors"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"

	"vault/internal/events"
	"vault/internal/httpx"
	"vault/internal/integrity"
	"vault/internal/objectid"
	"vault/internal/storage"
)

// Server exposes a Store over HTTP.
type Server struct {
	store         *Store
	log           *events.Logger
	maxObjectSize int64
	startedAt     time.Time

	// terminate is non-nil only when chaos endpoints are enabled.
	terminate func()
}

// NewServer returns a Server for store. Replica uploads larger than
// maxObjectSize bytes are refused.
func NewServer(store *Store, log *events.Logger, maxObjectSize int64) *Server {
	return &Server{store: store, log: log, maxObjectSize: maxObjectSize, startedAt: time.Now().UTC()}
}

// EnableChaos turns on the /admin chaos endpoints. terminate is called
// (in its own goroutine, after the response is flushed) to make the node
// process stop; for a real node process it exits the process.
func (s *Server) EnableChaos(terminate func()) { s.terminate = terminate }

// Handler returns the node's HTTP routes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("GET /replicas", s.handleInventory)
	mux.HandleFunc("GET /replicas/{id}", s.handleList)
	mux.HandleFunc("PUT /replicas/{id}/{version}", s.handlePut)
	mux.HandleFunc("GET /replicas/{id}/{version}", s.handleGet)
	mux.HandleFunc("DELETE /replicas/{id}/{version}", s.handleDelete)
	mux.HandleFunc("DELETE /replicas/{id}", s.handleDeleteAll)
	mux.HandleFunc("GET /replicas/{id}/{version}/verify", s.handleVerify)
	mux.HandleFunc("POST /admin/terminate", s.handleTerminate)
	mux.HandleFunc("POST /admin/corrupt/{id}/{version}", s.handleCorrupt)
	return mux
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	capacity, err := s.store.Capacity()
	if err != nil {
		httpx.WriteError(w, http.StatusInternalServerError, "capacity_unavailable", err.Error())
		return
	}
	httpx.WriteJSON(w, http.StatusOK, storage.NodeHealth{
		NodeID:    s.store.NodeID(),
		Status:    storage.StateHealthy,
		StartedAt: s.startedAt,
		Capacity:  capacity,
	})
}

// replicaKey parses and validates {id}/{version}, writing a 400 on failure.
func replicaKey(w http.ResponseWriter, r *http.Request) (string, uint64, bool) {
	id := r.PathValue("id")
	if err := objectid.Validate(id); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, "invalid_object_id", err.Error())
		return "", 0, false
	}
	v, err := ParseVersion(r.PathValue("version"))
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, "invalid_version", err.Error())
		return "", 0, false
	}
	return id, v, true
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	id, version, ok := replicaKey(w, r)
	if !ok {
		return
	}
	sha := r.Header.Get(storage.HeaderSHA256)
	if !integrity.ValidSHA256Hex(sha) {
		httpx.WriteError(w, http.StatusBadRequest, "missing_checksum", "header "+storage.HeaderSHA256+" must carry a lowercase hex SHA-256")
		return
	}
	if r.ContentLength < 0 {
		httpx.WriteError(w, http.StatusLengthRequired, "length_required", "Content-Length is required")
		return
	}
	if r.ContentLength > s.maxObjectSize {
		httpx.WriteError(w, http.StatusRequestEntityTooLarge, "too_large", "replica exceeds node size limit")
		return
	}
	body := http.MaxBytesReader(w, r.Body, s.maxObjectSize+1)
	want := integrity.Digest{SHA256: sha, Size: r.ContentLength}

	info, err := s.store.Put(id, version, want, body)
	if err != nil {
		if errors.Is(err, storage.ErrStaleVersion) {
			httpx.WriteError(w, http.StatusConflict, "stale_version", err.Error())
			return
		}
		var mm *integrity.MismatchError
		if errors.As(err, &mm) {
			s.log.Warn(r.Context(), events.ChecksumMismatch, "stage", "replica_write",
				"object_id", id, "version", version,
				"expected_sha256", mm.Expected.SHA256, "actual_sha256", mm.Actual.SHA256,
				"expected_size", mm.Expected.Size, "actual_size", mm.Actual.Size)
			httpx.WriteError(w, http.StatusUnprocessableEntity, "checksum_mismatch", err.Error())
			return
		}
		s.log.Error(r.Context(), "replica write failed", "object_id", id, "version", version, "error", err.Error())
		httpx.WriteError(w, http.StatusInternalServerError, "write_failed", err.Error())
		return
	}
	s.log.Emit(r.Context(), events.ChecksumVerified, "stage", "replica_write",
		"object_id", id, "version", version, "sha256", info.SHA256, "size", info.Size)
	httpx.WriteJSON(w, http.StatusCreated, info)
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	id, version, ok := replicaKey(w, r)
	if !ok {
		return
	}
	f, info, err := s.store.Open(id, version)
	if errors.Is(err, storage.ErrNotFound) {
		httpx.WriteError(w, http.StatusNotFound, "not_found", "replica not stored on this node")
		return
	}
	if err != nil {
		httpx.WriteError(w, http.StatusInternalServerError, "read_failed", err.Error())
		return
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		httpx.WriteError(w, http.StatusInternalServerError, "read_failed", err.Error())
		return
	}
	h := w.Header()
	h.Set("Content-Type", "application/octet-stream")
	h.Set("Content-Length", strconv.FormatInt(fi.Size(), 10))
	h.Set(storage.HeaderNodeID, s.store.NodeID())
	h.Set(storage.HeaderVersion, strconv.FormatUint(version, 10))
	// The checksum recorded at write time. It describes what SHOULD be on
	// disk, not a claim that the bytes below still match it.
	h.Set(storage.HeaderSHA256, info.SHA256)
	w.WriteHeader(http.StatusOK)
	if _, err := io.Copy(w, f); err != nil {
		s.log.Error(r.Context(), "replica stream interrupted", "object_id", id, "version", version, "error", err.Error())
	}
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	id, version, ok := replicaKey(w, r)
	if !ok {
		return
	}
	err := s.store.Delete(id, version)
	if errors.Is(err, storage.ErrNotFound) {
		httpx.WriteError(w, http.StatusNotFound, "not_found", "replica not stored on this node")
		return
	}
	if err != nil {
		httpx.WriteError(w, http.StatusInternalServerError, "delete_failed", err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleDeleteAll(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := objectid.Validate(id); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, "invalid_object_id", err.Error())
		return
	}
	n, err := s.store.DeleteAll(id)
	if errors.Is(err, storage.ErrNotFound) {
		httpx.WriteError(w, http.StatusNotFound, "not_found", "no replicas of this object on this node")
		return
	}
	if err != nil {
		httpx.WriteError(w, http.StatusInternalServerError, "delete_failed", err.Error())
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"node_id": s.store.NodeID(), "object_id": id, "replicas_removed": n})
}

func (s *Server) handleVerify(w http.ResponseWriter, r *http.Request) {
	id, version, ok := replicaKey(w, r)
	if !ok {
		return
	}
	res, err := s.store.Verify(id, version)
	if errors.Is(err, storage.ErrNotFound) {
		httpx.WriteError(w, http.StatusNotFound, "not_found", "replica not stored on this node")
		return
	}
	if err != nil {
		httpx.WriteError(w, http.StatusInternalServerError, "verify_failed", err.Error())
		return
	}
	if !res.Valid {
		s.log.Warn(r.Context(), events.ChecksumMismatch, "stage", "replica_verify",
			"object_id", id, "version", version,
			"expected_sha256", res.Recorded.SHA256, "actual_sha256", res.Actual.SHA256)
	}
	httpx.WriteJSON(w, http.StatusOK, res)
}

func (s *Server) chaosAllowed(w http.ResponseWriter) bool {
	if s.terminate == nil {
		httpx.WriteError(w, http.StatusForbidden, "chaos_disabled", "node was started without --enable-chaos")
		return false
	}
	return true
}

func (s *Server) handleTerminate(w http.ResponseWriter, r *http.Request) {
	if !s.chaosAllowed(w) {
		return
	}
	s.log.Warn(r.Context(), events.ChaosNodeTerminating, "node_id", s.store.NodeID(), "pid", os.Getpid())
	httpx.WriteJSON(w, http.StatusAccepted, map[string]any{
		"node_id": s.store.NodeID(), "action": "terminate", "pid": os.Getpid(),
	})
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
	go s.terminate()
}

func (s *Server) handleCorrupt(w http.ResponseWriter, r *http.Request) {
	if !s.chaosAllowed(w) {
		return
	}
	id, version, ok := replicaKey(w, r)
	if !ok {
		return
	}
	res, err := s.store.Corrupt(id, version)
	if errors.Is(err, storage.ErrNotFound) {
		httpx.WriteError(w, http.StatusNotFound, "not_found", "replica not stored on this node")
		return
	}
	if err != nil {
		httpx.WriteError(w, http.StatusInternalServerError, "corrupt_failed", err.Error())
		return
	}
	s.log.Warn(r.Context(), events.ChaosReplicaCorrupted, "node_id", s.store.NodeID(), "object_id", id,
		"version", version, "offset", res.Offset, "length", res.Length,
		"sha256_before", res.Before.SHA256, "sha256_after", res.After.SHA256)
	httpx.WriteJSON(w, http.StatusOK, res)
}

func (s *Server) handleInventory(w http.ResponseWriter, r *http.Request) {
	reps, err := s.store.Inventory()
	if err != nil {
		httpx.WriteError(w, http.StatusInternalServerError, "inventory_failed", err.Error())
		return
	}
	if reps == nil {
		reps = []storage.ReplicaInfo{}
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"node_id": s.store.NodeID(), "replicas": reps})
}

func (s *Server) handleList(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := objectid.Validate(id); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, "invalid_object_id", err.Error())
		return
	}
	reps, err := s.store.List(id)
	if err != nil {
		httpx.WriteError(w, http.StatusInternalServerError, "list_failed", err.Error())
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"node_id": s.store.NodeID(), "object_id": id, "replicas": reps})
}
