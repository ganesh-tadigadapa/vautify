// Package httpx holds small HTTP helpers shared by the node and API servers:
// JSON responses, the error envelope, and a server runner with timeouts and
// graceful shutdown.
package httpx

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"sync"
	"time"

	"vault/internal/storage"
)

// WriteJSON writes v as an indented JSON response with the given status.
func WriteJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v) // the status line is already sent; nothing useful to do on error
}

// WriteError writes the standard error envelope.
func WriteError(w http.ResponseWriter, status int, code, msg string) {
	WriteErrorDetails(w, status, code, msg, nil)
}

// WriteErrorDetails writes the standard error envelope with structured details.
func WriteErrorDetails(w http.ResponseWriter, status int, code, msg string, details any) {
	WriteJSON(w, status, storage.ErrorBody{Error: storage.ErrorDetail{Code: code, Message: msg, Details: details}})
}

// ServerConfig bounds how long a connection may spend in each phase.
// Body read/write timeouts are generous because objects are streamed.
type ServerConfig struct {
	ReadHeaderTimeout time.Duration
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
	ShutdownTimeout   time.Duration
	MaxHeaderBytes    int
}

// DefaultServerConfig returns conservative defaults.
func DefaultServerConfig() ServerConfig {
	return ServerConfig{
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Minute,
		WriteTimeout:      10 * time.Minute,
		IdleTimeout:       60 * time.Second,
		ShutdownTimeout:   10 * time.Second,
		MaxHeaderBytes:    64 << 10,
	}
}

// Serve listens on addr and serves h until ctx is cancelled, then shuts down
// gracefully, letting in-flight requests finish within ShutdownTimeout.
// ready, if non-nil, is called with the bound address once listening.
func Serve(ctx context.Context, addr string, h http.Handler, cfg ServerConfig, ready func(net.Addr)) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	srv := &http.Server{
		Handler:           h,
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
		ReadTimeout:       cfg.ReadTimeout,
		WriteTimeout:      cfg.WriteTimeout,
		IdleTimeout:       cfg.IdleTimeout,
		MaxHeaderBytes:    cfg.MaxHeaderBytes,
	}
	// Track connections that have been accepted but have not yet carried a
	// request. http.Server.Shutdown waits up to 5s for these; they hold no
	// in-flight work, so they are closed immediately on shutdown instead.
	var (
		connMu   sync.Mutex
		newConns = make(map[net.Conn]struct{})
	)
	srv.ConnState = func(c net.Conn, st http.ConnState) {
		connMu.Lock()
		defer connMu.Unlock()
		if st == http.StateNew {
			newConns[c] = struct{}{}
		} else {
			delete(newConns, c)
		}
	}
	if ready != nil {
		ready(ln.Addr())
	}

	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(ln) }()

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	shutdownErr := make(chan error, 1)
	go func() { shutdownErr <- srv.Shutdown(shutdownCtx) }()
	connMu.Lock()
	for c := range newConns {
		c.Close()
	}
	connMu.Unlock()
	if err := <-shutdownErr; err != nil {
		return err
	}
	if err := <-errc; err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
