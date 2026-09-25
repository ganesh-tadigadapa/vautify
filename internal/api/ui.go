package api

import (
	_ "embed"
	"net/http"
)

// dashboardHTML is the browser dashboard served at "/". It is one static
// page that drives the same JSON endpoints as curl and the tests; it has no
// server-side logic of its own.
//
//go:embed ui/index.html
var dashboardHTML []byte

func (s *Server) handleDashboard(w http.ResponseWriter, r *http.Request) {
	h := w.Header()
	h.Set("Content-Type", "text/html; charset=utf-8")
	h.Set("Cache-Control", "no-cache")
	h.Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	if r.Method != http.MethodHead {
		_, _ = w.Write(dashboardHTML)
	}
}
