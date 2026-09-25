package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDashboardServedAtRoot(t *testing.T) {
	h := (&Server{}).Handler()

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET / = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Fatalf("Content-Type = %q, want text/html", ct)
	}
	if !strings.Contains(rec.Body.String(), "<title>Vault Dashboard</title>") {
		t.Fatal("GET / did not return the dashboard page")
	}

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodHead, "/", nil))
	if rec.Code != http.StatusOK || rec.Body.Len() != 0 {
		t.Fatalf("HEAD / = %d with %d body bytes, want 200 and empty", rec.Code, rec.Body.Len())
	}

	// Only the exact root serves the page; unknown paths still 404.
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/no-such-page", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("GET /no-such-page = %d, want 404", rec.Code)
	}
}
