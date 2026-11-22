package api

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
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

func TestDashboardCompressionAndRevalidation(t *testing.T) {
	h := (&Server{}).Handler()
	get := func(hdr map[string]string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		for k, v := range hdr {
			req.Header.Set(k, v)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}

	plain := get(nil)
	if plain.Header().Get("Content-Encoding") != "" || !bytes.Equal(plain.Body.Bytes(), dashboardHTML) {
		t.Fatal("without Accept-Encoding the page must be sent uncompressed")
	}
	etag := plain.Header().Get("ETag")
	if etag == "" || plain.Header().Get("Vary") != "Accept-Encoding" {
		t.Fatalf("ETag %q, Vary %q", etag, plain.Header().Get("Vary"))
	}

	gz := get(map[string]string{"Accept-Encoding": "br, gzip;q=0.8"})
	if gz.Header().Get("Content-Encoding") != "gzip" {
		t.Fatal("gzip was accepted but not used")
	}
	zr, err := gzip.NewReader(bytes.NewReader(gz.Body.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	unzipped, err := io.ReadAll(zr)
	if err != nil || !bytes.Equal(unzipped, dashboardHTML) {
		t.Fatalf("gzip body does not decode to the page: %v", err)
	}
	if n := gz.Body.Len(); n >= len(dashboardHTML)/3 {
		t.Fatalf("gzip body is %d bytes for a %d-byte page", n, len(dashboardHTML))
	}
	if got, _ := strconv.Atoi(gz.Header().Get("Content-Length")); got != gz.Body.Len() {
		t.Fatalf("Content-Length %d, body %d", got, gz.Body.Len())
	}

	if refused := get(map[string]string{"Accept-Encoding": "gzip;q=0"}); refused.Header().Get("Content-Encoding") != "" {
		t.Fatal("gzip;q=0 must not be answered with gzip")
	}

	for _, inm := range []string{etag, strings.TrimPrefix(etag, "W/"), `"other", ` + etag, "*"} {
		if rec := get(map[string]string{"If-None-Match": inm}); rec.Code != http.StatusNotModified || rec.Body.Len() != 0 {
			t.Fatalf("If-None-Match %s = %d with %d bytes, want 304 and empty", inm, rec.Code, rec.Body.Len())
		}
	}
	if rec := get(map[string]string{"If-None-Match": `"stale"`}); rec.Code != http.StatusOK {
		t.Fatalf("stale If-None-Match = %d, want 200", rec.Code)
	}
}
