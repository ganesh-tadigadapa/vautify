package api

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"net/http"
	"strconv"
	"strings"
)

// dashboardHTML is the browser dashboard served at "/". It is one static
// page that drives the same JSON endpoints as curl and the tests; it has no
// server-side logic of its own.
//
//go:embed ui/index.html
var dashboardHTML []byte

// The page never changes while the process runs, so it is compressed and
// fingerprinted once. The ETag is weak because the gzip and identity bodies
// are the same resource in two encodings.
var (
	dashboardGzip = gzipBytes(dashboardHTML)
	dashboardETag = func() string {
		sum := sha256.Sum256(dashboardHTML)
		return `W/"` + hex.EncodeToString(sum[:8]) + `"`
	}()
)

func (s *Server) handleDashboard(w http.ResponseWriter, r *http.Request) {
	h := w.Header()
	h.Set("Content-Type", "text/html; charset=utf-8")
	// no-cache means "revalidate before use": with the ETag, a reload costs
	// a 304 instead of the whole page.
	h.Set("Cache-Control", "no-cache")
	h.Set("ETag", dashboardETag)
	h.Set("Vary", "Accept-Encoding")
	h.Set("X-Content-Type-Options", "nosniff")
	if etagMatches(r.Header.Get("If-None-Match"), dashboardETag) {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	body := dashboardHTML
	if acceptsGzip(r.Header.Get("Accept-Encoding")) {
		h.Set("Content-Encoding", "gzip")
		body = dashboardGzip
	}
	h.Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(http.StatusOK)
	if r.Method != http.MethodHead {
		_, _ = w.Write(body)
	}
}

func gzipBytes(b []byte) []byte {
	var buf bytes.Buffer
	zw, _ := gzip.NewWriterLevel(&buf, gzip.BestCompression)
	_, _ = zw.Write(b)
	_ = zw.Close()
	return buf.Bytes()
}

// acceptsGzip reports whether an Accept-Encoding header allows gzip,
// honouring an explicit q=0 refusal.
func acceptsGzip(header string) bool {
	for _, part := range strings.Split(header, ",") {
		name, params, _ := strings.Cut(strings.TrimSpace(part), ";")
		if name = strings.ToLower(strings.TrimSpace(name)); name != "gzip" && name != "*" {
			continue
		}
		q, ok := strings.CutPrefix(strings.ReplaceAll(params, " ", ""), "q=")
		if !ok {
			return true
		}
		if v, err := strconv.ParseFloat(q, 64); err != nil || v > 0 {
			return true
		}
	}
	return false
}

// etagMatches implements If-None-Match's weak comparison.
func etagMatches(header, etag string) bool {
	want := strings.TrimPrefix(etag, "W/")
	for _, tag := range strings.Split(header, ",") {
		tag = strings.TrimSpace(tag)
		if tag == "*" || strings.TrimPrefix(tag, "W/") == want {
			return true
		}
	}
	return false
}
