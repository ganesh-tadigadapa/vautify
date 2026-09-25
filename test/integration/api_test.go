package integration

import (
	"bytes"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"testing"

	"vault/internal/storage"
)

func TestAPIContract(t *testing.T) {
	e := startCluster(t, 3, 3, "--max-object-size", "1024")
	data := []byte("contract")
	obj := e.put("c.txt", data)

	r := e.do(http.MethodHead, "/objects/c.txt", nil)
	e.mustStatus(r, http.StatusOK, "HEAD")
	if len(r.Body) != 0 ||
		r.Header.Get("Content-Length") != strconv.Itoa(len(data)) ||
		r.Header.Get(storage.HeaderSHA256) != obj.SHA256 ||
		r.Header.Get("ETag") != `"`+obj.SHA256+`"` ||
		r.Header.Get(storage.HeaderReplicationFactor) != "3" {
		t.Fatalf("HEAD headers: %v", r.Header)
	}

	e.mustStatus(e.do(http.MethodGet, "/objects/missing", nil), http.StatusNotFound, "GET missing")
	e.mustStatus(e.do(http.MethodGet, "/objects/missing/metadata", nil), http.StatusNotFound, "metadata missing")
	e.mustStatus(e.do(http.MethodHead, "/objects/missing", nil), http.StatusNotFound, "HEAD missing")

	// Unsafe IDs never reach a node's filesystem.
	for _, id := range []string{".hidden", "..%2F..%2Fetc%2Fpasswd", "a%2Fb", "-x", "a%00b"} {
		r := e.do(http.MethodPut, "/objects/"+id, []byte("x"))
		e.mustStatus(r, http.StatusBadRequest, "PUT "+id)
		if !bytes.Contains(r.Body, []byte("invalid_object_id")) {
			t.Fatalf("PUT %s body: %s", id, r.Body)
		}
	}

	e.mustStatus(e.do(http.MethodPut, "/objects/big", make([]byte, 1025)), http.StatusRequestEntityTooLarge, "oversized PUT")
	e.mustStatus(e.do(http.MethodPut, "/objects/ok", make([]byte, 1024)), http.StatusCreated, "PUT at limit")
	e.mustStatus(e.do(http.MethodPut, "/objects/x?replication_factor=abc", []byte("x")), http.StatusBadRequest, "bad rf")
	e.mustStatus(e.do(http.MethodPut, "/objects/x?replication_factor=0", []byte("x")), http.StatusCreated, "rf=0 means default")
	e.mustStatus(e.do(http.MethodGet, "/objects/c.txt?prefer=node-9", nil), http.StatusBadRequest, "bad preference")

	// Empty objects are valid objects.
	e.put("empty", nil)
	r = e.do(http.MethodGet, "/objects/empty", nil)
	e.mustStatus(r, http.StatusOK, "GET empty")
	if len(r.Body) != 0 {
		t.Fatal("empty object returned bytes")
	}

	e.mustStatus(e.do(http.MethodGet, "/health", nil), http.StatusOK, "health")
	r = e.do(http.MethodGet, "/nodes", nil)
	e.mustStatus(r, http.StatusOK, "nodes")
	if bytes.Count(r.Body, []byte(`"HEALTHY"`)) != 3 {
		t.Fatalf("/nodes: %s", r.Body)
	}
}

func TestConcurrentClients(t *testing.T) {
	e := startCluster(t, 3, 3)
	shared := randomBytes(512<<10, 6)
	sharedObj := e.put("shared.bin", shared)

	const writers, readers = 32, 96
	var wg sync.WaitGroup
	errs := make(chan error, writers+readers)
	payloads := make([][]byte, writers)
	for i := range payloads {
		payloads[i] = randomBytes(64<<10+i, int64(100+i))
	}

	// Concurrent writes to different objects, interleaved with many reads of
	// an existing object.
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			req, _ := http.NewRequest(http.MethodPut, fmt.Sprintf("%s/objects/w-%d", e.c.API.URL, i), bytes.NewReader(payloads[i]))
			resp, err := e.hc.Do(req)
			if err != nil {
				errs <- err
				return
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusCreated {
				errs <- fmt.Errorf("write %d: HTTP %d", i, resp.StatusCode)
			}
		}(i)
	}
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := e.hc.Get(e.c.API.URL + "/objects/shared.bin")
			if err != nil {
				errs <- err
				return
			}
			defer resp.Body.Close()
			var buf bytes.Buffer
			buf.ReadFrom(resp.Body)
			if resp.StatusCode != http.StatusOK || sha(buf.Bytes()) != sharedObj.SHA256 {
				errs <- fmt.Errorf("read: HTTP %d, sha mismatch=%v", resp.StatusCode, sha(buf.Bytes()) != sharedObj.SHA256)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	// Every concurrently written object reads back intact, concurrently.
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r := e.do(http.MethodGet, fmt.Sprintf("/objects/w-%d", i), nil)
			if r.Status != http.StatusOK || !bytes.Equal(r.Body, payloads[i]) {
				t.Errorf("w-%d: HTTP %d, match=%v", i, r.Status, bytes.Equal(r.Body, payloads[i]))
			}
		}(i)
	}
	wg.Wait()
}
