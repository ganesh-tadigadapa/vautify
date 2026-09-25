// Package integration runs Vault as real, separate OS processes (one API
// process and N storage-node processes on localhost) and exercises it over
// real HTTP. Nothing here runs in-process except the test client.
package integration

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"vault/internal/cluster"
	"vault/internal/health"
	"vault/internal/metadata"
	"vault/internal/node"
	"vault/internal/nodeclient"
	"vault/internal/storage"
)

var vaultBin string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "vault-it-bin-*")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	vaultBin = filepath.Join(dir, "vault")
	args := []string{"build", "-o", vaultBin}
	if raceEnabled {
		args = append(args, "-race") // child processes get the race detector too
	}
	build := exec.Command("go", append(args, "vault/cmd/vault")...)
	build.Stdout, build.Stderr = os.Stderr, os.Stderr
	if err := build.Run(); err != nil {
		fmt.Fprintln(os.Stderr, "building vault binary:", err)
		os.Exit(1)
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

type env struct {
	t  *testing.T
	c  *cluster.Cluster
	hc *http.Client
}

// startCluster launches a cluster in Phase 1 configuration: no automatic
// repair, no background scrub, no chaos endpoints. Phase 1 tests use it so
// they keep pinning exactly the Phase 1 semantics.
func startCluster(t *testing.T, nodes, rf int, extraAPIArgs ...string) *env {
	t.Helper()
	return launch(t, cluster.Spec{
		Nodes: nodes, ReplicationFactor: rf, ProbeInterval: 200 * time.Millisecond, ExtraAPIArgs: extraAPIArgs,
	})
}

func launch(t *testing.T, spec cluster.Spec) *env {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	spec.Binary = vaultBin
	if spec.Dir == "" {
		spec.Dir = t.TempDir()
	}
	c, err := cluster.Start(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	e := &env{t: t, c: c, hc: &http.Client{Timeout: 60 * time.Second}}
	t.Cleanup(func() {
		c.Stop()
		// Child processes built with -race report races on stderr, which
		// goes to their log files; surface them as test failures.
		for _, p := range append([]*cluster.Process{c.API}, c.Nodes...) {
			if b, _ := os.ReadFile(p.LogPath); bytes.Contains(b, []byte("WARNING: DATA RACE")) {
				t.Errorf("data race detected in %s process", p.Name)
			}
		}
		if t.Failed() {
			e.dumpLogs()
		}
	})
	return e
}

func (e *env) dumpLogs() {
	for _, p := range append([]*cluster.Process{e.c.API}, e.c.Nodes...) {
		b, _ := os.ReadFile(p.LogPath)
		e.t.Logf("---- %s log ----\n%s", p.Name, b)
	}
}

type response struct {
	Status int
	Header http.Header
	Body   []byte
}

func (e *env) do(method, path string, body []byte) response {
	e.t.Helper()
	var r io.Reader
	if body != nil {
		r = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, e.c.API.URL+path, r)
	if err != nil {
		e.t.Fatal(err)
	}
	resp, err := e.hc.Do(req)
	if err != nil {
		e.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		e.t.Fatal(err)
	}
	return response{Status: resp.StatusCode, Header: resp.Header, Body: b}
}

func (e *env) mustStatus(r response, want int, what string) {
	e.t.Helper()
	if r.Status != want {
		e.t.Fatalf("%s: HTTP %d, want %d: %s", what, r.Status, want, r.Body)
	}
}

func (e *env) put(id string, data []byte) metadata.Object {
	e.t.Helper()
	r := e.do(http.MethodPut, "/objects/"+id, data)
	if r.Status != http.StatusCreated && r.Status != http.StatusOK {
		e.t.Fatalf("PUT %s: HTTP %d: %s", id, r.Status, r.Body)
	}
	var pr struct {
		Object      metadata.Object `json:"object"`
		Replication struct {
			Required, Achieved int
			Status             string
		} `json:"replication"`
	}
	if err := json.Unmarshal(r.Body, &pr); err != nil {
		e.t.Fatal(err)
	}
	if pr.Replication.Achieved != pr.Replication.Required || pr.Replication.Status != "satisfied" {
		e.t.Fatalf("PUT %s succeeded without satisfying policy: %s", id, r.Body)
	}
	return pr.Object
}

func (e *env) metadata(id string) metadata.Object {
	e.t.Helper()
	r := e.do(http.MethodGet, "/objects/"+id+"/metadata", nil)
	e.mustStatus(r, http.StatusOK, "metadata "+id)
	var obj metadata.Object
	if err := json.Unmarshal(r.Body, &obj); err != nil {
		e.t.Fatal(err)
	}
	return obj
}

func (e *env) nodeState(id string) storage.NodeState {
	e.t.Helper()
	r := e.do(http.MethodGet, "/nodes", nil)
	var body struct{ Nodes []health.NodeStatus }
	json.Unmarshal(r.Body, &body)
	for _, n := range body.Nodes {
		if n.ID == id {
			return n.State
		}
	}
	e.t.Fatalf("node %s not in /nodes", id)
	return ""
}

func (e *env) waitNodeState(id string, want storage.NodeState) {
	e.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if e.nodeState(id) == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	e.t.Fatalf("node %s never reached %s", id, want)
}

func (e *env) replicaPath(nodeID, objectID string, version uint64) string {
	return node.ReplicaDataPath(e.c.Node(nodeID).DataDir, objectID, version)
}

func (e *env) nodeClient(id string) *nodeclient.Client {
	nc, err := nodeclient.New(id, e.c.Node(id).URL, e.hc)
	if err != nil {
		e.t.Fatal(err)
	}
	return nc
}

// logEvents returns every parsed log line of process p with the given event.
func (e *env) logEvents(p *cluster.Process, event string) []map[string]any {
	e.t.Helper()
	f, err := os.Open(p.LogPath)
	if err != nil {
		e.t.Fatal(err)
	}
	defer f.Close()
	var out []map[string]any
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		var m map[string]any
		if json.Unmarshal(sc.Bytes(), &m) == nil && m["event"] == event {
			out = append(out, m)
		}
	}
	return out
}

func randomBytes(n int, seed int64) []byte {
	b := make([]byte, n)
	rand.New(rand.NewSource(seed)).Read(b)
	return b
}

func sha(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

func rejected(h http.Header) map[string]string {
	out := map[string]string{}
	for _, part := range strings.Split(h.Get(storage.HeaderRejectedReplicas), ",") {
		if id, outcome, ok := strings.Cut(part, "="); ok {
			out[id] = outcome
		}
	}
	return out
}
