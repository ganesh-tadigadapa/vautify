// Package demo runs Vault's end-to-end integrity demonstration against a
// real local cluster: separate node processes, real HTTP, real files. Every
// line it prints is derived from an actual API response, node response, file
// on disk, or log line.
package demo

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
	"strings"
	"time"

	"vault/internal/api"
	"vault/internal/cluster"
	"vault/internal/health"
	"vault/internal/metadata"
	"vault/internal/node"
	"vault/internal/nodeclient"
	"vault/internal/storage"
)

// Options configures a demo run.
type Options struct {
	Binary string // vault executable
	Dir    string // working directory for the cluster
	Size   int    // payload size in bytes
	Color  bool
}

type printer struct {
	w     io.Writer
	color bool
}

func (p printer) c(code, s string) string {
	if !p.color {
		return s
	}
	return "\x1b[" + code + "m" + s + "\x1b[0m"
}
func (p printer) ok() string                   { return p.c("32", "✓") }
func (p printer) bad() string                  { return p.c("31", "✗") }
func (p printer) dim(s string) string          { return p.c("2", s) }
func (p printer) bold(s string) string         { return p.c("1", s) }
func (p printer) f(format string, a ...any)    { fmt.Fprintf(p.w, format, a...) }
func (p printer) section(title string)         { p.f("\n%s\n", p.bold(title)) }
func (p printer) line(format string, a ...any) { p.f("  "+format+"\n", a...) }

// Run executes the demo and returns an error if any step does not behave as
// Vault claims it should.
func Run(ctx context.Context, out io.Writer, opts Options) error {
	p := printer{w: out, color: opts.Color}
	if opts.Size == 0 {
		opts.Size = 1 << 20
	}
	const objectID = "dataset.bin"

	p.f("%s\n", p.bold("VAULT DEMO — silent corruption vs. verified reads"))
	p.f("%s\n", p.dim("real processes · real HTTP · real files · nothing simulated"))

	// 1. Cluster ------------------------------------------------------------
	p.section("Starting cluster")
	cl, err := cluster.Start(ctx, cluster.Spec{Binary: opts.Binary, Dir: opts.Dir, Nodes: 3, ReplicationFactor: 3})
	if err != nil {
		return err
	}
	defer cl.Stop()
	p.line("%-7s %-22s pid %d", "api", cl.API.URL, cl.API.PID())
	for _, n := range cl.Nodes {
		p.line("%-7s %-22s pid %d  %s", n.Name, n.URL, n.PID(), p.dim(n.DataDir))
	}

	hc := &http.Client{Timeout: 30 * time.Second}
	nodeClients := map[string]*nodeclient.Client{}
	for _, n := range cl.Nodes {
		nc, err := nodeclient.New(n.Name, n.URL, hc)
		if err != nil {
			return err
		}
		nodeClients[n.Name] = nc
	}

	// 2. Upload -------------------------------------------------------------
	payload := make([]byte, opts.Size)
	rand.New(rand.NewSource(42)).Read(payload)
	sum := sha256.Sum256(payload)
	wantSHA := hex.EncodeToString(sum[:])

	p.section(fmt.Sprintf("Uploading object: %s (%d bytes, sha256 %s…)", objectID, len(payload), wantSHA[:16]))
	var put api.PutResponse
	if err := doJSON(ctx, hc, http.MethodPut, cl.API.URL+"/objects/"+objectID, bytes.NewReader(payload), http.StatusCreated, &put); err != nil {
		return fmt.Errorf("upload: %w", err)
	}
	obj := put.Object

	p.section("Replica placement (verified on each node's own disk)")
	for _, nodeID := range obj.Replicas {
		path := node.ReplicaDataPath(cl.Node(nodeID).DataDir, objectID, obj.Version)
		fi, err := os.Stat(path)
		if err != nil {
			return fmt.Errorf("replica file missing on %s: %w", nodeID, err)
		}
		vr, err := nodeClients[nodeID].VerifyReplica(ctx, objectID, obj.Version)
		if err != nil || !vr.Valid {
			return fmt.Errorf("%s did not verify its replica: %+v %v", nodeID, vr, err)
		}
		p.line("%s %s  %d bytes on disk, node re-hash matches  %s", nodeID, p.ok(), fi.Size(), p.dim(shortPath(path, opts.Dir)))
	}
	rep := put.Replication
	mark := p.ok()
	if rep.Achieved != rep.Required {
		mark = p.bad()
	}
	p.section("Replication")
	p.line("%d / %d %s  (policy %s)", rep.Achieved, rep.Required, mark, rep.Status)

	// 3. Corrupt ------------------------------------------------------------
	// Corrupt the replica a normal read would try first, so an ordinary GET
	// with no hints has to detect and route around it.
	victim := obj.Replicas[0]
	victimPath := node.ReplicaDataPath(cl.Node(victim).DataDir, objectID, obj.Version)
	if err := flipBytes(victimPath, int64(len(payload)/2), 16); err != nil {
		return err
	}
	p.section("Injecting silent corruption (16 bytes flipped on disk, size unchanged)")
	p.line("%s → corrupted  %s", victim, p.dim("(first replica in read order)"))

	nh, err := nodeClients[victim].Health(ctx)
	if err != nil {
		return fmt.Errorf("%s health: %w", victim, err)
	}
	apiState, err := apiNodeState(ctx, hc, cl.API.URL, victim)
	if err != nil {
		return err
	}
	p.line("%s process → %s (pid %d)  node /health: %s  api view: %s",
		victim, p.c("33", "ALIVE"), cl.Node(victim).PID(), nh.Status, apiState)
	if nh.Status != storage.StateHealthy || apiState != storage.StateHealthy {
		return fmt.Errorf("expected corrupted node to still look healthy")
	}

	// 4. Read ---------------------------------------------------------------
	p.section("Read (plain GET, no hints)")
	body, hdr, err := get(ctx, hc, cl.API.URL+"/objects/"+objectID)
	if err != nil {
		return err
	}
	servedBy := hdr.Get(storage.HeaderServedBy)
	rejected := parseRejected(hdr.Get(storage.HeaderRejectedReplicas))
	for _, nodeID := range obj.Replicas {
		switch {
		case rejected[nodeID] != "":
			p.line("%s → %s %s", nodeID, strings.ReplaceAll(rejected[nodeID], "_", " "), p.bad())
		case nodeID == servedBy:
			p.line("%s → checksum verified %s", nodeID, p.ok())
		default:
			p.line("%s → %s", nodeID, p.dim("not needed"))
		}
	}
	gotSum := sha256.Sum256(body)
	gotSHA := hex.EncodeToString(gotSum[:])
	if rejected[victim] != "checksum_mismatch" {
		return fmt.Errorf("corrupted replica on %s was not rejected (rejected=%v)", victim, rejected)
	}
	if servedBy == victim || gotSHA != wantSHA {
		return fmt.Errorf("read returned unverified data (served by %s, sha %s)", servedBy, gotSHA)
	}

	p.section("Result")
	p.line("object retrieved successfully %s  (served by %s)", p.ok(), servedBy)
	p.line("integrity verified %s  client-side sha256 %s… == original", p.ok(), gotSHA[:16])

	// 5. Evidence -----------------------------------------------------------
	p.section("Evidence recorded")
	var meta metadata.Object
	if err := doJSON(ctx, hc, http.MethodGet, cl.API.URL+"/objects/"+objectID+"/metadata", nil, http.StatusOK, &meta); err != nil {
		return err
	}
	for _, f := range meta.IntegrityFailures {
		p.line("metadata integrity_failures: node=%s reason=%s observed=%s… expected=%s…",
			f.NodeID, f.Reason, f.ObservedSHA256[:12], f.ExpectedSHA256[:12])
	}
	if line, err := findEvent(cl.API.LogPath, "CHECKSUM_MISMATCH", victim); err == nil {
		p.line("api log: %s", p.dim(line))
	} else {
		return fmt.Errorf("no CHECKSUM_MISMATCH event in api log: %w", err)
	}

	// 6. Node failure -------------------------------------------------------
	p.section(fmt.Sprintf("Node failure: stopping %s (the replica that just served the read)", servedBy))
	if err := cl.KillNode(servedBy); err != nil {
		return err
	}
	p.line("%s process → %s", servedBy, p.c("31", "STOPPED (SIGKILL)"))
	body, hdr, err = get(ctx, hc, cl.API.URL+"/objects/"+objectID)
	if err != nil {
		return fmt.Errorf("read after node failure: %w", err)
	}
	gotSum = sha256.Sum256(body)
	rejected = parseRejected(hdr.Get(storage.HeaderRejectedReplicas))
	remaining := hdr.Get(storage.HeaderServedBy)
	for _, nodeID := range obj.Replicas {
		switch {
		case rejected[nodeID] != "":
			p.line("%s → %s %s", nodeID, strings.ReplaceAll(rejected[nodeID], "_", " "), p.bad())
		case nodeID == remaining:
			p.line("%s → checksum verified %s", nodeID, p.ok())
		}
	}
	if hex.EncodeToString(gotSum[:]) != wantSHA {
		return fmt.Errorf("read after node failure returned wrong bytes")
	}
	p.line("object still retrievable and verified %s  (1 of 3 replicas usable)", p.ok())

	// 7. Honesty -------------------------------------------------------------
	p.section("Self-healing is disabled in this scenario")
	p.line("%s the corrupted replica on %s is left as-is; the object has 1 usable replica", p.dim("•"), victim)
	p.line("%s both facts are recorded (metadata + events) — see `vault demo self-heal`", p.dim("•"))
	p.line("%s and `vault demo corruption-repair` for automatic detection and repair", p.dim("•"))
	p.f("\n")
	return nil
}

func doJSON(ctx context.Context, hc *http.Client, method, url string, body io.Reader, wantStatus int, v any) error {
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return err
	}
	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != wantStatus {
		return fmt.Errorf("%s %s: HTTP %d: %s", method, url, resp.StatusCode, strings.TrimSpace(string(b)))
	}
	return json.Unmarshal(b, v)
}

func get(ctx context.Context, hc *http.Client, url string) ([]byte, http.Header, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, nil, err
	}
	resp, err := hc.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, nil, fmt.Errorf("GET %s: HTTP %d: %s", url, resp.StatusCode, strings.TrimSpace(string(b)))
	}
	return b, resp.Header, nil
}

func apiNodeState(ctx context.Context, hc *http.Client, apiURL, nodeID string) (storage.NodeState, error) {
	var resp struct {
		Nodes []health.NodeStatus `json:"nodes"`
	}
	if err := doJSON(ctx, hc, http.MethodGet, apiURL+"/nodes", nil, http.StatusOK, &resp); err != nil {
		return "", err
	}
	for _, n := range resp.Nodes {
		if n.ID == nodeID {
			return n.State, nil
		}
	}
	return "", fmt.Errorf("api does not know node %s", nodeID)
}

// parseRejected turns "node-2=checksum_mismatch,node-3=node_unavailable" into a map.
func parseRejected(h string) map[string]string {
	out := map[string]string{}
	for _, part := range strings.Split(h, ",") {
		if id, outcome, ok := strings.Cut(strings.TrimSpace(part), "="); ok {
			out[id] = outcome
		}
	}
	return out
}

// flipBytes inverts n bytes at offset in place, leaving the file size
// unchanged — the kind of damage only a checksum can reveal.
func flipBytes(path string, offset int64, n int) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	buf := make([]byte, n)
	if _, err := f.ReadAt(buf, offset); err != nil {
		return err
	}
	for i := range buf {
		buf[i] ^= 0xFF
	}
	if _, err := f.WriteAt(buf, offset); err != nil {
		return err
	}
	return f.Sync()
}

// findEvent returns a compact rendering of the first log line carrying the
// given event for the given node.
func findEvent(logPath, event, nodeID string) (string, error) {
	f, err := os.Open(logPath)
	if err != nil {
		return "", err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		var m map[string]any
		if json.Unmarshal(sc.Bytes(), &m) != nil {
			continue
		}
		if m["event"] != event || m["node_id"] != nodeID {
			continue
		}
		keys := []string{"event", "node_id", "object_id", "reason", "node_alive"}
		var parts []string
		for _, k := range keys {
			if v, ok := m[k]; ok {
				parts = append(parts, fmt.Sprintf("%s=%v", k, v))
			}
		}
		return strings.Join(parts, " "), nil
	}
	if err := sc.Err(); err != nil {
		return "", err
	}
	return "", fmt.Errorf("event %s for %s not found", event, nodeID)
}

func shortPath(path, base string) string {
	if rel, ok := strings.CutPrefix(path, base); ok {
		return "…" + rel
	}
	return path
}
