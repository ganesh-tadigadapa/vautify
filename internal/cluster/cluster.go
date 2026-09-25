// Package cluster launches a local Vault cluster as real OS processes: one
// `vault api` process and N `vault node` processes, each with its own port
// and data directory. It is used by `vault cluster`, `vault demo` and the
// integration tests.
package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Spec describes a local cluster.
type Spec struct {
	// Binary is the path to the vault executable.
	Binary string
	// Dir is the base directory; each process gets a subdirectory.
	Dir string
	// Nodes is the number of storage nodes (named node-1 … node-N).
	Nodes int
	// ReplicationFactor is the API's default replication factor.
	ReplicationFactor int
	// Host to bind every process to. Defaults to 127.0.0.1.
	Host string
	// APIPort and NodeBasePort fix ports (node-i uses NodeBasePort+i-1).
	// Zero picks free ports.
	APIPort      int
	NodeBasePort int
	// ProbeInterval is the API's node health probe interval.
	ProbeInterval time.Duration
	// LogTee, if set, also receives every process's log output.
	LogTee io.Writer
	// ExtraAPIArgs are appended to the `vault api` command line.
	ExtraAPIArgs []string

	// Self-healing. The zero value is Phase 1 behaviour: no automatic
	// repair, no background scrub, no chaos endpoints.
	AutoRepair    bool
	EnableChaos   bool
	RepairWorkers int           // 0: api default
	ScrubInterval time.Duration // 0: no periodic scrub
	SweepInterval time.Duration // 0: api default
}

// Process is one running (or stopped) cluster member.
type Process struct {
	Name    string
	Addr    string
	URL     string
	DataDir string
	LogPath string

	args []string
	// base is the original command line; restarts may append overrides
	// (later flags win) without accumulating them.
	base []string

	mu   sync.Mutex
	cmd  *exec.Cmd
	done chan struct{}
	err  error
}

// PID returns the OS process ID, or 0 if not running.
func (p *Process) PID() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cmd == nil || p.cmd.Process == nil || p.exited() {
		return 0
	}
	return p.cmd.Process.Pid
}

// Running reports whether the process is alive.
func (p *Process) Running() bool { return p.PID() != 0 }

func (p *Process) exited() bool {
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}

// Cluster is a set of running processes.
type Cluster struct {
	spec  Spec
	API   *Process
	Nodes []*Process
}

// Start launches every node, then the API, and waits until the API reports
// all nodes healthy.
func Start(ctx context.Context, spec Spec) (*Cluster, error) {
	if spec.Host == "" {
		spec.Host = "127.0.0.1"
	}
	if spec.Nodes < 1 {
		return nil, errors.New("cluster: need at least one node")
	}
	if spec.ReplicationFactor == 0 {
		spec.ReplicationFactor = min(3, spec.Nodes)
	}
	if spec.ProbeInterval == 0 {
		spec.ProbeInterval = 500 * time.Millisecond
	}
	logDir := filepath.Join(spec.Dir, "logs")
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		return nil, err
	}

	c := &Cluster{spec: spec}
	var nodeFlags []string
	for i := 1; i <= spec.Nodes; i++ {
		name := fmt.Sprintf("node-%d", i)
		port := 0
		if spec.NodeBasePort != 0 {
			port = spec.NodeBasePort + i - 1
		}
		addr, err := hostPort(spec.Host, port)
		if err != nil {
			return nil, err
		}
		dataDir := filepath.Join(spec.Dir, name)
		p := &Process{
			Name: name, Addr: addr, URL: "http://" + addr, DataDir: dataDir,
			LogPath: filepath.Join(logDir, name+".log"),
			args:    []string{"node", "--id", name, "--listen", addr, "--data", dataDir},
		}
		if spec.EnableChaos {
			p.args = append(p.args, "--enable-chaos")
		}
		c.Nodes = append(c.Nodes, p)
		nodeFlags = append(nodeFlags, name+"="+p.URL)
	}

	apiAddr, err := hostPort(spec.Host, spec.APIPort)
	if err != nil {
		return nil, err
	}
	apiDir := filepath.Join(spec.Dir, "api")
	c.API = &Process{
		Name: "api", Addr: apiAddr, URL: "http://" + apiAddr, DataDir: apiDir,
		LogPath: filepath.Join(logDir, "api.log"),
		args: []string{"api", "--listen", apiAddr,
			"--nodes", strings.Join(nodeFlags, ","),
			"--data", apiDir,
			"--replication-factor", strconv.Itoa(spec.ReplicationFactor),
			"--probe-interval", spec.ProbeInterval.String()},
	}
	c.API.args = append(c.API.args,
		"--auto-repair="+strconv.FormatBool(spec.AutoRepair),
		"--scrub-interval="+spec.ScrubInterval.String())
	if spec.EnableChaos {
		c.API.args = append(c.API.args, "--enable-chaos")
	}
	if spec.RepairWorkers > 0 {
		c.API.args = append(c.API.args, "--repair-workers="+strconv.Itoa(spec.RepairWorkers))
	}
	if spec.SweepInterval > 0 {
		c.API.args = append(c.API.args, "--sweep-interval="+spec.SweepInterval.String())
	}
	c.API.args = append(c.API.args, spec.ExtraAPIArgs...)
	c.API.base = slices.Clone(c.API.args)

	for _, n := range c.Nodes {
		if err := c.start(ctx, n); err != nil {
			c.Stop()
			return nil, err
		}
	}
	if err := c.start(ctx, c.API); err != nil {
		c.Stop()
		return nil, err
	}
	if err := c.WaitHealthyNodes(ctx, spec.Nodes, 15*time.Second); err != nil {
		c.Stop()
		return nil, err
	}
	return c, nil
}

func hostPort(host string, port int) (string, error) {
	if port == 0 {
		ln, err := net.Listen("tcp", net.JoinHostPort(host, "0"))
		if err != nil {
			return "", fmt.Errorf("cluster: find free port: %w", err)
		}
		port = ln.Addr().(*net.TCPAddr).Port
		ln.Close()
	}
	return net.JoinHostPort(host, strconv.Itoa(port)), nil
}

func (c *Cluster) start(ctx context.Context, p *Process) error {
	p.mu.Lock()
	if p.cmd != nil && !p.exited() {
		p.mu.Unlock()
		return fmt.Errorf("cluster: %s already running", p.Name)
	}
	logFile, err := os.OpenFile(p.LogPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		p.mu.Unlock()
		return err
	}
	var out io.Writer = logFile
	if c.spec.LogTee != nil {
		out = io.MultiWriter(logFile, c.spec.LogTee)
	}
	cmd := exec.Command(c.spec.Binary, p.args...)
	cmd.Stdout, cmd.Stderr = out, out
	if err := cmd.Start(); err != nil {
		logFile.Close()
		p.mu.Unlock()
		return fmt.Errorf("cluster: start %s: %w", p.Name, err)
	}
	p.cmd, p.done, p.err = cmd, make(chan struct{}), nil
	done := p.done
	go func() {
		err := cmd.Wait()
		logFile.Close()
		p.mu.Lock()
		p.err = err
		p.mu.Unlock()
		close(done)
	}()
	p.mu.Unlock()

	return waitHTTP(ctx, p, 15*time.Second)
}

// waitHTTP polls p's /health until it answers 200.
func waitHTTP(ctx context.Context, p *Process, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: 500 * time.Millisecond}
	for time.Now().Before(deadline) {
		if p.exited() {
			return fmt.Errorf("cluster: %s exited during startup (%v); see %s", p.Name, p.err, p.LogPath)
		}
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, p.URL+"/health", nil)
		if resp, err := client.Do(req); err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
	return fmt.Errorf("cluster: %s not ready after %s; see %s", p.Name, timeout, p.LogPath)
}

// WaitHealthyNodes waits until the API reports exactly want healthy nodes.
func (c *Cluster) WaitHealthyNodes(ctx context.Context, want int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: time.Second}
	last := -1
	for time.Now().Before(deadline) {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, c.API.URL+"/health", nil)
		if resp, err := client.Do(req); err == nil {
			var h struct {
				NodesHealthy int `json:"nodes_healthy"`
			}
			err := json.NewDecoder(resp.Body).Decode(&h)
			resp.Body.Close()
			if err == nil {
				last = h.NodesHealthy
				if h.NodesHealthy == want {
					return nil
				}
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
	return fmt.Errorf("cluster: API reports %d healthy nodes, wanted %d, after %s", last, want, timeout)
}

// Node returns the process for node id, or nil.
func (c *Cluster) Node(id string) *Process {
	for _, n := range c.Nodes {
		if n.Name == id {
			return n
		}
	}
	return nil
}

// StopNode gracefully stops a node (SIGTERM, then SIGKILL after 5s).
func (c *Cluster) StopNode(id string) error {
	p := c.Node(id)
	if p == nil {
		return fmt.Errorf("cluster: no node %q", id)
	}
	return stop(p, syscall.SIGTERM)
}

// KillNode stops a node abruptly with SIGKILL, like a crash.
func (c *Cluster) KillNode(id string) error {
	p := c.Node(id)
	if p == nil {
		return fmt.Errorf("cluster: no node %q", id)
	}
	return stop(p, syscall.SIGKILL)
}

// StartNode (re)starts a stopped node with the same port and data directory.
func (c *Cluster) StartNode(ctx context.Context, id string) error {
	p := c.Node(id)
	if p == nil {
		return fmt.Errorf("cluster: no node %q", id)
	}
	return c.start(ctx, p)
}

// RestartAPI stops (SIGTERM) and restarts the API process, reusing its data
// directory. overrides are appended to the original flags (later flags win).
func (c *Cluster) RestartAPI(ctx context.Context, overrides ...string) error {
	if err := stop(c.API, syscall.SIGTERM); err != nil {
		return err
	}
	return c.StartAPI(ctx, overrides...)
}

// KillAPI stops the API process abruptly with SIGKILL, like a crash.
func (c *Cluster) KillAPI() error { return stop(c.API, syscall.SIGKILL) }

// StartAPI starts a stopped API process with its original flags plus
// overrides.
func (c *Cluster) StartAPI(ctx context.Context, overrides ...string) error {
	c.API.args = append(slices.Clone(c.API.base), overrides...)
	return c.start(ctx, c.API)
}

func stop(p *Process, sig syscall.Signal) error {
	p.mu.Lock()
	cmd, done := p.cmd, p.done
	p.mu.Unlock()
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	select {
	case <-done:
		return nil
	default:
	}
	if err := cmd.Process.Signal(sig); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return err
	}
	select {
	case <-done:
		return nil
	case <-time.After(5 * time.Second):
		cmd.Process.Kill()
		<-done
		return nil
	}
}

// Stop terminates every process: the API first, then the nodes.
func (c *Cluster) Stop() {
	if c.API != nil {
		stop(c.API, syscall.SIGTERM)
	}
	var wg sync.WaitGroup
	for _, n := range c.Nodes {
		wg.Add(1)
		go func(n *Process) {
			defer wg.Done()
			stop(n, syscall.SIGTERM)
		}(n)
	}
	wg.Wait()
}
