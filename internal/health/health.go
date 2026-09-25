// Package health tracks the reachability of storage nodes.
//
// State changes come from two sources:
//   - active probes: GET /health on every node at a fixed interval;
//   - passive reports: the coordinator calls ReportFailure when a real
//     request to a node fails at the transport level, so a crashed node is
//     marked UNAVAILABLE immediately rather than at the next probe.
//
// Recovery is only ever decided by a successful active probe.
//
// Note that HEALTHY means "the process answers"; it says nothing about
// whether the node's data is intact. Integrity is checked separately, on
// every read.
package health

import (
	"context"
	"sync"
	"time"

	"vault/internal/events"
	"vault/internal/storage"
)

// Prober is the subset of a storage node the checker needs.
type Prober interface {
	ID() string
	Address() string
	Health(ctx context.Context) (storage.NodeHealth, error)
}

// NodeStatus is the checker's current view of one node.
type NodeStatus struct {
	ID          string            `json:"id"`
	Address     string            `json:"address"`
	State       storage.NodeState `json:"state"`
	StateSince  time.Time         `json:"state_since"`
	LastChecked time.Time         `json:"last_checked,omitempty"`
	LastError   string            `json:"last_error,omitempty"`
	Capacity    *storage.Capacity `json:"capacity,omitempty"`
}

// Checker maintains NodeStatus for a fixed set of nodes.
type Checker struct {
	nodes    []Prober
	interval time.Duration
	timeout  time.Duration
	log      *events.Logger

	mu        sync.RWMutex
	status    map[string]*NodeStatus
	listeners []Listener
}

// Listener is told about every node state transition. It is called
// synchronously from the goroutine that observed the change, so it must not
// block.
type Listener func(nodeID string, state storage.NodeState)

// Subscribe registers fn for state transitions.
func (c *Checker) Subscribe(fn Listener) {
	c.mu.Lock()
	c.listeners = append(c.listeners, fn)
	c.mu.Unlock()
}

func (c *Checker) notify(id string, state storage.NodeState) {
	c.mu.RLock()
	ls := append([]Listener(nil), c.listeners...)
	c.mu.RUnlock()
	for _, fn := range ls {
		fn(id, state)
	}
}

// New returns a Checker. Nodes start UNAVAILABLE until the first probe.
func New(nodes []Prober, interval, timeout time.Duration, log *events.Logger) *Checker {
	c := &Checker{nodes: nodes, interval: interval, timeout: timeout, log: log, status: make(map[string]*NodeStatus)}
	now := time.Now().UTC()
	for _, n := range nodes {
		c.status[n.ID()] = &NodeStatus{
			ID: n.ID(), Address: n.Address(), State: storage.StateUnavailable,
			StateSince: now, LastError: "not yet probed",
		}
	}
	return c
}

// Run probes all nodes immediately and then every interval until ctx ends.
func (c *Checker) Run(ctx context.Context) {
	c.ProbeAll(ctx)
	t := time.NewTicker(c.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.ProbeAll(ctx)
		}
	}
}

// ProbeAll probes every node concurrently and waits for all probes.
func (c *Checker) ProbeAll(ctx context.Context) {
	var wg sync.WaitGroup
	for _, n := range c.nodes {
		wg.Add(1)
		go func(n Prober) {
			defer wg.Done()
			pctx, cancel := context.WithTimeout(ctx, c.timeout)
			defer cancel()
			h, err := n.Health(pctx)
			if ctx.Err() != nil {
				return // shutting down; don't record a spurious failure
			}
			if err != nil {
				c.markUnavailable(ctx, n.ID(), "probe", err)
				return
			}
			c.markHealthy(ctx, n.ID(), h.Capacity)
		}(n)
	}
	wg.Wait()
}

// ReportFailure records that a real request to node id failed at the
// transport level.
func (c *Checker) ReportFailure(ctx context.Context, id string, err error) {
	c.markUnavailable(ctx, id, "request", err)
}

func (c *Checker) markUnavailable(ctx context.Context, id, source string, err error) {
	c.mu.Lock()
	st, ok := c.status[id]
	if !ok {
		c.mu.Unlock()
		return
	}
	now := time.Now().UTC()
	wasHealthy := st.State == storage.StateHealthy
	firstProbe := st.LastChecked.IsZero()
	st.LastChecked = now
	st.LastError = err.Error()
	if wasHealthy {
		st.State = storage.StateUnavailable
		st.StateSince = now
	}
	c.mu.Unlock()

	if wasHealthy || firstProbe {
		c.log.Warn(ctx, events.NodeUnavailable, "node_id", id, "detected_by", source, "error", err.Error())
		c.notify(id, storage.StateUnavailable)
	}
}

func (c *Checker) markHealthy(ctx context.Context, id string, capacity storage.Capacity) {
	c.mu.Lock()
	st := c.status[id]
	now := time.Now().UTC()
	recovered := st.State != storage.StateHealthy && !st.LastChecked.IsZero()
	changed := st.State != storage.StateHealthy
	if changed {
		st.State = storage.StateHealthy
		st.StateSince = now
	}
	st.LastChecked = now
	st.LastError = ""
	st.Capacity = &capacity
	c.mu.Unlock()

	if recovered {
		c.log.Emit(ctx, events.NodeRecovered, "node_id", id)
	}
	if changed {
		c.notify(id, storage.StateHealthy)
	}
}

// Healthy returns the IDs of nodes currently HEALTHY, in configuration order.
func (c *Checker) Healthy() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var out []string
	for _, n := range c.nodes {
		if c.status[n.ID()].State == storage.StateHealthy {
			out = append(out, n.ID())
		}
	}
	return out
}

// State returns the current state of node id (UNAVAILABLE if unknown).
func (c *Checker) State(id string) storage.NodeState {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if st, ok := c.status[id]; ok {
		return st.State
	}
	return storage.StateUnavailable
}

// Snapshot returns a copy of every node's status, in configuration order.
func (c *Checker) Snapshot() []NodeStatus {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]NodeStatus, 0, len(c.nodes))
	for _, n := range c.nodes {
		st := *c.status[n.ID()]
		if st.Capacity != nil {
			cp := *st.Capacity
			st.Capacity = &cp
		}
		out = append(out, st)
	}
	return out
}
