// Package selfheal connects detection to repair. The Controller:
//
//   - reacts to node state changes: computes the failed node's blast radius
//     and re-assesses every object;
//   - reacts to recorded integrity failures (from reads and scrubs);
//   - sweeps all objects periodically as a safety net;
//   - runs the background scrubber on an interval;
//   - enqueues every DEGRADED/CRITICAL object into the repair Manager.
//
// Every number it reports is computed from metadata and live node state.
package selfheal

import (
	"context"
	"sync"
	"time"

	"vault/internal/coordinator"
	"vault/internal/durability"
	"vault/internal/events"
	"vault/internal/health"
	"vault/internal/metadata"
	"vault/internal/repair"
	"vault/internal/storage"
)

// Config tunes the controller.
type Config struct {
	// AutoRepair enqueues objects below policy. When false the controller
	// still assesses and reports, but never repairs.
	AutoRepair bool
	// ScrubInterval between background scrubs; 0 disables periodic scrub.
	ScrubInterval time.Duration
	// ScrubConcurrency bounds simultaneous replica verifications.
	ScrubConcurrency int
	// SweepInterval between full durability sweeps (a safety net; sweeps
	// are also triggered by events).
	SweepInterval time.Duration
	// ReconcileOnRecovery reconciles a node against metadata as soon as it
	// is reachable again after this process saw it UNAVAILABLE (a healed
	// partition, a restarted node). Off, only scrub's stale-version purge
	// and the manual reconcile endpoints clean up after it.
	ReconcileOnRecovery bool
}

// HealthView is what the controller needs from the health checker.
type HealthView interface {
	State(id string) storage.NodeState
	Snapshot() []health.NodeStatus
}

// Controller is safe for concurrent use.
type Controller struct {
	cfg     Config
	coord   *coordinator.Coordinator
	meta    metadata.Store
	health  HealthView
	repairs *repair.Manager
	log     *events.Logger

	kick chan struct{}

	mu           sync.Mutex
	lastState    map[string]durability.State
	belowSince   map[string]time.Time
	pendingNodes map[string]storage.NodeState
	impacts      map[string]durability.BlastRadius
	lastScrub    *coordinator.ScrubReport
	lastSweep    *SweepReport
	scrubRunning bool
	scrubMu      sync.Mutex // serialises scrub passes
	sweepMu      sync.Mutex // serialises sweeps
	// everDown: nodes this process has seen UNAVAILABLE. When one of them
	// is HEALTHY again (a partition healed, a node restarted) it may hold
	// stale, deleted or uncommitted bytes, so it is reconciled.
	everDown map[string]bool
}

// New returns a Controller. Wire it with:
//
//	checker.Subscribe(ctrl.NodeStateChanged)
//	coord.SetIntegrityFailureHook(ctrl.IntegrityFailureDetected)
//	mgr.OnDone(ctrl.RepairFinished)
func New(cfg Config, coord *coordinator.Coordinator, meta metadata.Store, hv HealthView, mgr *repair.Manager, log *events.Logger) *Controller {
	if cfg.ScrubConcurrency < 1 {
		cfg.ScrubConcurrency = 4
	}
	if cfg.SweepInterval <= 0 {
		cfg.SweepInterval = 5 * time.Second
	}
	return &Controller{
		cfg: cfg, coord: coord, meta: meta, health: hv, repairs: mgr, log: log,
		kick:         make(chan struct{}, 1),
		lastState:    map[string]durability.State{},
		belowSince:   map[string]time.Time{},
		pendingNodes: map[string]storage.NodeState{},
		impacts:      map[string]durability.BlastRadius{},
		everDown:     map[string]bool{},
	}
}

// Config returns the controller configuration.
func (c *Controller) Config() Config { return c.cfg }

// Repairs returns the repair manager.
func (c *Controller) Repairs() *repair.Manager { return c.repairs }

func (c *Controller) trigger() {
	select {
	case c.kick <- struct{}{}:
	default:
	}
}

// NodeStateChanged is a health.Listener. It never blocks.
func (c *Controller) NodeStateChanged(nodeID string, state storage.NodeState) {
	c.mu.Lock()
	c.pendingNodes[nodeID] = state
	if state == storage.StateUnavailable {
		c.everDown[nodeID] = true
	}
	c.mu.Unlock()
	c.trigger()
}

// IntegrityFailureDetected is the coordinator's integrity hook.
func (c *Controller) IntegrityFailureDetected(string) { c.trigger() }

// RepairFinished is the repair manager's completion hook.
func (c *Controller) RepairFinished(repair.Result) { c.trigger() }

// Run drives event-triggered and periodic sweeps and periodic scrubs until
// ctx is cancelled.
func (c *Controller) Run(ctx context.Context) {
	if c.cfg.ScrubInterval > 0 {
		go func() {
			t := time.NewTicker(c.cfg.ScrubInterval)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					if _, err := c.Scrub(ctx); err != nil && ctx.Err() == nil {
						c.log.Error(ctx, "background scrub failed", "error", err.Error())
					}
				}
			}
		}()
	}
	t := time.NewTicker(c.cfg.SweepInterval)
	defer t.Stop()
	c.sweepLogged(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.kick:
		case <-t.C:
		}
		c.handleNodeChanges(ctx)
		c.sweepLogged(ctx)
	}
}

func (c *Controller) sweepLogged(ctx context.Context) {
	if _, err := c.Sweep(ctx); err != nil && ctx.Err() == nil {
		c.log.Error(ctx, "durability sweep failed", "error", err.Error())
	}
}

func (c *Controller) handleNodeChanges(ctx context.Context) {
	c.mu.Lock()
	pending := c.pendingNodes
	c.pendingNodes = map[string]storage.NodeState{}
	c.mu.Unlock()
	if len(pending) == 0 {
		return
	}
	// Reconcile returning nodes first, so that repairs released below see
	// (and write into) an already reconciled node.
	for nodeID, state := range pending {
		if state == storage.StateHealthy {
			c.mu.Lock()
			returning := c.everDown[nodeID]
			c.mu.Unlock()
			if returning && c.cfg.ReconcileOnRecovery {
				// Connectivity is back: reconcile the node against metadata
				// before the sweep that follows queues repairs.
				if _, err := c.coord.ReconcileNode(ctx, nodeID, "node_recovered"); err != nil && ctx.Err() == nil {
					c.log.Error(ctx, "reconcile failed", "node_id", nodeID, "error", err.Error())
				}
			}
		}
	}
	// Capacity or sources may have appeared or vanished: let waiting and
	// parked repairs try again against the new topology.
	c.repairs.TopologyChanged()
	for nodeID, state := range pending {
		if state != storage.StateUnavailable {
			continue
		}
		br, err := c.BlastRadius(ctx, nodeID)
		if err != nil {
			c.log.Error(ctx, "blast radius failed", "node_id", nodeID, "error", err.Error())
			continue
		}
		c.mu.Lock()
		c.impacts[nodeID] = br
		c.mu.Unlock()
		c.log.Warn(ctx, events.BlastRadiusCalculated, "node_id", nodeID,
			"affected_objects", br.AffectedObjects, "affected_bytes", br.AffectedBytes,
			"protected", br.Protected, "degraded", br.Degraded, "critical", br.Critical, "lost", br.Lost)
	}
}

// BlastRadius computes the impact of losing nodeID given current state.
func (c *Controller) BlastRadius(ctx context.Context, nodeID string) (durability.BlastRadius, error) {
	if !c.knownNode(nodeID) {
		return durability.BlastRadius{}, coordinator.ErrUnknownNode
	}
	objs, err := c.meta.ListObjects(ctx)
	if err != nil {
		return durability.BlastRadius{}, err
	}
	return durability.ComputeBlastRadius(objs, nodeID, c.health.State, time.Now().UTC()), nil
}

func (c *Controller) knownNode(id string) bool {
	for _, n := range c.health.Snapshot() {
		if n.ID == id {
			return true
		}
	}
	return false
}

// SweepReport summarises one durability sweep.
type SweepReport struct {
	At       time.Time         `json:"at"`
	Counts   durability.Counts `json:"counts"`
	Enqueued map[string]int    `json:"enqueue_outcomes"`
}

// Sweep assesses every object, emits state-transition events, and (with
// AutoRepair) enqueues objects below policy.
func (c *Controller) Sweep(ctx context.Context) (SweepReport, error) {
	c.sweepMu.Lock()
	defer c.sweepMu.Unlock()
	assessedAt := time.Now()
	objs, err := c.meta.ListObjects(ctx)
	if err != nil {
		return SweepReport{}, err
	}
	now := time.Now().UTC()
	rep := SweepReport{At: now, Enqueued: map[string]int{}}
	seen := make(map[string]bool, len(objs))
	for _, obj := range objs {
		h := durability.Assess(obj, c.health.State)
		seen[obj.ID] = true
		rep.Counts.Add(h.State)

		c.mu.Lock()
		prev, known := c.lastState[obj.ID]
		c.lastState[obj.ID] = h.State
		since, below := c.belowSince[obj.ID]
		if h.State == durability.Healthy {
			delete(c.belowSince, obj.ID)
		} else if !below {
			since = now
			c.belowSince[obj.ID] = now
		}
		c.mu.Unlock()

		if prev != h.State && (known || h.State != durability.Healthy) {
			c.emitTransition(ctx, h, prev, since, now)
		}
		if c.cfg.AutoRepair && h.NeedsRepair() {
			rep.Enqueued[c.repairs.EnqueueAssessed(ctx, h, since, assessedAt)]++
		}
	}
	c.mu.Lock()
	for id := range c.lastState {
		if !seen[id] {
			delete(c.lastState, id)
			delete(c.belowSince, id)
		}
	}
	c.lastSweep = &rep
	c.mu.Unlock()
	return rep, nil
}

func (c *Controller) emitTransition(ctx context.Context, h durability.ObjectHealth, prev durability.State, since, now time.Time) {
	attrs := []any{"object_id", h.ObjectID, "version", h.Version, "state", string(h.State),
		"previous_state", string(prev), "healthy_replicas", h.HealthyReplicas,
		"replication_factor", h.ReplicationFactor, "reason", h.Reason}
	switch h.State {
	case durability.Degraded:
		c.log.Warn(ctx, events.ObjectDegraded, attrs...)
	case durability.Critical:
		c.log.Warn(ctx, events.ObjectCritical, attrs...)
	case durability.Lost:
		c.log.Warn(ctx, events.ObjectLost, attrs...)
	case durability.Healthy:
		c.log.Emit(ctx, events.ObjectRestored, append(attrs, "below_policy_ms", now.Sub(since).Milliseconds())...)
	}
}

// Scrub runs one full scrub pass, then a sweep so findings are enqueued.
// Passes never overlap; a request during a pass waits for it and then runs.
func (c *Controller) Scrub(ctx context.Context) (coordinator.ScrubReport, error) {
	c.scrubMu.Lock()
	defer c.scrubMu.Unlock()
	c.mu.Lock()
	c.scrubRunning = true
	c.mu.Unlock()
	rep, err := c.coord.Scrub(ctx, c.cfg.ScrubConcurrency)
	c.mu.Lock()
	c.scrubRunning = false
	c.lastScrub = &rep
	c.mu.Unlock()
	if err != nil {
		return rep, err
	}
	_, err = c.Sweep(ctx)
	return rep, err
}

// NodeView is a node's health plus how many replicas metadata places on it.
type NodeView struct {
	health.NodeStatus
	Replicas int `json:"tracked_replicas"`
}

// ClusterStatus is the whole-cluster view for operators and UIs.
type ClusterStatus struct {
	GeneratedAt    time.Time                 `json:"generated_at"`
	NodesTotal     int                       `json:"nodes_total"`
	NodesHealthy   int                       `json:"nodes_healthy"`
	Nodes          []NodeView                `json:"nodes"`
	Objects        durability.Counts         `json:"objects"`
	AtRisk         []durability.ObjectHealth `json:"at_risk"`
	Repair         RepairSummary             `json:"repair"`
	Scrub          ScrubSummary              `json:"scrub"`
	FailureImpacts []durability.BlastRadius  `json:"failure_impacts"`
	// LastReconcile is the latest reconciliation of each node (counts only;
	// the full action list is in the reconcile endpoint's response).
	LastReconcile []coordinator.ReconcileReport `json:"last_reconcile"`
}

// RepairSummary is the repair subsystem at a glance.
type RepairSummary struct {
	AutoRepair bool `json:"auto_repair"`
	Paused     bool `json:"paused"`
	Workers    int  `json:"workers"`
	QueueDepth int  `json:"queue_depth"`
	Active     int  `json:"active"`
	Parked     int  `json:"parked_failed"`
}

// ScrubSummary describes the scrubber.
type ScrubSummary struct {
	IntervalMS  int64                    `json:"interval_ms"`
	Concurrency int                      `json:"concurrency"`
	Running     bool                     `json:"running"`
	LastRun     *coordinator.ScrubReport `json:"last_run,omitempty"`
}

// Status assembles the live cluster view.
func (c *Controller) Status(ctx context.Context) (ClusterStatus, error) {
	objs, err := c.meta.ListObjects(ctx)
	if err != nil {
		return ClusterStatus{}, err
	}
	st := ClusterStatus{GeneratedAt: time.Now().UTC(), AtRisk: []durability.ObjectHealth{}, FailureImpacts: []durability.BlastRadius{}}
	perNode := map[string]int{}
	for _, obj := range objs {
		h := durability.Assess(obj, c.health.State)
		st.Objects.Add(h.State)
		if h.State != durability.Healthy {
			st.AtRisk = append(st.AtRisk, h)
		}
		for _, n := range obj.Replicas {
			perNode[n]++
		}
	}
	durability.SortBySeverity(st.AtRisk)
	for _, n := range c.health.Snapshot() {
		st.NodesTotal++
		if n.State == storage.StateHealthy {
			st.NodesHealthy++
		}
		st.Nodes = append(st.Nodes, NodeView{NodeStatus: n, Replicas: perNode[n.ID]})
	}
	snap := c.repairs.Snapshot()
	st.Repair = RepairSummary{AutoRepair: c.cfg.AutoRepair, Paused: snap.Paused, Workers: c.repairs.Config().Workers,
		QueueDepth: snap.Depth, Active: len(snap.Active), Parked: len(snap.Failed)}

	c.mu.Lock()
	st.Scrub = ScrubSummary{IntervalMS: c.cfg.ScrubInterval.Milliseconds(), Concurrency: c.cfg.ScrubConcurrency,
		Running: c.scrubRunning, LastRun: c.lastScrub}
	for _, br := range c.impacts {
		br.Objects = nil // counts only; full list via the blast-radius endpoint
		st.FailureImpacts = append(st.FailureImpacts, br)
	}
	c.mu.Unlock()
	st.LastReconcile = c.coord.LastReconciles()
	for i := range st.LastReconcile {
		st.LastReconcile[i].Actions = nil
	}
	return st, nil
}

// Reconcile reconciles one node now and then sweeps, so anything it
// recorded or left below policy is queued for repair.
func (c *Controller) Reconcile(ctx context.Context, nodeID string) (coordinator.ReconcileReport, error) {
	if !c.knownNode(nodeID) {
		return coordinator.ReconcileReport{}, coordinator.ErrUnknownNode
	}
	rep, err := c.coord.ReconcileNode(ctx, nodeID, "manual")
	if err != nil {
		return rep, err
	}
	_, err = c.Sweep(ctx)
	return rep, err
}

// ReconcileAll reconciles every reachable node, then sweeps.
func (c *Controller) ReconcileAll(ctx context.Context) ([]coordinator.ReconcileReport, error) {
	reps := c.coord.ReconcileAll(ctx, "manual")
	_, err := c.Sweep(ctx)
	return reps, err
}

// ObjectHealthView is one object's durability plus its repair status.
type ObjectHealthView struct {
	durability.ObjectHealth
	Repair     *repair.Task         `json:"repair,omitempty"`
	LastRepair *repair.HistoryEntry `json:"last_repair,omitempty"`
}

// ObjectHealth assesses one object now.
func (c *Controller) ObjectHealth(ctx context.Context, id string) (ObjectHealthView, error) {
	obj, err := c.meta.GetObject(ctx, id)
	if err != nil {
		return ObjectHealthView{}, err
	}
	v := ObjectHealthView{ObjectHealth: durability.Assess(obj, c.health.State)}
	if t, ok := c.repairs.Lookup(id); ok {
		v.Repair = &t
	}
	hist := c.repairs.History(0)
	for i := len(hist) - 1; i >= 0; i-- {
		if hist[i].ObjectID == id {
			e := hist[i]
			v.LastRepair = &e
			break
		}
	}
	return v, nil
}

// RequestRepair assesses one object and enqueues it if below policy,
// returning the assessment and the enqueue outcome.
func (c *Controller) RequestRepair(ctx context.Context, id string) (durability.ObjectHealth, string, error) {
	obj, err := c.meta.GetObject(ctx, id)
	if err != nil {
		return durability.ObjectHealth{}, "", err
	}
	h := durability.Assess(obj, c.health.State)
	c.mu.Lock()
	since, ok := c.belowSince[id]
	if !ok && h.State != durability.Healthy {
		since = time.Now().UTC()
		c.belowSince[id] = since
	}
	c.mu.Unlock()
	return h, c.repairs.Enqueue(ctx, h, since), nil
}
