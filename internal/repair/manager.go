package repair

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"vault/internal/durability"
	"vault/internal/events"
)

// Enqueue outcomes.
const (
	OutcomeQueued     = "queued"
	OutcomeUpdated    = "updated"
	OutcomeDuplicate  = "duplicate"
	OutcomeActive     = "already_repairing"
	OutcomeParked     = "parked_after_failures"
	OutcomeNotNeeded  = "not_needed"
	OutcomeUnrepaired = "unrepairable"
)

// FailedTask is a task that exhausted its attempts.
type FailedTask struct {
	Task     Task      `json:"task"`
	FailedAt time.Time `json:"failed_at"`
}

// Manager owns the repair queue and worker pool. It is safe for concurrent use.
type Manager struct {
	cfg  Config
	exec Executor
	log  *events.Logger

	mu      sync.Mutex
	queued  map[string]*Task
	active  map[string]*Task
	failed  map[string]FailedTask
	paused  bool
	changed chan struct{} // closed and replaced on every state change
	onDone  []func(Result)

	metrics metrics
	history []HistoryEntry // ring buffer
	histPos int
	histLen int

	// restored remembers when each object version was last found or made
	// to meet policy, so an assessment taken before that moment (a sweep
	// racing the repair's completion) cannot queue a pointless second run.
	restored map[string]restoredMark
}

type restoredMark struct {
	version uint64
	at      time.Time
}

// NewManager returns a Manager. Call Run to start its workers.
func NewManager(exec Executor, cfg Config, log *events.Logger) *Manager {
	cfg.setDefaults()
	return &Manager{
		cfg: cfg, exec: exec, log: log,
		queued: map[string]*Task{}, active: map[string]*Task{}, failed: map[string]FailedTask{},
		changed:  make(chan struct{}),
		history:  make([]HistoryEntry, cfg.HistorySize),
		restored: map[string]restoredMark{},
	}
}

// Config returns the effective configuration.
func (m *Manager) Config() Config { return m.cfg }

// OnDone registers fn to be called after every repair attempt.
func (m *Manager) OnDone(fn func(Result)) {
	m.mu.Lock()
	m.onDone = append(m.onDone, fn)
	m.mu.Unlock()
}

// notifyLocked wakes every waiting worker. Caller holds m.mu.
func (m *Manager) notifyLocked() {
	close(m.changed)
	m.changed = make(chan struct{})
}

// Enqueue requests repair of the assessed object. Requests for an object
// already queued or repairing are merged, never duplicated. detectedAt is
// when the object was first seen below policy; it anchors recovery time.
func (m *Manager) Enqueue(ctx context.Context, h durability.ObjectHealth, detectedAt time.Time) string {
	return m.EnqueueAssessed(ctx, h, detectedAt, time.Now())
}

// EnqueueAssessed is Enqueue for an assessment computed from metadata read
// at assessedAt. If a repair of the same object version completed (restored
// or found already meeting policy) after assessedAt, the assessment is stale
// and is dropped as a duplicate; the next assessment sees the new state.
func (m *Manager) EnqueueAssessed(ctx context.Context, h durability.ObjectHealth, detectedAt, assessedAt time.Time) string {
	if h.State == durability.Lost {
		return OutcomeUnrepaired
	}
	if !h.NeedsRepair() {
		return OutcomeNotNeeded
	}
	now := time.Now().UTC()

	m.mu.Lock()
	if r, ok := m.restored[h.ObjectID]; ok {
		if r.version == h.Version && r.at.After(assessedAt) {
			m.metrics.Deduplicated++
			m.mu.Unlock()
			m.log.Emit(ctx, events.RepairDeduplicated, "object_id", h.ObjectID, "version", h.Version,
				"status", "restored_after_assessment")
			return OutcomeDuplicate
		}
		delete(m.restored, h.ObjectID)
	}
	if _, ok := m.active[h.ObjectID]; ok {
		m.metrics.Deduplicated++
		m.mu.Unlock()
		m.log.Emit(ctx, events.RepairDeduplicated, "object_id", h.ObjectID, "version", h.Version, "status", StatusRepairing)
		return OutcomeActive
	}
	if _, ok := m.failed[h.ObjectID]; ok {
		m.mu.Unlock()
		return OutcomeParked
	}
	if t, ok := m.queued[h.ObjectID]; ok {
		m.metrics.Deduplicated++
		changed := t.Priority != h.State || t.HealthyReplicas != h.HealthyReplicas || t.Version != h.Version
		t.Priority, t.Reason, t.HealthyReplicas, t.Version = h.State, h.Reason, h.HealthyReplicas, h.Version
		t.ReplicationFactor = h.ReplicationFactor
		status := t.Status
		if changed {
			m.notifyLocked()
		}
		m.mu.Unlock()
		if changed {
			m.log.Emit(ctx, events.RepairPriorityCalculated, "object_id", h.ObjectID, "version", h.Version,
				"priority", string(h.State), "reason", h.Reason, "healthy_replicas", h.HealthyReplicas,
				"replication_factor", h.ReplicationFactor, "requeued", true)
			return OutcomeUpdated
		}
		m.log.Emit(ctx, events.RepairDeduplicated, "object_id", h.ObjectID, "version", h.Version, "status", status)
		return OutcomeDuplicate
	}
	if detectedAt.IsZero() {
		detectedAt = now
	}
	t := &Task{
		ObjectID: h.ObjectID, Version: h.Version, Priority: h.State, Reason: h.Reason,
		HealthyReplicas: h.HealthyReplicas, ReplicationFactor: h.ReplicationFactor,
		Status: StatusQueued, DetectedAt: detectedAt, QueuedAt: now,
	}
	m.queued[h.ObjectID] = t
	m.metrics.Queued++
	m.notifyLocked()
	depth := len(m.queued)
	m.mu.Unlock()

	m.log.Emit(ctx, events.RepairPriorityCalculated, "object_id", h.ObjectID, "version", h.Version,
		"priority", string(h.State), "reason", h.Reason, "healthy_replicas", h.HealthyReplicas,
		"replication_factor", h.ReplicationFactor)
	m.log.Emit(ctx, events.RepairQueued, "object_id", h.ObjectID, "version", h.Version,
		"priority", string(h.State), "reason", h.Reason, "queue_depth", depth)
	return OutcomeQueued
}

// Pause stops workers from starting new repairs. Running repairs finish.
func (m *Manager) Pause() {
	m.mu.Lock()
	m.paused = true
	m.notifyLocked()
	m.mu.Unlock()
}

// Resume lets workers start repairs again.
func (m *Manager) Resume() {
	m.mu.Lock()
	m.paused = false
	m.notifyLocked()
	m.mu.Unlock()
}

// Paused reports whether new repairs are held back.
func (m *Manager) Paused() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.paused
}

// TopologyChanged should be called when nodes fail or recover: capacity or
// sources may now exist, so waiting tasks retry now and parked tasks are
// released to be enqueued again by the next assessment.
func (m *Manager) TopologyChanged() {
	m.mu.Lock()
	for _, t := range m.queued {
		t.NextAttemptAt = nil
		if t.Status == StatusRetryWait {
			t.Status = StatusQueued
		}
	}
	m.failed = map[string]FailedTask{}
	m.notifyLocked()
	m.mu.Unlock()
}

// Run starts the worker pool and blocks until ctx is cancelled and every
// worker has finished its current repair.
func (m *Manager) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for i := 0; i < m.cfg.Workers; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for {
				t, ok := m.next(ctx)
				if !ok {
					return
				}
				m.runTask(ctx, worker, t)
			}
		}(i + 1)
	}
	wg.Wait()
}

// next blocks until a task is eligible (not paused, backoff elapsed) and
// claims it, or until ctx is done.
func (m *Manager) next(ctx context.Context) (Task, bool) {
	for {
		m.mu.Lock()
		now := time.Now()
		var best *Task
		var wake time.Duration = -1
		if !m.paused {
			for _, t := range m.queued {
				if t.NextAttemptAt != nil && t.NextAttemptAt.After(now) {
					if d := t.NextAttemptAt.Sub(now); wake < 0 || d < wake {
						wake = d
					}
					continue
				}
				if best == nil || less(t, best) {
					best = t
				}
			}
		}
		if best != nil {
			delete(m.queued, best.ObjectID)
			started := now.UTC()
			best.Status, best.StartedAt, best.NextAttemptAt = StatusRepairing, &started, nil
			best.Attempts++
			m.active[best.ObjectID] = best
			m.metrics.Started++
			t := *best
			m.mu.Unlock()
			return t, true
		}
		ch := m.changed
		m.mu.Unlock()

		var (
			tm    *time.Timer
			timer <-chan time.Time
		)
		if wake >= 0 {
			tm = time.NewTimer(wake)
			timer = tm.C
		}
		select {
		case <-ctx.Done():
		case <-ch:
		case <-timer:
		}
		if tm != nil {
			tm.Stop()
		}
		if ctx.Err() != nil {
			return Task{}, false
		}
	}
}

func (m *Manager) runTask(ctx context.Context, worker int, t Task) {
	m.log.Emit(ctx, events.RepairStarted, "object_id", t.ObjectID, "version", t.Version,
		"priority", string(t.Priority), "reason", t.Reason, "attempt", t.Attempts, "worker", worker,
		"healthy_replicas", t.HealthyReplicas, "replication_factor", t.ReplicationFactor)
	start := time.Now()
	res, err := m.exec.Repair(ctx, t)
	m.finish(ctx, t, res, err, time.Since(start))
}

func (m *Manager) finish(ctx context.Context, t Task, res Result, err error, took time.Duration) {
	now := time.Now().UTC()
	entry := HistoryEntry{
		ObjectID: t.ObjectID, Version: t.Version, Priority: t.Priority, Reason: t.Reason,
		Attempt: t.Attempts, DetectedAt: t.DetectedAt, QueuedAt: t.QueuedAt, StartedAt: *t.StartedAt,
		CompletedAt: now, DurationMS: took.Milliseconds(), Result: res,
	}
	if err != nil {
		entry.Error = err.Error()
	}

	m.mu.Lock()
	delete(m.active, t.ObjectID)
	m.metrics.BytesTransferred += res.BytesTransferred
	m.metrics.ReplicasCreated += int64(len(res.Targets))
	m.metrics.RepairDuration.add(took)

	requeue := func(nextErr string) (Task, bool) {
		t.LastError = nextErr
		if res.StateAfter != "" && res.StateAfter != durability.Healthy {
			t.Priority, t.HealthyReplicas = res.StateAfter, res.HealthyAfter
		}
		if t.Attempts >= m.cfg.MaxAttempts {
			t.Status = StatusFailed
			m.failed[t.ObjectID] = FailedTask{Task: t, FailedAt: now}
			m.metrics.Failed++
			return t, false
		}
		at := now.Add(m.cfg.backoff(t.Attempts))
		t.Status, t.NextAttemptAt, t.StartedAt = StatusRetryWait, &at, nil
		queued := t // the queue owns its own copy; t stays local
		m.queued[t.ObjectID] = &queued
		m.metrics.Retries++
		return t, true
	}

	var (
		logName  events.Name
		retried  bool
		parked   bool
		retryAt  *time.Time
		recovery time.Duration
	)
	switch {
	case errors.Is(err, ErrSuperseded):
		entry.Outcome = "superseded"
		m.metrics.Superseded++
		logName = events.RepairSuperseded
	case errors.Is(err, ErrInProgress):
		entry.Outcome = "deferred"
		at := now.Add(m.cfg.BaseBackoff)
		t.Status, t.NextAttemptAt, t.StartedAt, t.Attempts = StatusRetryWait, &at, nil, t.Attempts-1
		queued := t
		m.queued[t.ObjectID] = &queued
		logName = events.RepairRetry
		retried, retryAt = true, &at
	case err == nil && res.NoOp:
		entry.Outcome = "not_needed"
		m.metrics.NoOp++
		logName = events.RepairCompleted
	case err == nil && res.Restored:
		entry.Outcome = "restored"
		recovery = now.Sub(t.DetectedAt)
		entry.RecoveryMS = recovery.Milliseconds()
		m.metrics.Completed++
		m.metrics.RecoveryTime.add(recovery)
		logName = events.RepairCompleted
	default:
		msg := "partial repair: policy still not met"
		entry.Outcome = "partial"
		if err != nil {
			msg, entry.Outcome = err.Error(), "failed_attempt"
		} else {
			m.metrics.Partial++
		}
		var nt Task
		nt, retried = requeue(msg)
		parked = !retried
		retryAt = nt.NextAttemptAt
		if parked {
			entry.Outcome = "failed"
			logName = events.RepairFailed
		} else {
			logName = events.RepairRetry
		}
	}
	if err == nil && res.Restored {
		m.restored[t.ObjectID] = restoredMark{version: t.Version, at: time.Now()}
	}
	m.pushHistory(entry)
	m.notifyLocked()
	callbacks := append([]func(Result){}, m.onDone...)
	m.mu.Unlock()

	attrs := []any{"object_id", t.ObjectID, "version", t.Version, "priority", string(t.Priority),
		"attempt", t.Attempts, "outcome", entry.Outcome, "duration_ms", entry.DurationMS,
		"source", res.Source, "targets", res.Targets, "bytes", res.BytesTransferred,
		"replicas_before", res.ReplicasBefore, "replicas_after", res.ReplicasAfter,
		"healthy_before", res.HealthyBefore, "healthy_after", res.HealthyAfter}
	if err != nil {
		attrs = append(attrs, "error", err.Error())
	}
	switch logName {
	case events.RepairCompleted:
		m.log.Emit(ctx, logName, append(attrs, "recovery_ms", entry.RecoveryMS)...)
	case events.RepairRetry:
		m.log.Warn(ctx, logName, append(attrs, "retry_at", retryAt)...)
	case events.RepairFailed:
		m.log.Warn(ctx, logName, append(attrs, "final", true,
			"object_state", string(t.Priority), "note", "object remains below policy until the cluster changes")...)
	default:
		m.log.Emit(ctx, logName, attrs...)
	}
	for _, fn := range callbacks {
		fn(res)
	}
}

// Snapshot is a consistent view of the queue.
type Snapshot struct {
	Paused bool         `json:"paused"`
	Depth  int          `json:"depth"`
	Active []Task       `json:"active"`
	Queued []Task       `json:"queued"`
	Failed []FailedTask `json:"failed"`
}

// Snapshot returns active tasks, queued tasks in the exact order workers
// will take them (ignoring backoff timers), and parked failures.
func (m *Manager) Snapshot() Snapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := Snapshot{Paused: m.paused, Depth: len(m.queued), Active: []Task{}, Queued: []Task{}, Failed: []FailedTask{}}
	for _, t := range m.active {
		s.Active = append(s.Active, *t)
	}
	q := make([]*Task, 0, len(m.queued))
	for _, t := range m.queued {
		q = append(q, t)
	}
	sort.Slice(q, func(i, j int) bool { return less(q[i], q[j]) })
	for _, t := range q {
		s.Queued = append(s.Queued, *t)
	}
	sort.Slice(s.Active, func(i, j int) bool { return less(&s.Active[i], &s.Active[j]) })
	for _, f := range m.failed {
		s.Failed = append(s.Failed, f)
	}
	sort.Slice(s.Failed, func(i, j int) bool { return s.Failed[i].Task.ObjectID < s.Failed[j].Task.ObjectID })
	return s
}

// Lookup returns the task for objectID if it is queued, active or parked.
func (m *Manager) Lookup(objectID string) (Task, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if t, ok := m.active[objectID]; ok {
		return *t, true
	}
	if t, ok := m.queued[objectID]; ok {
		return *t, true
	}
	if f, ok := m.failed[objectID]; ok {
		return f.Task, true
	}
	return Task{}, false
}

// Idle reports whether nothing is queued or running.
func (m *Manager) Idle() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.queued) == 0 && len(m.active) == 0
}
