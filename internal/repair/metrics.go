package repair

import (
	"time"

	"vault/internal/durability"
)

// DurationStats summarises observed durations.
type DurationStats struct {
	Count  int64 `json:"count"`
	LastMS int64 `json:"last_ms"`
	MinMS  int64 `json:"min_ms"`
	MaxMS  int64 `json:"max_ms"`
	AvgMS  int64 `json:"avg_ms"`
	sumMS  int64
}

func (d *DurationStats) add(x time.Duration) {
	ms := x.Milliseconds()
	d.Count++
	d.sumMS += ms
	d.LastMS = ms
	if d.Count == 1 || ms < d.MinMS {
		d.MinMS = ms
	}
	if ms > d.MaxMS {
		d.MaxMS = ms
	}
	d.AvgMS = d.sumMS / d.Count
}

type metrics struct {
	Queued, Deduplicated, Started, Completed, Partial, Failed, Retries, NoOp, Superseded int64
	ReplicasCreated, BytesTransferred                                                    int64
	RepairDuration, RecoveryTime                                                         DurationStats
}

// Metrics are counters since the Manager started plus current gauges. Every
// value is derived from repairs that actually ran.
type Metrics struct {
	QueueDepth int  `json:"queue_depth"`
	Repairing  int  `json:"repairing"`
	Parked     int  `json:"parked_failed"`
	Paused     bool `json:"paused"`
	Workers    int  `json:"workers"`

	Queued       int64 `json:"queued_total"`
	Deduplicated int64 `json:"deduplicated_total"`
	Started      int64 `json:"started_total"`
	Completed    int64 `json:"repaired_total"`
	Partial      int64 `json:"partial_total"`
	Failed       int64 `json:"failed_total"`
	Retries      int64 `json:"retries_total"`
	NoOp         int64 `json:"not_needed_total"`
	Superseded   int64 `json:"superseded_total"`

	ReplicasCreated  int64 `json:"replicas_created_total"`
	BytesTransferred int64 `json:"bytes_transferred_total"`

	// RepairDuration: wall time of each repair attempt.
	RepairDuration DurationStats `json:"repair_duration"`
	// RecoveryTime: from the moment an object was first seen below policy
	// to the moment a repair restored it.
	RecoveryTime DurationStats `json:"recovery_time"`

	LastRepair *HistoryEntry `json:"last_repair,omitempty"`
}

// Metrics returns a snapshot.
func (m *Manager) Metrics() Metrics {
	m.mu.Lock()
	defer m.mu.Unlock()
	x := m.metrics
	out := Metrics{
		QueueDepth: len(m.queued), Repairing: len(m.active), Parked: len(m.failed),
		Paused: m.paused, Workers: m.cfg.Workers,
		Queued: x.Queued, Deduplicated: x.Deduplicated, Started: x.Started, Completed: x.Completed,
		Partial: x.Partial, Failed: x.Failed, Retries: x.Retries, NoOp: x.NoOp, Superseded: x.Superseded,
		ReplicasCreated: x.ReplicasCreated, BytesTransferred: x.BytesTransferred,
		RepairDuration: x.RepairDuration, RecoveryTime: x.RecoveryTime,
	}
	if m.histLen > 0 {
		last := m.history[(m.histPos-1+len(m.history))%len(m.history)]
		out.LastRepair = &last
	}
	return out
}

// HistoryEntry records one repair attempt: its timeline and what it did.
type HistoryEntry struct {
	ObjectID    string           `json:"object_id"`
	Version     uint64           `json:"version"`
	Priority    durability.State `json:"priority"`
	Reason      string           `json:"reason"`
	Attempt     int              `json:"attempt"`
	Outcome     string           `json:"outcome"`
	Error       string           `json:"error,omitempty"`
	DetectedAt  time.Time        `json:"detected_at"`
	QueuedAt    time.Time        `json:"queued_at"`
	StartedAt   time.Time        `json:"started_at"`
	CompletedAt time.Time        `json:"completed_at"`
	DurationMS  int64            `json:"duration_ms"`
	RecoveryMS  int64            `json:"recovery_ms,omitempty"`
	Result      Result           `json:"result"`
}

func (m *Manager) pushHistory(e HistoryEntry) {
	m.history[m.histPos] = e
	m.histPos = (m.histPos + 1) % len(m.history)
	if m.histLen < len(m.history) {
		m.histLen++
	}
}

// History returns up to limit most recent attempts, oldest first.
func (m *Manager) History(limit int) []HistoryEntry {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := m.histLen
	if limit > 0 && limit < n {
		n = limit
	}
	out := make([]HistoryEntry, 0, n)
	for i := n; i > 0; i-- {
		out = append(out, m.history[(m.histPos-i+len(m.history))%len(m.history)])
	}
	return out
}
