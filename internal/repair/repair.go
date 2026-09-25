// Package repair schedules and runs object repairs.
//
// A Manager holds a risk-prioritised queue of objects whose durability is
// below policy and a bounded pool of workers that hand each task to an
// Executor (the coordinator), which does the actual verified copy.
//
// Guarantees:
//   - ordering: more urgent state first (CRITICAL before DEGRADED), then
//     fewer verified replicas, then oldest queue time, then object ID;
//   - at most one active repair per object, and at most one queue entry per
//     object (duplicate requests are merged and counted);
//   - bounded work: exactly Workers goroutines, never one per object;
//   - bounded retries with exponential backoff; a task that keeps failing is
//     parked as failed and the object stays honestly DEGRADED/CRITICAL until
//     the cluster topology changes.
package repair

import (
	"context"
	"errors"
	"time"

	"vault/internal/durability"
)

// ErrSuperseded means the object changed (new version, or deleted) while
// the repair was pending or running; the task is dropped and the new state
// will be assessed afresh.
var ErrSuperseded = errors.New("repair superseded by a newer object state")

// ErrInProgress means another repair of the same object is already running
// outside this manager's control; the task is retried shortly.
var ErrInProgress = errors.New("repair already in progress for this object")

// Task is one object awaiting or undergoing repair.
type Task struct {
	ObjectID          string           `json:"object_id"`
	Version           uint64           `json:"version"`
	Priority          durability.State `json:"priority"`
	Reason            string           `json:"reason"`
	HealthyReplicas   int              `json:"healthy_replicas"`
	ReplicationFactor int              `json:"replication_factor"`
	Status            string           `json:"status"`
	DetectedAt        time.Time        `json:"detected_at"`
	QueuedAt          time.Time        `json:"queued_at"`
	StartedAt         *time.Time       `json:"started_at,omitempty"`
	Attempts          int              `json:"attempts"`
	NextAttemptAt     *time.Time       `json:"next_attempt_at,omitempty"`
	LastError         string           `json:"last_error,omitempty"`
}

// Task statuses.
const (
	StatusQueued    = "queued"
	StatusRepairing = "repairing"
	StatusRetryWait = "retry_wait"
	StatusFailed    = "failed"
)

// less reports whether a should be repaired before b.
func less(a, b *Task) bool {
	if a.Priority.Rank() != b.Priority.Rank() {
		return a.Priority.Rank() < b.Priority.Rank()
	}
	if a.HealthyReplicas != b.HealthyReplicas {
		return a.HealthyReplicas < b.HealthyReplicas
	}
	if !a.QueuedAt.Equal(b.QueuedAt) {
		return a.QueuedAt.Before(b.QueuedAt)
	}
	return a.ObjectID < b.ObjectID
}

// Result is what an Executor reports about one repair attempt. It is
// meaningful even when the attempt returns an error (e.g. partial progress).
type Result struct {
	ObjectID          string           `json:"object_id"`
	Version           uint64           `json:"version"`
	ReplicationFactor int              `json:"replication_factor"`
	StateBefore       durability.State `json:"state_before"`
	StateAfter        durability.State `json:"state_after"`
	HealthyBefore     int              `json:"healthy_before"`
	HealthyAfter      int              `json:"healthy_after"`
	ReplicasBefore    []string         `json:"replicas_before"`
	ReplicasAfter     []string         `json:"replicas_after"`
	Source            string           `json:"source,omitempty"`
	Targets           []string         `json:"targets,omitempty"`
	FailedTargets     []string         `json:"failed_targets,omitempty"`
	RejectedSources   []string         `json:"rejected_sources,omitempty"`
	BytesTransferred  int64            `json:"bytes_transferred"`
	// NoOp: the object already met its policy; nothing was done.
	NoOp bool `json:"no_op"`
	// Restored: the object meets its policy after this attempt.
	Restored bool `json:"restored"`
}

// Executor performs one repair attempt.
type Executor interface {
	Repair(ctx context.Context, t Task) (Result, error)
}

// Config tunes the Manager.
type Config struct {
	Workers     int
	MaxAttempts int
	BaseBackoff time.Duration
	MaxBackoff  time.Duration
	HistorySize int
}

func (c *Config) setDefaults() {
	if c.Workers <= 0 {
		c.Workers = 4
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = 5
	}
	if c.BaseBackoff <= 0 {
		c.BaseBackoff = 500 * time.Millisecond
	}
	if c.MaxBackoff <= 0 {
		c.MaxBackoff = 30 * time.Second
	}
	if c.HistorySize <= 0 {
		c.HistorySize = 500
	}
}

func (c Config) backoff(attempts int) time.Duration {
	d := c.BaseBackoff
	for i := 1; i < attempts && d < c.MaxBackoff; i++ {
		d *= 2
	}
	return min(d, c.MaxBackoff)
}
