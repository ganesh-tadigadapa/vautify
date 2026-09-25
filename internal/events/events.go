// Package events defines Vault's structured event vocabulary.
//
// Every event is a single JSON log line with an "event" field carrying one of
// the names below. Later stages (repair prioritisation, timelines, dashboards)
// consume these lines, so names are part of Vault's external contract.
package events

import (
	"context"
	"io"
	"log/slog"
)

// Name is a structured event name.
type Name string

const (
	ObjectWriteStarted   Name = "OBJECT_WRITE_STARTED"
	ObjectReplicaWrite   Name = "OBJECT_REPLICA_WRITE"
	ObjectWriteCompleted Name = "OBJECT_WRITE_COMPLETED"
	ObjectWriteFailed    Name = "OBJECT_WRITE_FAILED"
	ObjectReadStarted    Name = "OBJECT_READ_STARTED"
	ObjectReadReplica    Name = "OBJECT_READ_REPLICA"
	ObjectReadCompleted  Name = "OBJECT_READ_COMPLETED"
	ObjectReadFailed     Name = "OBJECT_READ_FAILED"
	ChecksumVerified     Name = "CHECKSUM_VERIFIED"
	ChecksumMismatch     Name = "CHECKSUM_MISMATCH"
	NodeUnavailable      Name = "NODE_UNAVAILABLE"
	NodeRecovered        Name = "NODE_RECOVERED"
	ObjectDelete         Name = "OBJECT_DELETE"
	ReplicaCleanup       Name = "REPLICA_CLEANUP"

	// Phase 2: scrubbing, durability, repair, chaos.
	ScrubStarted             Name = "SCRUB_STARTED"
	ScrubReplicaVerified     Name = "SCRUB_REPLICA_VERIFIED"
	ScrubCorruptionDetected  Name = "SCRUB_CORRUPTION_DETECTED"
	ScrubReplicaMissing      Name = "SCRUB_REPLICA_MISSING"
	ScrubCompleted           Name = "SCRUB_COMPLETED"
	ObjectDegraded           Name = "OBJECT_DEGRADED"
	ObjectCritical           Name = "OBJECT_CRITICAL"
	ObjectLost               Name = "OBJECT_LOST"
	ObjectRestored           Name = "OBJECT_RESTORED"
	BlastRadiusCalculated    Name = "BLAST_RADIUS_CALCULATED"
	RepairQueued             Name = "REPAIR_QUEUED"
	RepairPriorityCalculated Name = "REPAIR_PRIORITY_CALCULATED"
	RepairDeduplicated       Name = "REPAIR_DEDUPLICATED"
	RepairStarted            Name = "REPAIR_STARTED"
	RepairSourceSelected     Name = "REPAIR_SOURCE_SELECTED"
	RepairSourceRejected     Name = "REPAIR_SOURCE_REJECTED"
	RepairTargetSelected     Name = "REPAIR_TARGET_SELECTED"
	RepairTargetFailed       Name = "REPAIR_TARGET_FAILED"
	RepairReplicaWritten     Name = "REPAIR_REPLICA_WRITTEN"
	RepairChecksumVerified   Name = "REPAIR_CHECKSUM_VERIFIED"
	RepairMetadataCommitted  Name = "REPAIR_METADATA_COMMITTED"
	RepairCompleted          Name = "REPAIR_COMPLETED"
	RepairFailed             Name = "REPAIR_FAILED"
	RepairRetry              Name = "REPAIR_RETRY"
	RepairSuperseded         Name = "REPAIR_SUPERSEDED"
	ChaosNodeKilled          Name = "CHAOS_NODE_KILLED"
	ChaosNodeTerminating     Name = "CHAOS_NODE_TERMINATING"
	ChaosReplicaCorrupted    Name = "CHAOS_REPLICA_CORRUPTED"

	// Phase 3: versions, tombstones, conflicts, stale replicas.
	VersionCreated       Name = "VERSION_CREATED"
	WriteConflict        Name = "WRITE_CONFLICT"
	TombstoneCreated     Name = "TOMBSTONE_CREATED"
	StaleReplicaDetected Name = "STALE_REPLICA_DETECTED"
	StaleReplicaRepaired Name = "STALE_REPLICA_REPAIRED"

	// Phase 4: network partitions, quorum, reconciliation, guarded removal,
	// rebalancing.
	NetworkPartitionStarted Name = "NETWORK_PARTITION_STARTED"
	NetworkPartitionArmed   Name = "NETWORK_PARTITION_ARMED"
	NetworkPartitionHealed  Name = "NETWORK_PARTITION_HEALED"
	NetworkLatencyChanged   Name = "NETWORK_LATENCY_CHANGED"
	NetworkTransferCut      Name = "NETWORK_TRANSFER_CUT"
	QuorumNotMet            Name = "QUORUM_NOT_MET"
	ReconcileStarted        Name = "RECONCILE_STARTED"
	ReconcileCompleted      Name = "RECONCILE_COMPLETED"
	ReplicaAdopted          Name = "REPLICA_ADOPTED"
	ReplicaRemoved          Name = "REPLICA_REMOVED"
	ReplicaRemovalRefused   Name = "REPLICA_REMOVAL_REFUSED"
	RebalanceStarted        Name = "REBALANCE_STARTED"
	RebalanceMoved          Name = "REBALANCE_REPLICA_MOVED"
	RebalanceCompleted      Name = "REBALANCE_COMPLETED"
)

// Logger emits structured events. It is safe for concurrent use.
type Logger struct {
	l         *slog.Logger
	component string
	rec       *Recorder
}

// New returns a Logger writing JSON lines to w, tagged with the emitting
// component (e.g. "api", "node-2").
func New(w io.Writer, component string) *Logger {
	h := slog.NewJSONHandler(w, &slog.HandlerOptions{Level: slog.LevelInfo})
	return &Logger{l: slog.New(h).With("component", component), component: component}
}

// WithRecorder returns a Logger that also keeps every event in rec, so the
// recent event timeline can be served over HTTP.
func (lg *Logger) WithRecorder(rec *Recorder) *Logger {
	cp := *lg
	cp.rec = rec
	return &cp
}

// Discard returns a Logger that drops everything. Useful in unit tests.
func Discard() *Logger {
	return &Logger{l: slog.New(slog.NewJSONHandler(io.Discard, nil))}
}

// Emit records an event at INFO level. attrs are slog key/value pairs.
func (lg *Logger) Emit(ctx context.Context, name Name, attrs ...any) {
	lg.l.Log(ctx, slog.LevelInfo, string(name), append([]any{"event", string(name)}, attrs...)...)
	if lg.rec != nil {
		lg.rec.record("INFO", name, lg.component, attrs)
	}
}

// Warn records an event at WARN level. Used for integrity and availability
// problems so they stand out when scanning logs.
func (lg *Logger) Warn(ctx context.Context, name Name, attrs ...any) {
	lg.l.Log(ctx, slog.LevelWarn, string(name), append([]any{"event", string(name)}, attrs...)...)
	if lg.rec != nil {
		lg.rec.record("WARN", name, lg.component, attrs)
	}
}

// Error logs a non-event operational error.
func (lg *Logger) Error(ctx context.Context, msg string, attrs ...any) {
	lg.l.Log(ctx, slog.LevelError, msg, attrs...)
}

// Info logs a non-event operational message.
func (lg *Logger) Info(ctx context.Context, msg string, attrs ...any) {
	lg.l.Log(ctx, slog.LevelInfo, msg, attrs...)
}
