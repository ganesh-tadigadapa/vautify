// Package coordinator orchestrates object operations across storage nodes:
// replica placement, replicated writes with rollback, integrity-verified
// reads, and deletes. It owns no bytes itself beyond short-lived spool files.
package coordinator

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"vault/internal/events"
	"vault/internal/health"
	"vault/internal/keylock"
	"vault/internal/metadata"
	"vault/internal/placement"
	"vault/internal/storage"
)

// HealthView is what the coordinator needs from the health checker.
type HealthView interface {
	Healthy() []string
	State(id string) storage.NodeState
	ReportFailure(ctx context.Context, id string, err error)
	Snapshot() []health.NodeStatus
}

// Config tunes the coordinator.
type Config struct {
	// DefaultReplicationFactor applies when a request does not specify one.
	DefaultReplicationFactor int
	// MaxObjectSize bounds accepted uploads, in bytes.
	MaxObjectSize int64
	// TempDir holds spool files for in-flight uploads and reads.
	TempDir string
	// MaxConcurrentTransfers bounds simultaneous uploads+reads that are
	// spooling data, which bounds temp disk and node connection usage.
	MaxConcurrentTransfers int
	// ReplicaTimeout bounds a single replica transfer to or from a node.
	ReplicaTimeout time.Duration
	// RepairCommitDelay pauses every repair after its copies are written
	// and verified, just before the metadata commit. It is a fault-injection
	// knob for tests (e.g. killing the coordinator mid-repair); 0 in normal
	// operation.
	RepairCommitDelay time.Duration
	// WriteQuorum is how many replicas must store and verify a write before
	// it is acknowledged. The zero value is QuorumAll (every replica, the
	// P1-P3 behaviour). A write that meets its quorum but not the full RF is
	// committed DEGRADED and repaired back to RF.
	WriteQuorum Quorum
	// ReadQuorum is how many replicas must hold the current version with
	// the metadata checksum before a read returns bytes. The zero value is
	// QuorumOne (one verified replica, the P1-P3 behaviour).
	ReadQuorum Quorum
}

func (c *Config) setDefaults() {
	if c.DefaultReplicationFactor == 0 {
		c.DefaultReplicationFactor = 3
	}
	if c.MaxObjectSize == 0 {
		c.MaxObjectSize = 256 << 20
	}
	if c.TempDir == "" {
		c.TempDir = os.TempDir()
	}
	if c.MaxConcurrentTransfers == 0 {
		c.MaxConcurrentTransfers = 64
	}
	if c.ReplicaTimeout == 0 {
		c.ReplicaTimeout = 5 * time.Minute
	}
	if c.WriteQuorum.isZero() {
		c.WriteQuorum = QuorumAll
	}
	if c.ReadQuorum.isZero() {
		c.ReadQuorum = QuorumOne
	}
}

// Coordinator is safe for concurrent use.
type Coordinator struct {
	cfg    Config
	nodes  map[string]storage.StorageNode
	order  []string
	meta   metadata.Store
	placer placement.Manager
	health HealthView
	log    *events.Logger

	locks     keylock.Map // per-object write/delete/repair-commit serialisation
	transfers chan struct{}

	repairMu  sync.Mutex
	repairing map[string]bool // objects with a repair in flight

	onIntegrityFailure func(objectID string)
	hooks              testHooks
	stats              resilienceStats
}

// testHooks let unit tests interleave operations deterministically.
type testHooks struct {
	beforeRepairCommit func(objectID string)
	// beforeReplicaDelete runs just before a guarded removal deletes a
	// replica, after every verification it required has passed.
	beforeReplicaDelete func(objectID, nodeID string, version uint64)
}

// SetIntegrityFailureHook registers fn to be called whenever a replica
// integrity failure is recorded (by a read, a scrub or a repair source
// check). It must be set before the coordinator starts serving.
func (c *Coordinator) SetIntegrityFailureHook(fn func(objectID string)) { c.onIntegrityFailure = fn }

// New builds a Coordinator over the given nodes.
func New(cfg Config, nodes []storage.StorageNode, meta metadata.Store, placer placement.Manager, hv HealthView, log *events.Logger) (*Coordinator, error) {
	cfg.setDefaults()
	if len(nodes) == 0 {
		return nil, errors.New("coordinator: at least one storage node is required")
	}
	for name, q := range map[string]Quorum{"write": cfg.WriteQuorum, "read": cfg.ReadQuorum} {
		if q.Mode == "" && (q.N < 1 || q.N > len(nodes)) {
			return nil, fmt.Errorf("coordinator: %s quorum %d must be between 1 and the node count %d", name, q.N, len(nodes))
		}
	}
	if cfg.DefaultReplicationFactor < 1 || cfg.DefaultReplicationFactor > len(nodes) {
		return nil, fmt.Errorf("coordinator: default replication factor %d must be between 1 and the node count %d",
			cfg.DefaultReplicationFactor, len(nodes))
	}
	if err := os.MkdirAll(cfg.TempDir, 0o755); err != nil {
		return nil, fmt.Errorf("coordinator: temp dir: %w", err)
	}
	c := &Coordinator{
		cfg: cfg, nodes: make(map[string]storage.StorageNode, len(nodes)),
		meta: meta, placer: placer, health: hv, log: log,
		transfers: make(chan struct{}, cfg.MaxConcurrentTransfers),
		repairing: map[string]bool{},
	}
	for _, n := range nodes {
		if _, dup := c.nodes[n.ID()]; dup {
			return nil, fmt.Errorf("coordinator: duplicate node id %q", n.ID())
		}
		c.nodes[n.ID()] = n
		c.order = append(c.order, n.ID())
	}
	return c, nil
}

// Config returns the effective configuration.
func (c *Coordinator) Config() Config { return c.cfg }

// NodeCount returns the number of configured storage nodes.
func (c *Coordinator) NodeCount() int { return len(c.order) }

// Nodes returns the health view of every configured node.
func (c *Coordinator) Nodes() []health.NodeStatus { return c.health.Snapshot() }

// Stat returns an object's metadata without contacting any node.
func (c *Coordinator) Stat(ctx context.Context, id string) (metadata.Object, error) {
	return c.meta.GetObject(ctx, id)
}

func (c *Coordinator) acquireTransfer(ctx context.Context) (release func(), err error) {
	select {
	case c.transfers <- struct{}{}:
		return func() { <-c.transfers }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Replica attempt outcomes, reported to clients and in events.
const (
	OutcomeStored           = "stored"
	OutcomeVerified         = "checksum_verified"
	OutcomeChecksumMismatch = "checksum_mismatch"
	OutcomeSizeMismatch     = "size_mismatch"
	OutcomeMissing          = "replica_missing"
	OutcomeRejected         = "rejected_by_node"
	OutcomeStaleRefused     = "stale_version_refused"
	OutcomeUnavailable      = "node_unavailable"
	OutcomeError            = "error"
	OutcomeNotNeeded        = "not_needed"
	// OutcomeConfirmed: a read-quorum member re-hashed its copy of the
	// current version and it matched the metadata checksum.
	OutcomeConfirmed = "quorum_confirmed"
	// OutcomeStale: the node holds only an older version.
	OutcomeStale = "stale_version"
)

// ReplicaAttempt records what happened when the coordinator used one replica.
type ReplicaAttempt struct {
	NodeID     string `json:"node_id"`
	Outcome    string `json:"outcome"`
	Error      string `json:"error,omitempty"`
	DurationMS int64  `json:"duration_ms"`
}

// ErrInvalidReplicationFactor means the requested factor can never be met by
// this cluster (it is < 1 or larger than the number of configured nodes).
var ErrInvalidReplicationFactor = errors.New("invalid replication factor")

// ErrInvalidReadPreference means a read asked for a node that holds no replica.
var ErrInvalidReadPreference = errors.New("invalid read preference")

// ReplicationError means a write could not reach the requested replication
// factor. Nothing was committed: replicas that were written were rolled back
// and any previous version of the object is unchanged.
type ReplicationError struct {
	ObjectID string           `json:"object_id"`
	Required int              `json:"required"`
	Achieved int              `json:"achieved"`
	Attempts []ReplicaAttempt `json:"attempts"`
}

func (e *ReplicationError) Error() string {
	return fmt.Sprintf("replication policy not satisfied for %s: %d of %d required replicas written; write rolled back",
		e.ObjectID, e.Achieved, e.Required)
}

// NoVerifiedReplicaError means no replica could be read and verified.
type NoVerifiedReplicaError struct {
	ObjectID string           `json:"object_id"`
	Version  uint64           `json:"version"`
	Attempts []ReplicaAttempt `json:"attempts"`
}

func (e *NoVerifiedReplicaError) Error() string {
	return fmt.Sprintf("no verified replica of %s v%d could be read (%d replicas tried)", e.ObjectID, e.Version, len(e.Attempts))
}

func (c *Coordinator) resolveReplicationFactor(rf int) (int, error) {
	if rf == 0 {
		rf = c.cfg.DefaultReplicationFactor
	}
	if rf < 1 || rf > len(c.order) {
		return 0, fmt.Errorf("%w: %d (cluster has %d nodes)", ErrInvalidReplicationFactor, rf, len(c.order))
	}
	return rf, nil
}

func (c *Coordinator) reportIfUnavailable(ctx context.Context, nodeID string, err error) bool {
	if errors.Is(err, storage.ErrUnavailable) {
		c.health.ReportFailure(ctx, nodeID, err)
		return true
	}
	return false
}

func since(t time.Time) int64 { return time.Since(t).Milliseconds() }

// node returns the client for id. Metadata can name a node that is no
// longer configured (e.g. the cluster was reconfigured); such a node is
// reported as unavailable rather than dereferenced.
func (c *Coordinator) node(id string) (storage.StorageNode, error) {
	if n, ok := c.nodes[id]; ok {
		return n, nil
	}
	return nil, fmt.Errorf("%w: node %s is not configured on this coordinator", storage.ErrUnavailable, id)
}
