package coordinator

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"vault/internal/events"
	"vault/internal/integrity"
	"vault/internal/metadata"
	"vault/internal/storage"
)

// Quorum is a quorum size policy: an absolute count, or relative to the
// object's replication factor.
type Quorum struct {
	// Mode is "all" (= RF), "majority" (RF/2+1) or "" for the count N.
	Mode string `json:"mode,omitempty"`
	N    int    `json:"n,omitempty"`
}

var (
	// QuorumAll requires every replica: the P1-P3 write behaviour.
	QuorumAll = Quorum{Mode: "all"}
	// QuorumOne requires a single verified replica: the P1-P3 read behaviour.
	QuorumOne = Quorum{N: 1}
)

// ErrInvalidQuorum means a requested quorum can never be satisfied or is
// malformed.
var ErrInvalidQuorum = errors.New("invalid quorum")

// ErrQuorumNotMet is wrapped by every *QuorumError.
var ErrQuorumNotMet = errors.New("quorum not met")

// ParseQuorum parses "all", "majority", "one" or a positive integer.
func ParseQuorum(s string) (Quorum, error) {
	switch v := strings.ToLower(strings.TrimSpace(s)); v {
	case "all":
		return QuorumAll, nil
	case "majority":
		return Quorum{Mode: "majority"}, nil
	case "one":
		return QuorumOne, nil
	default:
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			return Quorum{}, fmt.Errorf("%w: %q (want all, majority, one or a positive integer)", ErrInvalidQuorum, s)
		}
		return Quorum{N: n}, nil
	}
}

func (q Quorum) String() string {
	if q.Mode != "" {
		return q.Mode
	}
	return strconv.Itoa(q.N)
}

func (q Quorum) isZero() bool { return q.Mode == "" && q.N == 0 }

// resolve returns the quorum size for replication factor rf. An absolute
// count above rf is an error when the client asked for it (explicit), and
// is capped at rf when it is the configured default.
func (q Quorum) resolve(rf int, explicit bool) (int, error) {
	switch q.Mode {
	case "all":
		return rf, nil
	case "majority":
		return rf/2 + 1, nil
	}
	if q.N < 1 {
		return 0, fmt.Errorf("%w: %d", ErrInvalidQuorum, q.N)
	}
	if q.N > rf {
		if explicit {
			return 0, fmt.Errorf("%w: quorum %d exceeds the replication factor %d", ErrInvalidQuorum, q.N, rf)
		}
		return rf, nil
	}
	return q.N, nil
}

func (c *Coordinator) writeQuorum(requested *Quorum, rf int) (int, error) {
	if requested != nil {
		return requested.resolve(rf, true)
	}
	return c.cfg.WriteQuorum.resolve(rf, false)
}

func (c *Coordinator) readQuorum(requested *Quorum, rf int) (int, error) {
	if requested != nil {
		return requested.resolve(rf, true)
	}
	return c.cfg.ReadQuorum.resolve(rf, false)
}

// QuorumError means an operation could not gather its quorum. A write that
// fails its quorum commits nothing (written replicas are rolled back); a
// read that fails its quorum returns no bytes.
type QuorumError struct {
	ObjectID          string           `json:"object_id"`
	Operation         string           `json:"operation"` // "write" or "read"
	Version           uint64           `json:"version,omitempty"`
	Required          int              `json:"required"`
	Achieved          int              `json:"achieved"`
	ReplicationFactor int              `json:"replication_factor"`
	HealthyNodes      int              `json:"healthy_nodes"`
	Detail            string           `json:"detail,omitempty"`
	Attempts          []ReplicaAttempt `json:"attempts,omitempty"`
}

func (e *QuorumError) Error() string {
	s := fmt.Sprintf("%s quorum not met for %s: %d of %d required replicas", e.Operation, e.ObjectID, e.Achieved, e.Required)
	if e.Detail != "" {
		s += " (" + e.Detail + ")"
	}
	return s
}

func (e *QuorumError) Unwrap() error { return ErrQuorumNotMet }

func (c *Coordinator) quorumFailed(ctx context.Context, e *QuorumError) error {
	e.HealthyNodes = len(c.health.Healthy())
	c.log.Warn(ctx, events.QuorumNotMet, "object_id", e.ObjectID, "operation", e.Operation,
		"version", e.Version, "required", e.Required, "achieved", e.Achieved,
		"replication_factor", e.ReplicationFactor, "healthy_nodes", e.HealthyNodes, "detail", e.Detail)
	return e
}

// confirmReadQuorum asks every other listed replica of the version res
// served to re-hash its copy, and succeeds only if at least r replicas in
// total (the one that served, plus confirmations) hold that exact version
// with the metadata checksum. A replica on an unreachable node, a stale
// replica (older version only), a missing or a corrupted one never counts.
func (c *Coordinator) confirmReadQuorum(ctx context.Context, res *ReadResult, r int) error {
	obj := res.Object
	var candidates []string
	for _, n := range c.readOrder(obj, "") {
		if n != res.ServedBy {
			candidates = append(candidates, n)
		}
	}
	results := make([]ReplicaAttempt, len(candidates))
	var wg sync.WaitGroup
	for i, n := range candidates {
		wg.Add(1)
		go func(i int, n string) {
			defer wg.Done()
			results[i] = c.confirmReplica(ctx, obj, n)
		}(i, n)
	}
	wg.Wait()
	res.Confirmed = []string{res.ServedBy}
	res.Confirmations = results
	for _, a := range results {
		if a.Outcome == OutcomeConfirmed {
			res.Confirmed = append(res.Confirmed, a.NodeID)
		}
	}
	if len(res.Confirmed) >= r {
		return nil
	}
	return c.quorumFailed(ctx, &QuorumError{
		ObjectID: obj.ID, Operation: "read", Version: obj.Version, Required: r, Achieved: len(res.Confirmed),
		ReplicationFactor: obj.ReplicationFactor, Attempts: append(append([]ReplicaAttempt{}, res.Attempts...), results...),
		Detail: "no bytes were returned",
	})
}

// confirmReplica checks, without moving object bytes, that nodeID holds
// obj's current version with the metadata checksum: the node re-hashes its
// file and reports the version its record names.
func (c *Coordinator) confirmReplica(ctx context.Context, obj metadata.Object, nodeID string) (a ReplicaAttempt) {
	start := time.Now()
	a.NodeID = nodeID
	defer func() { a.DurationMS = since(start) }()
	n, err := c.node(nodeID)
	var vr storage.VerifyResult
	if err == nil {
		vctx, cancel := context.WithTimeout(ctx, c.cfg.ReplicaTimeout)
		vr, err = n.VerifyReplica(vctx, obj.ID, obj.Version)
		cancel()
	}
	want := integrity.Digest{SHA256: obj.SHA256, Size: obj.Size}
	switch {
	case err == nil && vr.ObjectID == obj.ID && vr.Version == obj.Version && integrity.Verify(want, vr.Actual) == nil:
		a.Outcome = OutcomeConfirmed
	case err == nil:
		reason, outcome := metadata.ReasonChecksumMismatch, OutcomeChecksumMismatch
		if vr.Actual.Size != want.Size {
			reason, outcome = metadata.ReasonSizeMismatch, OutcomeSizeMismatch
		}
		a.Outcome, a.Error = outcome, fmt.Sprintf("re-hash %s/%d does not match metadata", vr.Actual.SHA256, vr.Actual.Size)
		c.log.Warn(ctx, events.ChecksumMismatch, "stage", "read_quorum", "object_id", obj.ID, "version", obj.Version,
			"node_id", nodeID, "reason", reason, "node_alive", true,
			"expected_sha256", want.SHA256, "actual_sha256", vr.Actual.SHA256)
		c.recordFailure(ctx, obj, nodeID, reason, vr.Actual, 0)
	case errors.Is(err, storage.ErrNotFound):
		a.Error = err.Error()
		if older := c.newestOlderVersion(ctx, n, obj); older > 0 {
			a.Outcome = OutcomeStale
			c.log.Warn(ctx, events.StaleReplicaDetected, "object_id", obj.ID, "version", obj.Version,
				"node_id", nodeID, "stale_version", older, "listed_replica", true, "stage", "read_quorum")
			c.recordFailure(ctx, obj, nodeID, metadata.ReasonStaleVersion, integrity.Digest{}, older)
		} else {
			a.Outcome = OutcomeMissing
			c.recordFailure(ctx, obj, nodeID, metadata.ReasonReplicaMissing, integrity.Digest{}, 0)
		}
	case c.reportIfUnavailable(ctx, nodeID, err):
		a.Outcome, a.Error = OutcomeUnavailable, err.Error()
	default:
		a.Outcome, a.Error = OutcomeError, err.Error()
	}
	return
}
