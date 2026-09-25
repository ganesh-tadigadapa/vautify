package coordinator

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"time"

	"vault/internal/events"
	"vault/internal/integrity"
	"vault/internal/metadata"
	"vault/internal/storage"
)

// ReadOptions adjusts how replicas are chosen for a read.
type ReadOptions struct {
	// PreferNode, if set, is tried first. It must be a node that holds a
	// replica of the object. Other replicas are still used as fallbacks.
	PreferNode string
	// ReadQuorum overrides the configured read quorum for this read.
	ReadQuorum *Quorum
}

// ReadResult is a verified object. Body yields exactly the bytes whose
// SHA-256 matched the object's metadata; the caller must Close it.
type ReadResult struct {
	Object   metadata.Object
	ServedBy string
	Attempts []ReplicaAttempt
	Body     io.ReadCloser
	// ReadQuorum is the number of replicas that had to hold the current
	// version with the metadata checksum; Confirmed lists the ones that did
	// (the serving replica first).
	ReadQuorum int
	Confirmed  []string
	// Confirmations are the re-hash checks of the other listed replicas
	// (only when ReadQuorum > 1).
	Confirmations []ReplicaAttempt
}

// Get returns the object's bytes after verifying them against the
// metadata checksum. Replicas that fail verification are rejected, recorded
// as integrity failures, and the next replica is tried. Bytes are never
// handed to the caller before they have been fully verified.
//
// With a read quorum R > 1, the other listed replicas re-hash their copies
// and the bytes are returned only if at least R replicas in total hold the
// current version with the metadata checksum; otherwise *QuorumError.
// Metadata (never a node) names the current version, so a stale or
// partitioned replica can neither serve nor count toward the quorum, and a
// tombstoned object is not found.
func (c *Coordinator) Get(ctx context.Context, id string, opts ReadOptions) (*ReadResult, error) {
	obj, err := c.meta.GetObject(ctx, id)
	if err != nil {
		return nil, err
	}
	if opts.PreferNode != "" && !slices.Contains(obj.Replicas, opts.PreferNode) {
		return nil, fmt.Errorf("%w: node %q holds no replica of %s (replicas: %v)",
			ErrInvalidReadPreference, opts.PreferNode, id, obj.Replicas)
	}
	r, err := c.readQuorum(opts.ReadQuorum, obj.ReplicationFactor)
	if err != nil {
		return nil, err
	}

	release, err := c.acquireTransfer(ctx)
	if err != nil {
		return nil, err
	}
	defer release()

	res, err := c.readQuorumVerified(ctx, obj, opts, r)
	var nv *NoVerifiedReplicaError
	var qe *QuorumError
	if errors.As(err, &nv) || errors.As(err, &qe) {
		// A concurrent overwrite may have replaced (and cleaned up) the
		// version we started with. If so, retry once against the new one.
		if latest, lerr := c.meta.GetObject(ctx, id); lerr == nil && latest.Version != obj.Version {
			return c.readQuorumVerified(ctx, latest, opts, r)
		}
	}
	return res, err
}

func (c *Coordinator) readQuorumVerified(ctx context.Context, obj metadata.Object, opts ReadOptions, r int) (*ReadResult, error) {
	if r > 1 && len(obj.Replicas) < r {
		return nil, c.quorumFailed(ctx, &QuorumError{ObjectID: obj.ID, Operation: "read", Version: obj.Version,
			Required: r, ReplicationFactor: obj.ReplicationFactor,
			Detail: fmt.Sprintf("only %d replicas are listed for v%d; no bytes were returned", len(obj.Replicas), obj.Version)})
	}
	res, err := c.readVerified(ctx, obj, opts)
	if err != nil {
		var nv *NoVerifiedReplicaError
		if r > 1 && errors.As(err, &nv) {
			return nil, c.quorumFailed(ctx, &QuorumError{ObjectID: obj.ID, Operation: "read", Version: obj.Version,
				Required: r, ReplicationFactor: obj.ReplicationFactor, Attempts: nv.Attempts,
				Detail: "no replica could be read and verified"})
		}
		return nil, err
	}
	res.ReadQuorum, res.Confirmed = r, []string{res.ServedBy}
	if r > 1 {
		if err := c.confirmReadQuorum(ctx, res, r); err != nil {
			res.Body.Close()
			return nil, err
		}
	}
	return res, nil
}

// readOrder returns replicas in the order they should be tried: the
// preferred node, then HEALTHY replicas, then replicas currently believed
// UNAVAILABLE (a last resort, since health information can be stale).
func (c *Coordinator) readOrder(obj metadata.Object, prefer string) []string {
	var healthy, unavailable []string
	for _, n := range obj.Replicas {
		if n == prefer {
			continue
		}
		if c.health.State(n) == storage.StateHealthy {
			healthy = append(healthy, n)
		} else {
			unavailable = append(unavailable, n)
		}
	}
	order := make([]string, 0, len(obj.Replicas))
	if prefer != "" {
		order = append(order, prefer)
	}
	order = append(order, healthy...)
	return append(order, unavailable...)
}

func (c *Coordinator) readVerified(ctx context.Context, obj metadata.Object, opts ReadOptions) (*ReadResult, error) {
	order := c.readOrder(obj, opts.PreferNode)
	c.log.Emit(ctx, events.ObjectReadStarted, "object_id", obj.ID, "version", obj.Version,
		"sha256", obj.SHA256, "candidates", order)

	want := integrity.Digest{SHA256: obj.SHA256, Size: obj.Size}
	var attempts []ReplicaAttempt
	for i, nodeID := range order {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		spool, a := c.readOne(ctx, obj, want, nodeID)
		attempts = append(attempts, a)
		if spool == nil {
			continue
		}
		for _, rest := range order[i+1:] {
			attempts = append(attempts, ReplicaAttempt{NodeID: rest, Outcome: OutcomeNotNeeded})
		}
		body, err := spool.Open()
		if err != nil {
			spool.Close()
			return nil, err
		}
		c.log.Emit(ctx, events.ObjectReadCompleted, "object_id", obj.ID, "version", obj.Version,
			"served_by", nodeID, "rejected", rejectedNodes(attempts))
		return &ReadResult{
			Object: obj, ServedBy: nodeID, Attempts: attempts,
			Body: &spoolReader{ReadCloser: body, spool: spool},
		}, nil
	}

	err := &NoVerifiedReplicaError{ObjectID: obj.ID, Version: obj.Version, Attempts: attempts}
	c.log.Warn(ctx, events.ObjectReadFailed, "object_id", obj.ID, "version", obj.Version, "error", err.Error())
	return nil, err
}

// readOne fetches one replica into a spool and verifies it. It returns the
// spool only if verification passed.
func (c *Coordinator) readOne(ctx context.Context, obj metadata.Object, want integrity.Digest, nodeID string) (*integrity.Spooled, ReplicaAttempt) {
	start := time.Now()
	a := ReplicaAttempt{NodeID: nodeID}
	logAttempt := func() {
		a.DurationMS = since(start)
		attrs := []any{"object_id", obj.ID, "version", obj.Version, "node_id", nodeID,
			"outcome", a.Outcome, "duration_ms", a.DurationMS}
		if a.Outcome == OutcomeVerified {
			c.log.Emit(ctx, events.ObjectReadReplica, attrs...)
		} else {
			c.log.Warn(ctx, events.ObjectReadReplica, append(attrs, "error", a.Error)...)
		}
	}

	rctx, cancel := context.WithTimeout(ctx, c.cfg.ReplicaTimeout)
	defer cancel()
	var rc io.ReadCloser
	n, err := c.node(nodeID)
	if err == nil {
		rc, err = n.GetReplica(rctx, obj.ID, obj.Version)
	}
	if err != nil {
		switch {
		case errors.Is(err, storage.ErrNotFound):
			a.Outcome = OutcomeMissing
			c.recordFailure(ctx, obj, nodeID, metadata.ReasonReplicaMissing, integrity.Digest{}, 0)
		case c.reportIfUnavailable(ctx, nodeID, err):
			a.Outcome = OutcomeUnavailable
		default:
			a.Outcome = OutcomeError
		}
		a.Error = err.Error()
		logAttempt()
		return nil, a
	}

	// Read at most one byte more than expected: enough to detect growth
	// without letting a bad replica fill the coordinator's disk.
	spool, err := integrity.Spool(rc, c.cfg.TempDir, want.Size)
	rc.Close()
	if errors.Is(err, integrity.ErrTooLarge) {
		a.Outcome = OutcomeSizeMismatch
		a.Error = fmt.Sprintf("replica is larger than the expected %d bytes", want.Size)
		logAttempt()
		c.log.Warn(ctx, events.ChecksumMismatch, "object_id", obj.ID, "version", obj.Version, "node_id", nodeID,
			"reason", metadata.ReasonSizeMismatch, "expected_size", want.Size, "expected_sha256", want.SHA256)
		c.recordFailure(ctx, obj, nodeID, metadata.ReasonSizeMismatch, integrity.Digest{Size: want.Size + 1}, 0)
		return nil, a
	}
	if err != nil {
		a.Outcome = OutcomeError
		a.Error = "transfer failed: " + err.Error()
		logAttempt()
		return nil, a
	}

	if verr := integrity.Verify(want, spool.Digest); verr != nil {
		spool.Close()
		reason := metadata.ReasonChecksumMismatch
		a.Outcome = OutcomeChecksumMismatch
		if spool.Digest.Size != want.Size {
			reason, a.Outcome = metadata.ReasonSizeMismatch, OutcomeSizeMismatch
		}
		a.Error = verr.Error()
		logAttempt()
		c.log.Warn(ctx, events.ChecksumMismatch, "object_id", obj.ID, "version", obj.Version, "node_id", nodeID,
			"reason", reason, "node_alive", true,
			"expected_sha256", want.SHA256, "actual_sha256", spool.Digest.SHA256,
			"expected_size", want.Size, "actual_size", spool.Digest.Size)
		c.recordFailure(ctx, obj, nodeID, reason, spool.Digest, 0)
		return nil, a
	}

	a.Outcome = OutcomeVerified
	logAttempt()
	c.log.Emit(ctx, events.ChecksumVerified, "stage", "read", "object_id", obj.ID, "version", obj.Version,
		"node_id", nodeID, "sha256", spool.Digest.SHA256, "size", spool.Digest.Size)
	return spool, a
}

func (c *Coordinator) recordFailure(ctx context.Context, obj metadata.Object, nodeID, reason string, observed integrity.Digest, observedVersion uint64) {
	f := metadata.IntegrityFailure{
		NodeID: nodeID, Version: obj.Version, Reason: reason,
		ExpectedSHA256: obj.SHA256, ObservedSHA256: observed.SHA256,
		ExpectedSize: obj.Size, ObservedSize: observed.Size, ObservedVersion: observedVersion,
		LastDetectedAt: time.Now().UTC(),
	}
	err := c.meta.RecordIntegrityFailure(context.WithoutCancel(ctx), obj.ID, f)
	if err != nil && !errors.Is(err, metadata.ErrVersionConflict) && !errors.Is(err, metadata.ErrNotFound) {
		c.log.Error(ctx, "failed to record integrity failure", "object_id", obj.ID, "node_id", nodeID, "error", err.Error())
	}
	if err == nil && c.onIntegrityFailure != nil {
		c.onIntegrityFailure(obj.ID)
	}
}

func rejectedNodes(attempts []ReplicaAttempt) []string {
	var out []string
	for _, a := range attempts {
		if a.Outcome != OutcomeVerified && a.Outcome != OutcomeNotNeeded {
			out = append(out, a.NodeID+"="+a.Outcome)
		}
	}
	return out
}

// RejectedReplicas returns "node=outcome" entries for every replica that was
// tried and not used.
func (r *ReadResult) RejectedReplicas() []string { return rejectedNodes(r.Attempts) }

// spoolReader deletes the spool file when the reader is closed.
type spoolReader struct {
	io.ReadCloser
	spool *integrity.Spooled
}

func (s *spoolReader) Close() error {
	err := s.ReadCloser.Close()
	if serr := s.spool.Close(); err == nil {
		err = serr
	}
	return err
}
