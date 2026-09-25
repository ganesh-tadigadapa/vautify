package coordinator

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"vault/internal/events"
	"vault/internal/storage"
)

var (
	// ErrUnknownNode: no configured node has that ID.
	ErrUnknownNode = errors.New("unknown node")
	// ErrNotAReplica: the node holds no tracked replica of the object.
	ErrNotAReplica = errors.New("node holds no replica of this object")
	// ErrChaosUnsupported: the node client cannot drive chaos operations.
	ErrChaosUnsupported = errors.New("node does not support chaos operations")
)

// KillResult reports a chaos node kill.
type KillResult struct {
	NodeID               string    `json:"node_id"`
	Address              string    `json:"address"`
	RequestedAt          time.Time `json:"requested_at"`
	ConfirmedUnavailable bool      `json:"confirmed_unavailable"`
	ConfirmationMS       int64     `json:"confirmation_ms"`
}

func (c *Coordinator) chaosNode(nodeID string) (storage.ChaosNode, error) {
	n, ok := c.nodes[nodeID]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnknownNode, nodeID)
	}
	cn, ok := n.(storage.ChaosNode)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrChaosUnsupported, nodeID)
	}
	return cn, nil
}

// KillNode makes a storage node process terminate (via the node's chaos
// endpoint; the coordinator does not own node processes). It then probes
// the node until it stops answering, and reports that observation to the
// health checker exactly as a failed request would.
func (c *Coordinator) KillNode(ctx context.Context, nodeID string) (KillResult, error) {
	cn, err := c.chaosNode(nodeID)
	if err != nil {
		return KillResult{}, err
	}
	res := KillResult{NodeID: nodeID, Address: c.nodes[nodeID].Address(), RequestedAt: time.Now().UTC()}
	if err := cn.Terminate(ctx); err != nil {
		return res, err
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		pctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		_, perr := c.nodes[nodeID].Health(pctx)
		cancel()
		if perr != nil && errors.Is(perr, storage.ErrUnavailable) {
			res.ConfirmedUnavailable = true
			res.ConfirmationMS = time.Since(res.RequestedAt).Milliseconds()
			c.health.ReportFailure(ctx, nodeID, fmt.Errorf("unreachable after chaos terminate: %w", perr))
			break
		}
		select {
		case <-ctx.Done():
			return res, ctx.Err()
		case <-time.After(25 * time.Millisecond):
		}
	}
	c.log.Warn(ctx, events.ChaosNodeKilled, "node_id", nodeID,
		"confirmed_unavailable", res.ConfirmedUnavailable, "confirmation_ms", res.ConfirmationMS)
	return res, nil
}

// CorruptReplica silently damages the bytes of object's current-version
// replica on nodeID's disk. Nothing is recorded in metadata: Vault must
// discover the damage itself, by scrub or read.
func (c *Coordinator) CorruptReplica(ctx context.Context, objectID, nodeID string) (storage.CorruptResult, error) {
	cn, err := c.chaosNode(nodeID)
	if err != nil {
		return storage.CorruptResult{}, err
	}
	obj, err := c.meta.GetObject(ctx, objectID)
	if err != nil {
		return storage.CorruptResult{}, err
	}
	if !slices.Contains(obj.Replicas, nodeID) {
		return storage.CorruptResult{}, fmt.Errorf("%w: %s holds none of %s (replicas %v)", ErrNotAReplica, nodeID, objectID, obj.Replicas)
	}
	res, err := cn.CorruptReplica(ctx, objectID, obj.Version)
	if err != nil {
		return res, err
	}
	c.log.Warn(ctx, events.ChaosReplicaCorrupted, "object_id", objectID, "version", obj.Version, "node_id", nodeID,
		"offset", res.Offset, "length", res.Length, "sha256_before", res.Before.SHA256, "sha256_after", res.After.SHA256)
	return res, nil
}
