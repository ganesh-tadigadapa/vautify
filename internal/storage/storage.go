// Package storage defines the contract between the coordinator and storage
// nodes: the StorageNode interface, the JSON wire types exchanged over HTTP,
// and the sentinel errors a node client reports.
package storage

import (
	"context"
	"errors"
	"io"
	"time"

	"vault/internal/integrity"
)

// Node states as seen by the coordinator. Stage 1 distinguishes only these
// two; richer states (corrupted, stale, degraded) belong to later stages.
type NodeState string

const (
	StateHealthy     NodeState = "HEALTHY"
	StateUnavailable NodeState = "UNAVAILABLE"
)

// HTTP headers shared by nodes and the API.
const (
	HeaderSHA256            = "X-Vault-Sha256"
	HeaderVersion           = "X-Vault-Version"
	HeaderNodeID            = "X-Vault-Node-Id"
	HeaderServedBy          = "X-Vault-Served-By"
	HeaderRejectedReplicas  = "X-Vault-Rejected-Replicas"
	HeaderReplicationFactor = "X-Vault-Replication-Factor"
	HeaderReplicas          = "X-Vault-Replicas"
	HeaderExpectedVersion   = "X-Vault-Expected-Version"
	// HeaderReadQuorum: replicas a read required; HeaderConfirmedBy: the
	// replicas that held the current version with the metadata checksum.
	HeaderReadQuorum  = "X-Vault-Read-Quorum"
	HeaderConfirmedBy = "X-Vault-Confirmed-By"
)

var (
	// ErrNotFound: the node is reachable but does not hold the replica.
	ErrNotFound = errors.New("replica not found")
	// ErrUnavailable: the node could not be reached or failed to respond.
	ErrUnavailable = errors.New("node unavailable")
	// ErrRejected: the node refused the bytes because they did not match
	// the checksum the coordinator declared.
	ErrRejected = errors.New("replica rejected by node")
	// ErrStaleVersion: the node refused a write because it already holds a
	// newer version of the object. Older data never overwrites newer data.
	ErrStaleVersion = errors.New("node holds a newer version; stale write refused")
	// ErrChaosDisabled: the node was started without --enable-chaos.
	ErrChaosDisabled = errors.New("chaos endpoints are disabled on this node")
)

// Capacity reports a node's storage usage.
type Capacity struct {
	TotalBytes   uint64 `json:"total_bytes"`
	FreeBytes    uint64 `json:"free_bytes"`
	UsedBytes    int64  `json:"used_bytes"`
	ReplicaCount int64  `json:"replica_count"`
}

// NodeHealth is the body of a node's GET /health response.
type NodeHealth struct {
	NodeID    string    `json:"node_id"`
	Status    NodeState `json:"status"`
	StartedAt time.Time `json:"started_at"`
	Capacity  Capacity  `json:"capacity"`
}

// ReplicaInfo describes one stored replica of one object version.
type ReplicaInfo struct {
	NodeID   string    `json:"node_id"`
	ObjectID string    `json:"object_id"`
	Version  uint64    `json:"version"`
	SHA256   string    `json:"sha256"`
	Size     int64     `json:"size"`
	StoredAt time.Time `json:"stored_at"`
}

// VerifyResult is a node's re-hash of a replica it stores on disk, compared
// with the checksum it recorded when the replica was written.
type VerifyResult struct {
	NodeID   string           `json:"node_id"`
	ObjectID string           `json:"object_id"`
	Version  uint64           `json:"version"`
	Recorded integrity.Digest `json:"recorded"`
	Actual   integrity.Digest `json:"actual"`
	Valid    bool             `json:"valid"`
}

// ErrorBody is the JSON error envelope used by nodes and the API.
type ErrorBody struct {
	Error ErrorDetail `json:"error"`
}

// ErrorDetail carries a machine-readable code and a human-readable message.
type ErrorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Details any    `json:"details,omitempty"`
}

// StorageNode is the coordinator's view of one storage node.
type StorageNode interface {
	ID() string
	Address() string
	// PutReplica stores body as (objectID, version). The node must verify
	// the bytes against want and reject them with ErrRejected on mismatch.
	PutReplica(ctx context.Context, objectID string, version uint64, want integrity.Digest, body io.Reader) (ReplicaInfo, error)
	// GetReplica streams the stored bytes. The node does not vouch for them:
	// callers must verify against their own expected digest.
	GetReplica(ctx context.Context, objectID string, version uint64) (io.ReadCloser, error)
	DeleteReplica(ctx context.Context, objectID string, version uint64) error
	// DeleteObject removes every stored version of objectID.
	DeleteObject(ctx context.Context, objectID string) error
	Health(ctx context.Context) (NodeHealth, error)
	// VerifyReplica asks the node to re-hash the replica from its own disk.
	// No object bytes cross the network.
	VerifyReplica(ctx context.Context, objectID string, version uint64) (VerifyResult, error)
	// ListReplicas returns every version of objectID the node holds.
	ListReplicas(ctx context.Context, objectID string) ([]ReplicaInfo, error)
	// Inventory returns every replica the node holds, of every object.
	Inventory(ctx context.Context) ([]ReplicaInfo, error)
}

// CorruptResult describes a deliberate, silent mutation of a stored replica
// performed by a node's chaos endpoint. The node's own checksum record is
// left untouched, exactly like real bit rot.
type CorruptResult struct {
	NodeID   string           `json:"node_id"`
	ObjectID string           `json:"object_id"`
	Version  uint64           `json:"version"`
	Offset   int64            `json:"offset"`
	Length   int              `json:"length"`
	Before   integrity.Digest `json:"before"`
	After    integrity.Digest `json:"after"`
}

// ChaosNode is implemented by node clients that can drive a node's chaos
// endpoints. Nodes only honour these when started with --enable-chaos.
type ChaosNode interface {
	// Terminate makes the node process exit, as an abrupt crash would.
	Terminate(ctx context.Context) error
	// CorruptReplica flips bytes of a stored replica on the node's disk
	// while the node keeps running.
	CorruptReplica(ctx context.Context, objectID string, version uint64) (CorruptResult, error)
}
