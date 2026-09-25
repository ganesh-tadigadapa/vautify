// Package metadata defines Vault's object metadata model and the Store
// abstraction behind which it is persisted.
//
// Stage 1 ships a single-process, file-backed implementation (FileStore).
// It is durable across coordinator restarts but it is NOT replicated and NOT
// distributed-consistent; the interface exists so a replicated/consensus
// backed store can replace it later without touching the coordinator.
package metadata

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"
)

var (
	ErrNotFound = errors.New("object not found")
	// ErrDeleted is returned by live-object reads when the object's latest
	// record is a tombstone. It wraps ErrNotFound, so callers that only
	// care about "is there a live object" need not distinguish.
	ErrDeleted         = fmt.Errorf("%w: deleted (tombstone)", ErrNotFound)
	ErrExists          = errors.New("object already exists")
	ErrVersionConflict = errors.New("object version conflict")
)

// Reasons an integrity failure can be recorded for.
const (
	ReasonChecksumMismatch = "checksum_mismatch"
	ReasonSizeMismatch     = "size_mismatch"
	ReasonReplicaMissing   = "replica_missing"
	// ReasonStaleVersion: the node holds only an older version of the
	// object than the current one.
	ReasonStaleVersion = "stale_version"
)

// IntegrityFailure records that a replica of the current version was found
// not to match the object's checksum (or was missing from its node). It is a
// factual observation used by later repair stages; nothing in stage 1 acts
// on it automatically.
type IntegrityFailure struct {
	NodeID         string `json:"node_id"`
	Version        uint64 `json:"version"`
	Reason         string `json:"reason"`
	ExpectedSHA256 string `json:"expected_sha256"`
	ObservedSHA256 string `json:"observed_sha256,omitempty"`
	ExpectedSize   int64  `json:"expected_size"`
	ObservedSize   int64  `json:"observed_size"`
	// ObservedVersion is the newest version the node actually holds, when
	// that is older than the current version (ReasonStaleVersion).
	ObservedVersion uint64    `json:"observed_version,omitempty"`
	FirstDetectedAt time.Time `json:"first_detected_at"`
	LastDetectedAt  time.Time `json:"last_detected_at"`
	Occurrences     int       `json:"occurrences"`
}

// Object is the metadata for one stored object.
type Object struct {
	ID                string             `json:"id"`
	Version           uint64             `json:"version"`
	Size              int64              `json:"size"`
	SHA256            string             `json:"sha256"`
	CreatedAt         time.Time          `json:"created_at"`
	UpdatedAt         time.Time          `json:"updated_at"`
	ReplicationFactor int                `json:"replication_factor"`
	Replicas          []string           `json:"replicas"`
	IntegrityFailures []IntegrityFailure `json:"integrity_failures,omitempty"`
	// LastRepairedAt is set when a repair changed the replica set. Repairs do
	// not change content, so they do not touch Version or UpdatedAt.
	LastRepairedAt *time.Time `json:"last_repaired_at,omitempty"`
	// Deleted marks a tombstone: the object was deleted at Version. A
	// tombstone is durable and has a version newer than every replica of
	// the object that ever existed, so no older data can resurrect it; a
	// later PUT creates Version+1.
	Deleted   bool       `json:"deleted,omitempty"`
	DeletedAt *time.Time `json:"deleted_at,omitempty"`
}

// FailureFor returns the integrity failure recorded against nodeID for the
// object's CURRENT version, if any. Failures of older versions are ignored.
func (o Object) FailureFor(nodeID string) (IntegrityFailure, bool) {
	for _, f := range o.IntegrityFailures {
		if f.NodeID == nodeID && f.Version == o.Version {
			return f, true
		}
	}
	return IntegrityFailure{}, false
}

// WithoutFailure returns a copy of failures without the entry for nodeID at
// the given version.
func WithoutFailure(failures []IntegrityFailure, nodeID string, version uint64) []IntegrityFailure {
	out := failures[:0:0]
	for _, f := range failures {
		if f.NodeID != nodeID || f.Version != version {
			out = append(out, f)
		}
	}
	return out
}

// Clone returns a deep copy so callers never share slices with the store.
func (o Object) Clone() Object {
	o.Replicas = slices.Clone(o.Replicas)
	o.IntegrityFailures = slices.Clone(o.IntegrityFailures)
	return o
}

// Store persists object metadata. Every record is either a live object or
// a tombstone. GetObject, ListObjects, Modify and RecordIntegrityFailure
// see LIVE objects only (a tombstoned object reads as ErrDeleted/absent);
// GetRecord and ListRecords see both.
type Store interface {
	GetObject(ctx context.Context, id string) (Object, error)
	// GetRecord returns the latest record, live or tombstone.
	GetRecord(ctx context.Context, id string) (Object, error)
	// ListRecords returns every record, live and tombstones.
	ListRecords(ctx context.Context) ([]Object, error)
	// PutObject creates a new object; ErrExists if id is already present.
	PutObject(ctx context.Context, obj Object) error
	// UpdateObject replaces an existing record (live or tombstone) with a
	// newer one. obj.Version must be greater than the stored version,
	// otherwise ErrVersionConflict. Writing a tombstone is an update.
	UpdateObject(ctx context.Context, obj Object) error
	// DeleteObject removes a record entirely, tombstone included. Vault's
	// delete path writes tombstones instead; this exists for completeness
	// of the store abstraction.
	DeleteObject(ctx context.Context, id string) error
	ListObjects(ctx context.Context) ([]Object, error)
	// RecordIntegrityFailure attaches f to object id if f.Version is still
	// the current version (ErrVersionConflict otherwise). Repeated failures
	// for the same node and version are merged.
	RecordIntegrityFailure(ctx context.Context, id string, f IntegrityFailure) error
	// Modify atomically applies fn to the current state of object id and
	// persists the result. fn may change anything except ID and Version
	// (a new version is created with UpdateObject). If fn returns an error,
	// nothing is written and that error is returned.
	Modify(ctx context.Context, id string, fn func(Object) (Object, error)) (Object, error)
}
