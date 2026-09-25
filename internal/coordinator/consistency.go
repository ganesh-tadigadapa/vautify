package coordinator

import (
	"context"
	"slices"
	"time"

	"vault/internal/durability"
	"vault/internal/integrity"
	"vault/internal/metadata"
	"vault/internal/storage"
)

// Replica consistency states. The first five are the replica model; the
// last describes bytes of a version newer than any committed record.
const (
	ConsistencyCurrent     = "CURRENT"
	ConsistencyStale       = "STALE"
	ConsistencyCorrupted   = "CORRUPTED"
	ConsistencyMissing     = "MISSING"
	ConsistencyUnavailable = "UNAVAILABLE"
	ConsistencyUncommitted = "UNCOMMITTED"
)

// ReplicaConsistency is what one node actually holds for an object.
type ReplicaConsistency struct {
	NodeID    string            `json:"node_id"`
	NodeState storage.NodeState `json:"node_state"`
	// Listed: metadata names this node as a replica of the current version.
	Listed bool `json:"listed"`
	// Authoritative: this replica may serve reads (listed AND CURRENT).
	Authoritative bool   `json:"authoritative"`
	State         string `json:"state"`
	// Version is the version this entry is about: the current version for
	// CURRENT/CORRUPTED/MISSING, the newest held version for STALE.
	Version      uint64   `json:"version,omitempty"`
	VersionsHeld []uint64 `json:"versions_held"`
	SHA256       string   `json:"sha256,omitempty"`
	Size         int64    `json:"size,omitempty"`
	Detail       string   `json:"detail,omitempty"`
}

// ConsistencyReport is the version-level truth about one object.
type ConsistencyReport struct {
	ObjectID          string               `json:"object_id"`
	State             string               `json:"state"` // "live" or "deleted"
	CurrentVersion    uint64               `json:"current_version"`
	SHA256            string               `json:"sha256,omitempty"`
	Size              int64                `json:"size"`
	ReplicationFactor int                  `json:"replication_factor"`
	DeletedAt         *time.Time           `json:"deleted_at,omitempty"`
	Durability        durability.State     `json:"durability,omitempty"`
	CurrentReplicas   int                  `json:"current_replicas"`
	Verified          bool                 `json:"verified"`
	Replicas          []ReplicaConsistency `json:"replicas"`
	// UncheckedNodes are unreachable nodes not listed in metadata: they may
	// hold leftover bytes that could not be inspected.
	UncheckedNodes []string `json:"unchecked_nodes"`
	// Consistent: live — exactly RF authoritative replicas and no stale or
	// uncommitted bytes seen anywhere; deleted — no node holds any bytes.
	Consistent bool      `json:"consistent"`
	CheckedAt  time.Time `json:"checked_at"`
}

// Consistency inspects every reachable node's actual holdings for object id
// and classifies each against the latest metadata record (live or
// tombstone). With verify, listed current replicas are re-hashed by their
// nodes; otherwise recorded integrity failures are used.
func (c *Coordinator) Consistency(ctx context.Context, id string, verify bool) (ConsistencyReport, error) {
	rec, err := c.meta.GetRecord(ctx, id)
	if err != nil {
		return ConsistencyReport{}, err
	}
	rep := ConsistencyReport{
		ObjectID: id, State: "live", CurrentVersion: rec.Version, SHA256: rec.SHA256, Size: rec.Size,
		ReplicationFactor: rec.ReplicationFactor, Verified: verify, CheckedAt: time.Now().UTC(),
		Replicas: []ReplicaConsistency{}, UncheckedNodes: []string{},
	}
	if rec.Deleted {
		rep.State, rep.DeletedAt, rep.SHA256 = "deleted", rec.DeletedAt, ""
	} else {
		rep.Durability = durability.Assess(rec, c.health.State).State
	}

	// Configured nodes in order, then any listed node this coordinator no
	// longer knows.
	nodes := slices.Clone(c.order)
	for _, n := range rec.Replicas {
		if !slices.Contains(nodes, n) {
			nodes = append(nodes, n)
		}
	}
	clean := true
	for _, nodeID := range nodes {
		listed := !rec.Deleted && slices.Contains(rec.Replicas, nodeID)
		rc := ReplicaConsistency{NodeID: nodeID, NodeState: c.health.State(nodeID), Listed: listed, VersionsHeld: []uint64{}}
		n, nerr := c.node(nodeID)
		var held []storage.ReplicaInfo
		if nerr == nil && rc.NodeState == storage.StateHealthy {
			lctx, cancel := context.WithTimeout(ctx, c.cfg.ReplicaTimeout)
			held, nerr = n.ListReplicas(lctx, id)
			cancel()
			c.reportIfUnavailable(ctx, nodeID, nerr)
		} else if nerr == nil {
			nerr = storage.ErrUnavailable
		}
		if nerr != nil {
			if listed {
				rc.State, rc.Version, rc.Detail = ConsistencyUnavailable, rec.Version, "node unreachable"
				rep.Replicas = append(rep.Replicas, rc)
			} else {
				rep.UncheckedNodes = append(rep.UncheckedNodes, nodeID)
			}
			continue
		}
		var hasCurrent bool
		var older, newer []uint64
		for _, h := range held {
			rc.VersionsHeld = append(rc.VersionsHeld, h.Version)
			switch {
			case !rec.Deleted && h.Version == rec.Version:
				hasCurrent = true
			case h.Version < rec.Version:
				older = append(older, h.Version)
			default:
				newer = append(newer, h.Version)
			}
		}
		if !listed && len(held) == 0 {
			continue // nothing here, nothing expected
		}
		switch {
		case hasCurrent:
			rc.State, rc.Version = ConsistencyCurrent, rec.Version
			if listed && verify {
				vctx, cancel := context.WithTimeout(ctx, c.cfg.ReplicaTimeout)
				vr, verr := n.VerifyReplica(vctx, id, rec.Version)
				cancel()
				rc.SHA256, rc.Size = vr.Actual.SHA256, vr.Actual.Size
				if verr != nil || integrity.Verify(integrity.Digest{SHA256: rec.SHA256, Size: rec.Size}, vr.Actual) != nil {
					rc.State, rc.Detail = ConsistencyCorrupted, "re-hash does not match metadata"
				}
			} else if f, ok := rec.FailureFor(nodeID); ok && f.Reason != metadata.ReasonStaleVersion && f.Reason != metadata.ReasonReplicaMissing {
				rc.State, rc.Detail = ConsistencyCorrupted, "recorded "+f.Reason
			}
			if !listed {
				rc.Detail = "copy of the current version not listed in metadata; not authoritative"
			}
		case len(older) > 0:
			rc.State, rc.Version = ConsistencyStale, slices.Max(older)
			switch {
			case rec.Deleted:
				rc.Detail = "data of a deleted object; tombstone prevents it from being served"
			case listed:
				rc.Detail = "listed replica holds only an older version"
			default:
				rc.Detail = "superseded version left behind; never served"
			}
		case len(newer) > 0:
			rc.State, rc.Version = ConsistencyUncommitted, slices.Max(newer)
			rc.Detail = "version newer than any committed record (in-flight or failed write)"
		default:
			rc.State, rc.Version, rc.Detail = ConsistencyMissing, rec.Version, "listed replica holds nothing"
		}
		rc.Authoritative = listed && rc.State == ConsistencyCurrent
		if rc.Authoritative {
			rep.CurrentReplicas++
		}
		if len(older) > 0 || len(newer) > 0 || !rc.Authoritative {
			clean = false
		}
		rep.Replicas = append(rep.Replicas, rc)
	}
	if rec.Deleted {
		rep.Consistent = clean && len(rep.Replicas) == 0
	} else {
		rep.Consistent = clean && rep.CurrentReplicas == rec.ReplicationFactor && len(rep.Replicas) == rep.CurrentReplicas
	}
	return rep, nil
}
