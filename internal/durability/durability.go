// Package durability classifies how well each object is protected right
// now, from its metadata and the current node states. It is pure logic: no
// I/O, no clocks beyond timestamps, so every rule here is unit-testable.
//
// Replica state and node state are deliberately separate. A node can be
// HEALTHY (its process answers) while a replica it stores is CORRUPTED.
package durability

import (
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"vault/internal/metadata"
	"vault/internal/storage"
)

// ReplicaState is the state of one replica of the current object version.
type ReplicaState string

const (
	// ReplicaCurrent: the node is up and holds the CURRENT version, with no
	// integrity failure recorded. It was verified by its node when written,
	// and by every scrub or read that has touched it since.
	ReplicaCurrent ReplicaState = "CURRENT"
	// ReplicaStale: the node holds only an OLDER version of the object.
	// Stale replicas are never authoritative: reads and repair sources use
	// the current version only.
	ReplicaStale ReplicaState = "STALE"
	// ReplicaCorrupted: a read or scrub found bytes that do not match the
	// object's SHA-256 (or length). Its node may be perfectly alive.
	ReplicaCorrupted ReplicaState = "CORRUPTED"
	// ReplicaMissing: its node is alive but does not have the replica.
	ReplicaMissing ReplicaState = "MISSING"
	// ReplicaUnavailable: its node is not reachable.
	ReplicaUnavailable ReplicaState = "UNAVAILABLE"
)

// State is an object's durability state relative to its replication factor.
type State string

const (
	Healthy  State = "HEALTHY"  // verified replicas >= RF
	Degraded State = "DEGRADED" // 2 <= verified replicas < RF
	Critical State = "CRITICAL" // exactly 1 verified replica, RF > 1
	Lost     State = "LOST"     // no verified replica reachable
)

// Rank orders states by urgency: lower is more urgent.
func (s State) Rank() int {
	switch s {
	case Lost:
		return 0
	case Critical:
		return 1
	case Degraded:
		return 2
	default:
		return 3
	}
}

// Classify maps a verified-replica count to a State.
func Classify(healthy, replicationFactor int) State {
	switch {
	case healthy >= replicationFactor:
		return Healthy
	case healthy <= 0:
		return Lost
	case healthy == 1:
		return Critical
	default:
		return Degraded
	}
}

// NodeStateFunc reports the current state of a node.
type NodeStateFunc func(nodeID string) storage.NodeState

// ReplicaHealth is the assessed state of one replica.
type ReplicaHealth struct {
	NodeID     string            `json:"node_id"`
	NodeState  storage.NodeState `json:"node_state"`
	State      ReplicaState      `json:"replica_state"`
	Detail     string            `json:"detail,omitempty"`
	DetectedAt *time.Time        `json:"detected_at,omitempty"`
}

// ObjectHealth is the assessed durability of one object version.
type ObjectHealth struct {
	ObjectID          string          `json:"object_id"`
	Version           uint64          `json:"version"`
	Size              int64           `json:"size"`
	SHA256            string          `json:"sha256"`
	ReplicationFactor int             `json:"replication_factor"`
	HealthyReplicas   int             `json:"healthy_replicas"`
	State             State           `json:"state"`
	Reason            string          `json:"reason"`
	Replicas          []ReplicaHealth `json:"replicas"`
}

// NeedsRepair reports whether repair could improve this object: it is
// below policy but at least one verified replica can serve as a source.
func (h ObjectHealth) NeedsRepair() bool { return h.State == Degraded || h.State == Critical }

// HealthyNodes returns the nodes holding CURRENT replicas, in metadata order.
func (h ObjectHealth) HealthyNodes() []string {
	var out []string
	for _, r := range h.Replicas {
		if r.State == ReplicaCurrent {
			out = append(out, r.NodeID)
		}
	}
	return out
}

// Replica returns the assessed replica on nodeID.
func (h ObjectHealth) Replica(nodeID string) (ReplicaHealth, bool) {
	for _, r := range h.Replicas {
		if r.NodeID == nodeID {
			return r, true
		}
	}
	return ReplicaHealth{}, false
}

// AssessReplica determines the state of obj's replica on nodeID.
func AssessReplica(obj metadata.Object, nodeID string, nodeState NodeStateFunc) ReplicaHealth {
	r := ReplicaHealth{NodeID: nodeID, NodeState: nodeState(nodeID), State: ReplicaCurrent}
	if f, ok := obj.FailureFor(nodeID); ok {
		t := f.LastDetectedAt
		r.DetectedAt = &t
		switch f.Reason {
		case metadata.ReasonReplicaMissing:
			r.State = ReplicaMissing
			r.Detail = "replica not found on its node"
		case metadata.ReasonStaleVersion:
			r.State = ReplicaStale
			r.Detail = fmt.Sprintf("node holds v%d, current is v%d", f.ObservedVersion, obj.Version)
		default:
			r.State = ReplicaCorrupted
			r.Detail = f.Reason
		}
		return r
	}
	if r.NodeState != storage.StateHealthy {
		r.State = ReplicaUnavailable
		r.Detail = "node unavailable"
	}
	return r
}

// Assess computes obj's durability given current node states.
func Assess(obj metadata.Object, nodeState NodeStateFunc) ObjectHealth {
	h := ObjectHealth{
		ObjectID: obj.ID, Version: obj.Version, Size: obj.Size, SHA256: obj.SHA256,
		ReplicationFactor: obj.ReplicationFactor,
	}
	var problems []string
	for _, n := range obj.Replicas {
		r := AssessReplica(obj, n, nodeState)
		h.Replicas = append(h.Replicas, r)
		if r.State == ReplicaCurrent {
			h.HealthyReplicas++
		} else {
			problems = append(problems, fmt.Sprintf("%s %s", n, r.State))
		}
	}
	if missing := obj.ReplicationFactor - len(obj.Replicas); missing > 0 {
		problems = append(problems, fmt.Sprintf("%d replica slot(s) unassigned", missing))
	}
	h.State = Classify(h.HealthyReplicas, obj.ReplicationFactor)
	h.Reason = reason(h, problems)
	return h
}

func reason(h ObjectHealth, problems []string) string {
	detail := ""
	if len(problems) > 0 {
		detail = " (" + strings.Join(problems, ", ") + ")"
	}
	rf := h.ReplicationFactor
	switch h.State {
	case Healthy:
		return fmt.Sprintf("%d of %d verified replicas available for RF=%d.", h.HealthyReplicas, rf, rf)
	case Degraded:
		return fmt.Sprintf("%d of %d verified replicas remain for RF=%d%s.", h.HealthyReplicas, rf, rf, detail)
	case Critical:
		return fmt.Sprintf("Only 1 verified replica remains for RF=%d%s.", rf, detail)
	default:
		return fmt.Sprintf("No verified replica remains for RF=%d%s.", rf, detail)
	}
}

// Counts tallies objects by state.
type Counts struct {
	Total    int `json:"total"`
	Healthy  int `json:"healthy"`
	Degraded int `json:"degraded"`
	Critical int `json:"critical"`
	Lost     int `json:"lost"`
}

// Add counts one object in state s.
func (c *Counts) Add(s State) {
	c.Total++
	switch s {
	case Healthy:
		c.Healthy++
	case Degraded:
		c.Degraded++
	case Critical:
		c.Critical++
	case Lost:
		c.Lost++
	}
}

// SortBySeverity orders assessments most urgent first, then by fewest
// verified replicas, then by object ID.
func SortBySeverity(hs []ObjectHealth) {
	sort.SliceStable(hs, func(i, j int) bool {
		a, b := hs[i], hs[j]
		if a.State.Rank() != b.State.Rank() {
			return a.State.Rank() < b.State.Rank()
		}
		if a.HealthyReplicas != b.HealthyReplicas {
			return a.HealthyReplicas < b.HealthyReplicas
		}
		return a.ObjectID < b.ObjectID
	})
}

// AffectedObject is one object in a blast-radius report.
type AffectedObject struct {
	ObjectID          string `json:"object_id"`
	Version           uint64 `json:"version"`
	Size              int64  `json:"size"`
	ReplicationFactor int    `json:"replication_factor"`
	HealthyReplicas   int    `json:"healthy_replicas"`
	State             State  `json:"state"`
	Reason            string `json:"reason"`
}

// BlastRadius is the impact of losing one node, computed from metadata and
// the current state of every other node.
type BlastRadius struct {
	NodeID          string            `json:"node_id"`
	NodeState       storage.NodeState `json:"node_state"`
	ComputedAt      time.Time         `json:"computed_at"`
	AffectedObjects int               `json:"affected_objects"`
	AffectedBytes   int64             `json:"affected_bytes"`
	// Protected: affected objects that still meet their replication factor.
	Protected int              `json:"protected"`
	Degraded  int              `json:"degraded"`
	Critical  int              `json:"critical"`
	Lost      int              `json:"lost"`
	Objects   []AffectedObject `json:"objects"`
}

// ComputeBlastRadius reports every object with a replica on nodeID and how
// protected it is with nodeID treated as unavailable (whatever its actual
// state) and all other nodes in their current state.
func ComputeBlastRadius(objs []metadata.Object, nodeID string, nodeState NodeStateFunc, now time.Time) BlastRadius {
	without := func(id string) storage.NodeState {
		if id == nodeID {
			return storage.StateUnavailable
		}
		return nodeState(id)
	}
	br := BlastRadius{NodeID: nodeID, NodeState: nodeState(nodeID), ComputedAt: now}
	var hs []ObjectHealth
	for _, obj := range objs {
		if !slices.Contains(obj.Replicas, nodeID) {
			continue
		}
		h := Assess(obj, without)
		hs = append(hs, h)
		br.AffectedObjects++
		br.AffectedBytes += obj.Size
		switch h.State {
		case Healthy:
			br.Protected++
		case Degraded:
			br.Degraded++
		case Critical:
			br.Critical++
		case Lost:
			br.Lost++
		}
	}
	SortBySeverity(hs)
	br.Objects = make([]AffectedObject, len(hs))
	for i, h := range hs {
		br.Objects[i] = AffectedObject{
			ObjectID: h.ObjectID, Version: h.Version, Size: h.Size, ReplicationFactor: h.ReplicationFactor,
			HealthyReplicas: h.HealthyReplicas, State: h.State, Reason: h.Reason,
		}
	}
	return br
}
