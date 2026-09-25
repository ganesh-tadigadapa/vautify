// Package placement chooses which storage nodes hold an object's replicas.
//
// Stage 1 uses rendezvous (highest-random-weight) hashing over the currently
// healthy nodes: deterministic for a given object and node set, spreads
// objects across nodes, and needs no shared state. It is not failure-domain
// aware and not capacity aware.
package placement

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
)

// InsufficientNodesError means the replication policy cannot be satisfied by
// the nodes currently available.
type InsufficientNodesError struct {
	Required  int
	Available int
}

func (e *InsufficientNodesError) Error() string {
	return fmt.Sprintf("replication factor %d requires %d healthy nodes, only %d available",
		e.Required, e.Required, e.Available)
}

// Plan is the outcome of placing one object.
type Plan struct {
	// Targets are the nodes that should receive replicas, most preferred first.
	Targets []string
	// Spares are further healthy nodes, in preference order, that may be used
	// if writing to a target fails.
	Spares []string
}

// Manager decides replica placement.
type Manager interface {
	Place(objectID string, healthy []string, replicationFactor int) (Plan, error)
}

// Rendezvous is a Manager using rendezvous hashing.
type Rendezvous struct{}

var _ Manager = Rendezvous{}

// Place ranks the distinct healthy nodes for objectID and splits them into
// replicationFactor targets plus spares. It never returns fewer targets than
// requested: if that is impossible it returns *InsufficientNodesError.
func (Rendezvous) Place(objectID string, healthy []string, replicationFactor int) (Plan, error) {
	if replicationFactor < 1 {
		return Plan{}, fmt.Errorf("replication factor must be >= 1, got %d", replicationFactor)
	}
	ranked := Rank(objectID, healthy)
	if len(ranked) < replicationFactor {
		return Plan{}, &InsufficientNodesError{Required: replicationFactor, Available: len(ranked)}
	}
	return Plan{Targets: ranked[:replicationFactor], Spares: ranked[replicationFactor:]}, nil
}

// Rank returns the distinct node IDs ordered by rendezvous weight for key.
func Rank(key string, nodeIDs []string) []string {
	seen := make(map[string]bool, len(nodeIDs))
	type scored struct {
		id    string
		score uint64
	}
	var s []scored
	for _, id := range nodeIDs {
		if seen[id] {
			continue
		}
		seen[id] = true
		h := sha256.Sum256([]byte(key + "\x00" + id))
		s = append(s, scored{id, binary.BigEndian.Uint64(h[:8])})
	}
	sort.Slice(s, func(i, j int) bool {
		if s[i].score != s[j].score {
			return s[i].score > s[j].score
		}
		return s[i].id < s[j].id
	})
	out := make([]string, len(s))
	for i, x := range s {
		out[i] = x.id
	}
	return out
}
