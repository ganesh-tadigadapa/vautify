package coordinator

import (
	"context"
	"slices"
	"sort"
	"sync"
	"time"

	"vault/internal/durability"
	"vault/internal/metadata"
	"vault/internal/storage"
)

// resilienceStats are counters for Phase 4 operations since start.
type resilienceStats struct {
	mu             sync.Mutex
	removed        map[string]int64
	refused        int64
	reconciles     int64
	adopted        int64
	adoptedBytes   int64
	reclaimedBytes int64
	lastReconcile  map[string]ReconcileReport
}

func (s *resilienceStats) init() {
	if s.removed == nil {
		s.removed = map[string]int64{}
		s.lastReconcile = map[string]ReconcileReport{}
	}
}

func (c *Coordinator) noteRemoval(class string, removed bool) {
	c.stats.mu.Lock()
	defer c.stats.mu.Unlock()
	c.stats.init()
	if removed {
		c.stats.removed[class]++
	} else {
		c.stats.refused++
	}
}

func (c *Coordinator) noteReconcile(rep ReconcileReport) {
	c.stats.mu.Lock()
	defer c.stats.mu.Unlock()
	c.stats.init()
	c.stats.reconciles++
	c.stats.adopted += int64(rep.Adopted)
	c.stats.adoptedBytes += rep.AdoptedBytes
	c.stats.reclaimedBytes += rep.ReclaimedBytes
	if len(rep.Actions) > 200 {
		rep.Actions = rep.Actions[:200]
	}
	c.stats.lastReconcile[rep.NodeID] = rep
}

// LastReconciles returns the most recent reconcile report per node.
func (c *Coordinator) LastReconciles() []ReconcileReport {
	c.stats.mu.Lock()
	defer c.stats.mu.Unlock()
	out := make([]ReconcileReport, 0, len(c.stats.lastReconcile))
	for _, r := range c.stats.lastReconcile {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].NodeID < out[j].NodeID })
	return out
}

// ByteCount is a number of replica files and their bytes.
type ByteCount struct {
	Replicas int   `json:"replicas"`
	Bytes    int64 `json:"bytes"`
}

func (b *ByteCount) add(size int64) {
	b.Replicas++
	b.Bytes += size
}

// PhysicalBreakdown classifies the bytes found on reachable nodes against
// the latest metadata record of each object.
type PhysicalBreakdown struct {
	// CurrentListed: the current version on a node metadata lists.
	CurrentListed ByteCount `json:"current_listed"`
	// CurrentUnlisted: a copy of the current version on a node not listed
	// (an adoption or extra-copy candidate for reconciliation).
	CurrentUnlisted ByteCount `json:"current_unlisted"`
	// Stale: versions older than the current live version.
	Stale ByteCount `json:"stale"`
	// DeletedObject: any version of an object whose latest record is a tombstone.
	DeletedObject ByteCount `json:"deleted_object"`
	// Uncommitted: versions newer than every committed record.
	Uncommitted ByteCount `json:"uncommitted"`
	// Untracked: bytes of object IDs with no metadata record at all.
	Untracked ByteCount `json:"untracked"`
}

// NodeStorage is what one node physically holds.
type NodeStorage struct {
	NodeID    string            `json:"node_id"`
	State     storage.NodeState `json:"state"`
	Reachable bool              `json:"reachable"`
	// Replicas and Bytes are summed from the node's own inventory (file
	// sizes on its disk); nil when the node could not be reached.
	Replicas *int   `json:"replicas"`
	Bytes    *int64 `json:"bytes"`
	// ReportedUsedBytes, TotalBytes and FreeBytes come from the node's last
	// successful health probe.
	ReportedUsedBytes *int64  `json:"reported_used_bytes,omitempty"`
	TotalBytes        *uint64 `json:"fs_total_bytes,omitempty"`
	FreeBytes         *uint64 `json:"fs_free_bytes,omitempty"`
	Error             string  `json:"error,omitempty"`
}

// RemovalTotals counts guarded removals since start.
type RemovalTotals struct {
	ByClass map[string]int64 `json:"by_class"`
	Refused int64            `json:"refused"`
}

// ReconcileTotals counts reconciliation since start.
type ReconcileTotals struct {
	Runs           int64 `json:"runs"`
	Adopted        int64 `json:"replicas_adopted"`
	AdoptedBytes   int64 `json:"adopted_bytes_without_transfer"`
	ReclaimedBytes int64 `json:"reclaimed_bytes"`
}

// StorageMetrics is the cluster's storage footprint, measured: logical
// bytes from metadata, physical bytes from every node's inventory of its
// own disk. When any node cannot be reached, totals that need it are null
// and Complete is false; nothing is estimated.
type StorageMetrics struct {
	GeneratedAt      time.Time `json:"generated_at"`
	Complete         bool      `json:"complete"`
	UnreachableNodes []string  `json:"unreachable_nodes"`
	Note             string    `json:"note,omitempty"`

	LiveObjects int `json:"live_objects"`
	Tombstones  int `json:"tombstones"`
	// LogicalBytes: sum of the current version's size over live objects.
	LogicalBytes int64 `json:"logical_bytes"`
	// ExpectedReplicas/ExpectedPhysicalBytes: what the replication policy
	// asks for (sum of RF, sum of size × RF).
	ExpectedReplicas      int   `json:"expected_replicas"`
	ExpectedPhysicalBytes int64 `json:"expected_physical_bytes"`
	// ListedReplicas/ListedBytes: replicas metadata names.
	ListedReplicas int   `json:"listed_replicas"`
	ListedBytes    int64 `json:"listed_bytes"`

	// PhysicalReplicas/PhysicalBytes: every replica file on every node.
	// Null unless every node was reachable.
	PhysicalReplicas *int   `json:"physical_replicas"`
	PhysicalBytes    *int64 `json:"physical_bytes"`
	// ...Reachable: the same sums over the nodes that answered (a lower
	// bound when incomplete).
	PhysicalReplicasReachable int   `json:"physical_replicas_reachable"`
	PhysicalBytesReachable    int64 `json:"physical_bytes_reachable"`

	// StorageOverhead = physical_bytes / logical_bytes (null if unknown or
	// no logical bytes). PolicyOverhead = expected_physical / logical.
	StorageOverhead *float64 `json:"storage_overhead"`
	PolicyOverhead  *float64 `json:"policy_overhead"`

	Breakdown  PhysicalBreakdown `json:"breakdown_reachable"`
	Durability durability.Counts `json:"durability"`
	Nodes      []NodeStorage     `json:"nodes"`
	Removals   RemovalTotals     `json:"guarded_removals"`
	Reconcile  ReconcileTotals   `json:"reconcile"`
}

// StorageMetrics measures the cluster's storage now. It is a point-in-time
// reading, not an atomic snapshot across nodes: writes that land while it
// runs may be counted on some nodes and not others.
func (c *Coordinator) StorageMetrics(ctx context.Context) (StorageMetrics, error) {
	m := StorageMetrics{GeneratedAt: time.Now().UTC(), UnreachableNodes: []string{}}
	records, err := c.meta.ListRecords(ctx)
	if err != nil {
		return m, err
	}
	byID := make(map[string]metadata.Object, len(records))
	for _, r := range records {
		byID[r.ID] = r
		if r.Deleted {
			m.Tombstones++
			continue
		}
		m.LiveObjects++
		m.LogicalBytes += r.Size
		m.ExpectedReplicas += r.ReplicationFactor
		m.ExpectedPhysicalBytes += r.Size * int64(r.ReplicationFactor)
		m.ListedReplicas += len(r.Replicas)
		m.ListedBytes += r.Size * int64(len(r.Replicas))
		m.Durability.Add(durability.Assess(r, c.health.State).State)
	}

	type result struct {
		inv []storage.ReplicaInfo
		err error
	}
	results := make([]result, len(c.order))
	var wg sync.WaitGroup
	for i, id := range c.order {
		if c.health.State(id) != storage.StateHealthy {
			results[i].err = storage.ErrUnavailable
			continue
		}
		wg.Add(1)
		go func(i int, id string) {
			defer wg.Done()
			ictx, cancel := context.WithTimeout(ctx, c.cfg.ReplicaTimeout)
			defer cancel()
			inv, err := c.nodes[id].Inventory(ictx)
			if err != nil {
				c.reportIfUnavailable(ctx, id, err)
			}
			results[i] = result{inv, err}
		}(i, id)
	}
	wg.Wait()

	snap := map[string]storage.Capacity{}
	states := map[string]storage.NodeState{}
	for _, s := range c.health.Snapshot() {
		states[s.ID] = s.State
		if s.Capacity != nil {
			snap[s.ID] = *s.Capacity
		}
	}
	for i, id := range c.order {
		ns := NodeStorage{NodeID: id, State: states[id]}
		if capy, ok := snap[id]; ok {
			used, total, free := capy.UsedBytes, capy.TotalBytes, capy.FreeBytes
			ns.ReportedUsedBytes, ns.TotalBytes, ns.FreeBytes = &used, &total, &free
		}
		r := results[i]
		if r.err != nil {
			ns.Error = r.err.Error()
			m.UnreachableNodes = append(m.UnreachableNodes, id)
			m.Nodes = append(m.Nodes, ns)
			continue
		}
		ns.Reachable = true
		var count int
		var bytes int64
		for _, rep := range r.inv {
			count++
			bytes += rep.Size
			rec, ok := byID[rep.ObjectID]
			switch {
			case !ok:
				m.Breakdown.Untracked.add(rep.Size)
			case rec.Deleted:
				m.Breakdown.DeletedObject.add(rep.Size)
			case rep.Version < rec.Version:
				m.Breakdown.Stale.add(rep.Size)
			case rep.Version > rec.Version:
				m.Breakdown.Uncommitted.add(rep.Size)
			case slices.Contains(rec.Replicas, id):
				m.Breakdown.CurrentListed.add(rep.Size)
			default:
				m.Breakdown.CurrentUnlisted.add(rep.Size)
			}
		}
		ns.Replicas, ns.Bytes = &count, &bytes
		m.PhysicalReplicasReachable += count
		m.PhysicalBytesReachable += bytes
		m.Nodes = append(m.Nodes, ns)
	}
	m.Complete = len(m.UnreachableNodes) == 0
	if m.Complete {
		pr, pb := m.PhysicalReplicasReachable, m.PhysicalBytesReachable
		m.PhysicalReplicas, m.PhysicalBytes = &pr, &pb
		if m.LogicalBytes > 0 {
			o := float64(pb) / float64(m.LogicalBytes)
			m.StorageOverhead = &o
		}
	} else {
		m.Note = "physical totals and overhead are unknown while nodes are unreachable; *_reachable fields are lower bounds"
	}
	if m.LogicalBytes > 0 {
		o := float64(m.ExpectedPhysicalBytes) / float64(m.LogicalBytes)
		m.PolicyOverhead = &o
	}

	c.stats.mu.Lock()
	c.stats.init()
	m.Removals = RemovalTotals{ByClass: map[string]int64{}, Refused: c.stats.refused}
	for k, v := range c.stats.removed {
		m.Removals.ByClass[k] = v
	}
	m.Reconcile = ReconcileTotals{Runs: c.stats.reconciles, Adopted: c.stats.adopted,
		AdoptedBytes: c.stats.adoptedBytes, ReclaimedBytes: c.stats.reclaimedBytes}
	c.stats.mu.Unlock()
	return m, nil
}
