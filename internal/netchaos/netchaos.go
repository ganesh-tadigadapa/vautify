// Package netchaos injects network faults on the coordinator's links to its
// storage nodes: partitions (the link is cut) and latency (every request on
// the link is delayed).
//
// Faults are injected in the coordinator's HTTP transport, below the node
// client, so every request the coordinator makes to a node experiences them:
// health probes, replica reads and writes, verifications, deletes, repair
// copies. Nothing above the transport knows a fault is simulated — a
// partitioned node is seen exactly as the real code sees any unreachable
// node, and the node process itself keeps running and serving anyone else.
//
// Vault has no node-to-node traffic: every link is coordinator↔node. A
// partition is therefore expressed as the set of nodes cut off from the
// coordinator.
//
// A partition can also be armed to start in the middle of a transfer
// (after N bytes of an upload or download, or after a node has handled a
// request but before its response arrives), which makes "partition during
// write" and "partition during read" deterministic.
package netchaos

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"vault/internal/events"
)

// bg is the context for events the injector emits on its own behalf.
var bg = context.Background()

// ErrUnknownNode: the injector manages no link to that node.
var ErrUnknownNode = errors.New("netchaos: unknown node")

// PartitionError is the transport error seen on a cut link. It reads like
// a network failure because, to everything above the transport, it is one.
type PartitionError struct {
	NodeID string
	// Phase: "connect" (the link was already cut), "upload"/"download"
	// (an in-flight transfer was cut), "ack" (the node handled the request
	// but its response was lost).
	Phase string
}

func (e *PartitionError) Error() string {
	switch e.Phase {
	case "connect":
		return fmt.Sprintf("network partition (injected): no route from coordinator to %s", e.NodeID)
	case "ack":
		return fmt.Sprintf("network partition (injected): response from %s lost after the request was delivered", e.NodeID)
	default:
		return fmt.Sprintf("network partition (injected): %s to %s cut mid-transfer", e.Phase, e.NodeID)
	}
}

// Timeout reports true so the error behaves like a network timeout.
func (e *PartitionError) Timeout() bool { return true }

// Trigger points for an armed partition.
const (
	// PointUpload: the partition starts once AfterBytes of a request body
	// have been sent to the node; the rest of the upload never arrives.
	PointUpload = "upload"
	// PointAck: the partition starts after the node has fully handled the
	// request, before its response reaches the coordinator.
	PointAck = "ack"
	// PointDownload: the partition starts once AfterBytes of a response
	// body have been received from the node.
	PointDownload = "download"
)

// Trigger arms a partition that starts during the next matching replica
// transfer (a request under /replicas/). It fires once.
type Trigger struct {
	Point      string `json:"point"`
	AfterBytes int64  `json:"after_bytes,omitempty"`
	// Method restricts the trigger to one HTTP method ("" = any).
	Method string `json:"method,omitempty"`
}

func (t Trigger) validate() error {
	switch t.Point {
	case PointUpload, PointAck, PointDownload:
	default:
		return fmt.Errorf("netchaos: trigger point must be %s, %s or %s, got %q", PointUpload, PointAck, PointDownload, t.Point)
	}
	if t.AfterBytes < 0 {
		return errors.New("netchaos: after_bytes must be >= 0")
	}
	return nil
}

// LinkStatus is the current fault state of one coordinator→node link.
type LinkStatus struct {
	NodeID      string     `json:"node_id"`
	Partitioned bool       `json:"partitioned"`
	Since       *time.Time `json:"partitioned_since,omitempty"`
	// HealAt is set for a temporary partition that heals by itself.
	HealAt    *time.Time `json:"heal_at,omitempty"`
	LatencyMS int64      `json:"latency_ms"`
	Armed     *Trigger   `json:"armed_trigger,omitempty"`
	// Counters since the injector started.
	RequestsBlocked int64 `json:"requests_blocked"`
	TransfersCut    int64 `json:"transfers_cut"`
	RequestsDelayed int64 `json:"requests_delayed"`
	Partitions      int64 `json:"partitions_total"`
}

type link struct {
	partitioned bool
	since       time.Time
	cut         chan struct{} // closed when a partition starts
	latency     time.Duration
	trigger     *Trigger
	healTimer   *time.Timer
	healAt      time.Time

	blocked, cutCount, delayed, partitions int64
}

// Injector holds the fault state of every link. It is safe for concurrent
// use. The zero value is not usable; call New.
type Injector struct {
	log   *events.Logger
	mu    sync.Mutex
	links map[string]*link
	order []string
}

// New returns an injector managing links to the given nodes, all healthy.
func New(log *events.Logger, nodeIDs []string) *Injector {
	i := &Injector{log: log, links: map[string]*link{}}
	for _, id := range nodeIDs {
		if _, dup := i.links[id]; dup {
			continue
		}
		i.links[id] = &link{cut: make(chan struct{})}
		i.order = append(i.order, id)
	}
	return i
}

func (i *Injector) get(nodeID string) (*link, error) {
	l, ok := i.links[nodeID]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnknownNode, nodeID)
	}
	return l, nil
}

// Partition cuts the link to nodeID now: new requests fail and in-flight
// transfers break. With d > 0 the partition heals by itself after d.
func (i *Injector) Partition(nodeID string, d time.Duration) error {
	i.mu.Lock()
	l, err := i.get(nodeID)
	if err != nil {
		i.mu.Unlock()
		return err
	}
	i.startLocked(nodeID, l, "manual")
	if l.healTimer != nil {
		l.healTimer.Stop()
		l.healTimer, l.healAt = nil, time.Time{}
	}
	if d > 0 {
		l.healAt = time.Now().UTC().Add(d)
		l.healTimer = time.AfterFunc(d, func() { i.heal(nodeID, "timer") })
	}
	i.mu.Unlock()
	return nil
}

// startLocked begins a partition on l. Caller holds i.mu.
func (i *Injector) startLocked(nodeID string, l *link, cause string) {
	if l.partitioned {
		return
	}
	l.partitioned, l.since = true, time.Now().UTC()
	l.partitions++
	close(l.cut)
	i.log.Warn(bg, events.NetworkPartitionStarted, "node_id", nodeID, "cause", cause,
		"note", "coordinator→node link cut (injected); the node process keeps running")
}

// Arm makes a partition start during the next replica transfer matching t.
func (i *Injector) Arm(nodeID string, t Trigger) error {
	if err := t.validate(); err != nil {
		return err
	}
	t.Method = strings.ToUpper(t.Method)
	i.mu.Lock()
	defer i.mu.Unlock()
	l, err := i.get(nodeID)
	if err != nil {
		return err
	}
	l.trigger = &t
	i.log.Warn(bg, events.NetworkPartitionArmed, "node_id", nodeID, "point", t.Point,
		"after_bytes", t.AfterBytes, "method", t.Method)
	return nil
}

// Heal restores the link to nodeID and disarms any trigger. Latency is left
// as configured.
func (i *Injector) Heal(nodeID string) error {
	i.mu.Lock()
	_, err := i.get(nodeID)
	i.mu.Unlock()
	if err != nil {
		return err
	}
	i.heal(nodeID, "manual")
	return nil
}

func (i *Injector) heal(nodeID, cause string) {
	i.mu.Lock()
	l := i.links[nodeID]
	if l.healTimer != nil {
		l.healTimer.Stop()
		l.healTimer, l.healAt = nil, time.Time{}
	}
	l.trigger = nil
	was, since := l.partitioned, l.since
	if was {
		l.partitioned, l.since = false, time.Time{}
		l.cut = make(chan struct{})
	}
	i.mu.Unlock()
	if was {
		i.log.Emit(bg, events.NetworkPartitionHealed, "node_id", nodeID, "cause", cause,
			"partitioned_ms", time.Since(since).Milliseconds())
	}
}

// HealAll restores every link and clears all latency.
func (i *Injector) HealAll() {
	for _, id := range i.order {
		i.heal(id, "manual")
		_ = i.SetLatency(id, 0)
	}
}

// SetLatency delays every request on the link to nodeID by d (0 clears).
func (i *Injector) SetLatency(nodeID string, d time.Duration) error {
	if d < 0 {
		return errors.New("netchaos: latency must be >= 0")
	}
	i.mu.Lock()
	l, err := i.get(nodeID)
	if err != nil {
		i.mu.Unlock()
		return err
	}
	changed := l.latency != d
	l.latency = d
	i.mu.Unlock()
	if changed {
		i.log.Warn(bg, events.NetworkLatencyChanged, "node_id", nodeID, "latency_ms", d.Milliseconds())
	}
	return nil
}

// Status returns every link's fault state, in configuration order.
func (i *Injector) Status() []LinkStatus {
	i.mu.Lock()
	defer i.mu.Unlock()
	out := make([]LinkStatus, 0, len(i.order))
	for _, id := range i.order {
		out = append(out, i.statusLocked(id))
	}
	return out
}

// Link returns the fault state of the link to nodeID.
func (i *Injector) Link(nodeID string) (LinkStatus, bool) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if _, ok := i.links[nodeID]; !ok {
		return LinkStatus{}, false
	}
	return i.statusLocked(nodeID), true
}

func (i *Injector) statusLocked(id string) LinkStatus {
	l := i.links[id]
	s := LinkStatus{NodeID: id, Partitioned: l.partitioned, LatencyMS: l.latency.Milliseconds(),
		RequestsBlocked: l.blocked, TransfersCut: l.cutCount, RequestsDelayed: l.delayed, Partitions: l.partitions}
	if l.partitioned {
		t := l.since
		s.Since = &t
	}
	if !l.healAt.IsZero() {
		t := l.healAt
		s.HealAt = &t
	}
	if l.trigger != nil {
		t := *l.trigger
		s.Armed = &t
	}
	return s
}

// admit reports whether a request may start on the link, returning the
// link's latency and the channel that closes if a partition begins.
func (i *Injector) admit(nodeID string) (time.Duration, <-chan struct{}, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	l := i.links[nodeID]
	if l.partitioned {
		l.blocked++
		return 0, nil, &PartitionError{NodeID: nodeID, Phase: "connect"}
	}
	if l.latency > 0 {
		l.delayed++
	}
	return l.latency, l.cut, nil
}

// takeTrigger consumes the link's trigger if it matches req.
func (i *Injector) takeTrigger(nodeID string, req *http.Request) *Trigger {
	i.mu.Lock()
	defer i.mu.Unlock()
	l := i.links[nodeID]
	t := l.trigger
	if t == nil || !strings.HasPrefix(req.URL.Path, "/replicas/") || (t.Method != "" && t.Method != req.Method) {
		return nil
	}
	l.trigger = nil
	return t
}

func (i *Injector) fire(nodeID string, t *Trigger) {
	i.mu.Lock()
	i.startLocked(nodeID, i.links[nodeID], "trigger:"+t.Point)
	i.mu.Unlock()
}

func (i *Injector) countCut(nodeID, phase string) {
	i.mu.Lock()
	i.links[nodeID].cutCount++
	i.mu.Unlock()
	i.log.Warn(bg, events.NetworkTransferCut, "node_id", nodeID, "phase", phase)
}

// Transport returns a RoundTripper for requests to nodeID that applies the
// link's faults and forwards everything else to base. A nodeID the injector
// does not manage gets base unchanged.
func (i *Injector) Transport(nodeID string, base http.RoundTripper) http.RoundTripper {
	if i == nil {
		return base
	}
	if _, ok := i.links[nodeID]; !ok {
		return base
	}
	return &transport{inj: i, node: nodeID, base: base}
}

// NodeIDs returns the managed nodes in configuration order.
func (i *Injector) NodeIDs() []string { return slices.Clone(i.order) }

type transport struct {
	inj  *Injector
	node string
	base http.RoundTripper
}

func closed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func (t *transport) RoundTrip(req *http.Request) (*http.Response, error) {
	latency, cut, err := t.inj.admit(t.node)
	if err != nil {
		closeBody(req)
		return nil, err
	}
	if latency > 0 {
		timer := time.NewTimer(latency)
		select {
		case <-timer.C:
		case <-cut:
			timer.Stop()
			closeBody(req)
			return nil, &PartitionError{NodeID: t.node, Phase: "connect"}
		case <-req.Context().Done():
			timer.Stop()
			closeBody(req)
			return nil, req.Context().Err()
		}
	}
	trig := t.inj.takeTrigger(t.node, req)
	if trig != nil && trig.Point == PointUpload && (req.Body == nil || req.Body == http.NoBody) {
		trig = nil // nothing to cut; leave the partition unarmed
	}

	out := req
	if req.Body != nil && req.Body != http.NoBody {
		out = req.Clone(req.Context())
		out.GetBody = nil // a transport retry must not resend the unwrapped body
		b := &cutBody{rc: req.Body, t: t, cut: cut, phase: PointUpload, remaining: -1}
		if trig != nil && trig.Point == PointUpload {
			b.trigger, b.remaining = trig, trig.AfterBytes
		}
		out.Body = b
	}
	resp, err := t.base.RoundTrip(out)
	if err != nil {
		if closed(cut) {
			return nil, &PartitionError{NodeID: t.node, Phase: PointUpload}
		}
		return nil, err
	}
	if trig != nil && trig.Point == PointAck {
		t.inj.fire(t.node, trig)
	}
	if closed(cut) {
		// The partition began while the node was handling the request: the
		// node may have acted on it, but its answer never arrives.
		resp.Body.Close()
		t.inj.countCut(t.node, PointAck)
		return nil, &PartitionError{NodeID: t.node, Phase: PointAck}
	}
	b := &cutBody{rc: resp.Body, t: t, cut: cut, phase: PointDownload, remaining: -1}
	if trig != nil && trig.Point == PointDownload {
		b.trigger, b.remaining = trig, trig.AfterBytes
	}
	resp.Body = b
	return resp, nil
}

func closeBody(req *http.Request) {
	if req.Body != nil {
		req.Body.Close()
	}
}

// cutBody passes bytes through until the link's partition begins, then
// fails every further read like a reset connection. With a trigger it
// starts the partition itself after exactly remaining bytes.
type cutBody struct {
	rc        io.ReadCloser
	t         *transport
	cut       <-chan struct{}
	phase     string
	trigger   *Trigger
	remaining int64 // bytes until the trigger fires; -1 = no trigger
	failed    bool
}

func (b *cutBody) fail() (int, error) {
	if !b.failed {
		b.failed = true
		b.t.inj.countCut(b.t.node, b.phase)
	}
	return 0, &PartitionError{NodeID: b.t.node, Phase: b.phase}
}

func (b *cutBody) Read(p []byte) (int, error) {
	if b.remaining == 0 && b.trigger != nil {
		b.t.inj.fire(b.t.node, b.trigger)
		b.trigger = nil
	}
	if closed(b.cut) {
		return b.fail()
	}
	if b.remaining > 0 && int64(len(p)) > b.remaining {
		p = p[:b.remaining]
	}
	n, err := b.rc.Read(p)
	if b.remaining > 0 {
		b.remaining -= int64(n)
	}
	if err == io.EOF && b.remaining == 0 && b.trigger != nil {
		// The body ended exactly at the trigger point: nothing left to cut.
		b.trigger = nil
	}
	return n, err
}

func (b *cutBody) Close() error { return b.rc.Close() }
