package repair

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"vault/internal/durability"
	"vault/internal/events"
)

type fakeExec struct {
	mu      sync.Mutex
	order   []string
	calls   map[string]int
	active  map[string]int
	maxSame int
	running atomic.Int32
	maxAll  atomic.Int32
	fn      func(t Task) (Result, error)
}

func newFake(fn func(Task) (Result, error)) *fakeExec {
	return &fakeExec{calls: map[string]int{}, active: map[string]int{}, fn: fn}
}

func (f *fakeExec) Repair(_ context.Context, t Task) (Result, error) {
	f.mu.Lock()
	f.order = append(f.order, t.ObjectID)
	f.calls[t.ObjectID]++
	f.active[t.ObjectID]++
	if f.active[t.ObjectID] > f.maxSame {
		f.maxSame = f.active[t.ObjectID]
	}
	f.mu.Unlock()
	n := f.running.Add(1)
	for {
		m := f.maxAll.Load()
		if n <= m || f.maxAll.CompareAndSwap(m, n) {
			break
		}
	}
	res, err := f.fn(t)
	f.running.Add(-1)
	f.mu.Lock()
	f.active[t.ObjectID]--
	f.mu.Unlock()
	return res, err
}

func restored(t Task) (Result, error) {
	return Result{ObjectID: t.ObjectID, Restored: true, StateAfter: durability.Healthy, BytesTransferred: 10, Targets: []string{"n"}}, nil
}

func assess(id string, healthy, rf int) durability.ObjectHealth {
	return durability.ObjectHealth{
		ObjectID: id, Version: 1, ReplicationFactor: rf, HealthyReplicas: healthy,
		State: durability.Classify(healthy, rf), Reason: fmt.Sprintf("%d/%d", healthy, rf),
	}
}

func startManager(t *testing.T, exec Executor, cfg Config) (*Manager, func()) {
	t.Helper()
	m := NewManager(exec, cfg, events.Discard())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { m.Run(ctx); close(done) }()
	return m, func() { cancel(); <-done }
}

func waitIdle(t *testing.T, m *Manager) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !m.Idle() {
		if time.Now().After(deadline) {
			t.Fatalf("manager not idle: %+v", m.Snapshot())
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// A CRITICAL object is repaired before a DEGRADED one even when the
// DEGRADED one was queued first and sorts first by ID.
func TestCriticalBeforeDegradedRegardlessOfFIFOAndID(t *testing.T) {
	ctx := context.Background()
	exec := newFake(restored)
	m, stop := startManager(t, exec, Config{Workers: 1})
	defer stop()
	m.Pause()

	m.Enqueue(ctx, assess("aaa-degraded", 2, 3), time.Time{})
	time.Sleep(2 * time.Millisecond)
	m.Enqueue(ctx, assess("mmm-degraded", 2, 3), time.Time{})
	m.Enqueue(ctx, assess("zzz-critical", 1, 3), time.Time{})

	snap := m.Snapshot()
	got := []string{snap.Queued[0].ObjectID, snap.Queued[1].ObjectID, snap.Queued[2].ObjectID}
	want := []string{"zzz-critical", "aaa-degraded", "mmm-degraded"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("queue order %v, want %v", got, want)
	}
	if snap.Queued[0].Priority != durability.Critical || snap.Queued[0].Reason == "" {
		t.Fatalf("priority/reason not exposed: %+v", snap.Queued[0])
	}

	m.Resume()
	waitIdle(t, m)
	exec.mu.Lock()
	defer exec.mu.Unlock()
	if fmt.Sprint(exec.order) != fmt.Sprint(want) {
		t.Fatalf("execution order %v, want %v", exec.order, want)
	}
}

// Priority escalation of an already-queued object moves it forward.
func TestEscalationReordersQueue(t *testing.T) {
	ctx := context.Background()
	m, stop := startManager(t, newFake(restored), Config{Workers: 1})
	defer stop()
	m.Pause()
	m.Enqueue(ctx, assess("a", 2, 3), time.Time{})
	m.Enqueue(ctx, assess("b", 2, 3), time.Time{})
	if out := m.Enqueue(ctx, assess("b", 1, 3), time.Time{}); out != OutcomeUpdated {
		t.Fatalf("escalation outcome %s", out)
	}
	if q := m.Snapshot().Queued; q[0].ObjectID != "b" || q[0].Priority != durability.Critical || len(q) != 2 {
		t.Fatalf("queue %+v", q)
	}
}

// Duplicate requests never create a second queue entry or a concurrent
// second repair of the same object.
func TestDuplicateRequestsAreDeduplicated(t *testing.T) {
	ctx := context.Background()
	release := make(chan struct{})
	started := make(chan struct{}, 10)
	exec := newFake(func(t Task) (Result, error) {
		started <- struct{}{}
		<-release
		return restored(t)
	})
	m, stop := startManager(t, exec, Config{Workers: 8})
	defer stop()

	h := assess("obj", 2, 3)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); m.Enqueue(ctx, h, time.Time{}) }()
	}
	wg.Wait()
	<-started
	// More duplicates while the repair is running.
	for i := 0; i < 50; i++ {
		if out := m.Enqueue(ctx, h, time.Time{}); out != OutcomeActive {
			t.Fatalf("enqueue during repair = %s", out)
		}
	}
	close(release)
	waitIdle(t, m)

	exec.mu.Lock()
	defer exec.mu.Unlock()
	if exec.calls["obj"] != 1 || exec.maxSame != 1 {
		t.Fatalf("calls=%d maxConcurrentSameObject=%d; want 1/1", exec.calls["obj"], exec.maxSame)
	}
	if d := m.Metrics().Deduplicated; d != 99 {
		t.Fatalf("deduplicated = %d, want 99", d)
	}
}

func TestDifferentObjectsRepairConcurrently(t *testing.T) {
	ctx := context.Background()
	exec := newFake(func(t Task) (Result, error) {
		time.Sleep(40 * time.Millisecond)
		return restored(t)
	})
	m, stop := startManager(t, exec, Config{Workers: 4})
	defer stop()
	m.Pause()
	for i := 0; i < 8; i++ {
		m.Enqueue(ctx, assess(fmt.Sprintf("o-%d", i), 2, 3), time.Time{})
	}
	m.Resume()
	waitIdle(t, m)
	if got := exec.maxAll.Load(); got != 4 {
		t.Fatalf("max concurrent repairs = %d, want exactly the 4 workers", got)
	}
	if mt := m.Metrics(); mt.Completed != 8 || mt.BytesTransferred != 80 || mt.ReplicasCreated != 8 || mt.RecoveryTime.Count != 8 {
		t.Fatalf("metrics %+v", mt)
	}
}

func TestRetryBackoffThenParkThenTopologyChange(t *testing.T) {
	ctx := context.Background()
	var fail atomic.Bool
	fail.Store(true)
	exec := newFake(func(t Task) (Result, error) {
		if fail.Load() {
			return Result{ObjectID: t.ObjectID, StateAfter: durability.Degraded, HealthyAfter: 2}, errors.New("no eligible target")
		}
		return restored(t)
	})
	m, stop := startManager(t, exec, Config{Workers: 2, MaxAttempts: 3, BaseBackoff: 5 * time.Millisecond, MaxBackoff: 20 * time.Millisecond})
	defer stop()

	m.Enqueue(ctx, assess("obj", 2, 3), time.Time{})
	waitIdle(t, m)
	mt := m.Metrics()
	if mt.Failed != 1 || mt.Retries != 2 || mt.Started != 3 || mt.Completed != 0 || mt.Parked != 1 {
		t.Fatalf("metrics after exhausting attempts: %+v", mt)
	}
	snap := m.Snapshot()
	if len(snap.Failed) != 1 || snap.Failed[0].Task.LastError != "no eligible target" {
		t.Fatalf("failed = %+v", snap.Failed)
	}
	if out := m.Enqueue(ctx, assess("obj", 2, 3), time.Time{}); out != OutcomeParked {
		t.Fatalf("parked object re-enqueued without topology change: %s", out)
	}

	fail.Store(false)
	m.TopologyChanged()
	if out := m.Enqueue(ctx, assess("obj", 2, 3), time.Time{}); out != OutcomeQueued {
		t.Fatalf("after topology change: %s", out)
	}
	waitIdle(t, m)
	if mt := m.Metrics(); mt.Completed != 1 {
		t.Fatalf("not repaired after capacity returned: %+v", mt)
	}
}

func TestSupersededAndNoOpAreDropped(t *testing.T) {
	ctx := context.Background()
	exec := newFake(func(t Task) (Result, error) {
		if t.ObjectID == "gone" {
			return Result{}, fmt.Errorf("%w: deleted", ErrSuperseded)
		}
		return Result{ObjectID: t.ObjectID, NoOp: true, Restored: true}, nil
	})
	m, stop := startManager(t, exec, Config{Workers: 1})
	defer stop()
	m.Enqueue(ctx, assess("gone", 2, 3), time.Time{})
	m.Enqueue(ctx, assess("fine", 2, 3), time.Time{})
	waitIdle(t, m)
	mt := m.Metrics()
	if mt.Superseded != 1 || mt.NoOp != 1 || mt.Retries != 0 || mt.Completed != 0 {
		t.Fatalf("%+v", mt)
	}
	if h := m.History(0); len(h) != 2 {
		t.Fatalf("history %+v", h)
	}
}

func TestLostAndHealthyAreNotQueued(t *testing.T) {
	m := NewManager(newFake(restored), Config{}, events.Discard())
	if out := m.Enqueue(context.Background(), assess("x", 0, 3), time.Time{}); out != OutcomeUnrepaired {
		t.Fatal(out)
	}
	if out := m.Enqueue(context.Background(), assess("y", 3, 3), time.Time{}); out != OutcomeNotNeeded {
		t.Fatal(out)
	}
}
