package repair

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type panicExec struct{ calls atomic.Int32 }

func (p *panicExec) Repair(_ context.Context, t Task) (Result, error) {
	if p.calls.Add(1) == 1 {
		var m map[string]int
		m["boom"]++ // nil-map write: a real runtime panic
	}
	return restored(t)
}

// A panicking executor must not kill the worker (or the process): the
// attempt is recorded as failed, retried, and the next attempt succeeds.
func TestExecutorPanicBecomesFailedAttempt(t *testing.T) {
	exec := &panicExec{}
	m, stop := startManager(t, exec, Config{Workers: 1, BaseBackoff: time.Millisecond, MaxBackoff: time.Millisecond})
	defer stop()

	m.Enqueue(context.Background(), assess("a", 1, 3), time.Time{})
	waitIdle(t, m)

	if got := exec.calls.Load(); got != 2 {
		t.Fatalf("executor calls = %d, want 2 (panic, then retry)", got)
	}
	h := m.History(0)
	if len(h) != 2 || h[0].Outcome != "failed_attempt" || h[1].Outcome != "restored" {
		t.Fatalf("history = %+v", h)
	}
	if !strings.Contains(h[0].Error, "panicked") {
		t.Fatalf("first attempt should carry the panic error, got %q", h[0].Error)
	}
}

// Forget bounds the parked-failure and restored-mark maps by the live
// object set: entries for deleted objects are dropped, live ones kept.
func TestForgetDropsDeletedObjects(t *testing.T) {
	failing := newFake(func(t Task) (Result, error) { return Result{}, errors.New("no target") })
	m, stop := startManager(t, failing, Config{Workers: 2, MaxAttempts: 1})
	ctx := context.Background()
	for _, id := range []string{"gone-1", "gone-2", "live-1"} {
		m.Enqueue(ctx, assess(id, 1, 3), time.Time{})
	}
	waitIdle(t, m)
	stop()
	if n := m.Metrics().Parked; n != 3 {
		t.Fatalf("parked = %d, want 3", n)
	}
	m.mu.Lock()
	for _, id := range []string{"gone-3", "live-2"} {
		m.restored[id] = restoredMark{version: 1, at: time.Now()}
	}
	m.mu.Unlock()

	live := map[string]bool{"live-1": true, "live-2": true}
	m.Forget(func(id string) bool { return live[id] })

	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.failed) != 1 || m.failed["live-1"].Task.ObjectID != "live-1" {
		t.Fatalf("failed after Forget = %v", m.failed)
	}
	if len(m.restored) != 1 {
		t.Fatalf("restored after Forget = %v", m.restored)
	}
	if _, ok := m.restored["live-2"]; !ok {
		t.Fatalf("live restored mark was dropped: %v", m.restored)
	}
}
