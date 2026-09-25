package health

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"vault/internal/events"
	"vault/internal/storage"
)

type fakeProber struct {
	id   string
	down atomic.Bool
}

func (f *fakeProber) ID() string      { return f.id }
func (f *fakeProber) Address() string { return "fake://" + f.id }
func (f *fakeProber) Health(context.Context) (storage.NodeHealth, error) {
	if f.down.Load() {
		return storage.NodeHealth{}, errors.New("connection refused")
	}
	return storage.NodeHealth{NodeID: f.id, Status: storage.StateHealthy}, nil
}

func TestTransitionsAndEvents(t *testing.T) {
	ctx := context.Background()
	a, b := &fakeProber{id: "node-1"}, &fakeProber{id: "node-2"}
	var logs bytes.Buffer
	c := New([]Prober{a, b}, time.Hour, time.Second, events.New(&logs, "test"))

	if len(c.Healthy()) != 0 {
		t.Fatal("nodes healthy before first probe")
	}
	c.ProbeAll(ctx)
	if got := c.Healthy(); !slices.Equal(got, []string{"node-1", "node-2"}) {
		t.Fatalf("healthy = %v", got)
	}

	b.down.Store(true)
	c.ProbeAll(ctx)
	if c.State("node-2") != storage.StateUnavailable {
		t.Fatal("node-2 not marked unavailable")
	}
	if !strings.Contains(logs.String(), `"event":"NODE_UNAVAILABLE"`) {
		t.Fatalf("no NODE_UNAVAILABLE event: %s", logs.String())
	}

	b.down.Store(false)
	c.ProbeAll(ctx)
	if c.State("node-2") != storage.StateHealthy {
		t.Fatal("node-2 not recovered")
	}
	if !strings.Contains(logs.String(), `"event":"NODE_RECOVERED"`) {
		t.Fatalf("no NODE_RECOVERED event: %s", logs.String())
	}
}

func TestPassiveFailureReport(t *testing.T) {
	ctx := context.Background()
	a := &fakeProber{id: "node-1"}
	c := New([]Prober{a}, time.Hour, time.Second, events.Discard())
	c.ProbeAll(ctx)
	c.ReportFailure(ctx, "node-1", errors.New("dial tcp: connection refused"))
	if c.State("node-1") != storage.StateUnavailable {
		t.Fatal("passive failure did not mark node unavailable")
	}
	c.ReportFailure(ctx, "unknown", errors.New("x")) // must not panic
}
