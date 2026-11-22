package events

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestRecoverLogsAndSwallowsPanic(t *testing.T) {
	var buf bytes.Buffer
	lg := New(&buf, "test")
	var recovered bool
	func() {
		defer lg.Recover(context.Background(), "unit", &recovered)
		panic("kaboom")
	}()
	if !recovered {
		t.Fatal("recovered flag not set")
	}
	out := buf.String()
	for _, want := range []string{`"msg":"recovered panic"`, `"where":"unit"`, `"panic":"kaboom"`, `"stack"`} {
		if !strings.Contains(out, want) {
			t.Fatalf("log %q missing %s", out, want)
		}
	}

	// No panic: nothing logged, flag untouched.
	buf.Reset()
	recovered = false
	func() { defer lg.Recover(context.Background(), "unit", &recovered) }()
	if recovered || buf.Len() != 0 {
		t.Fatalf("spurious recovery: %v %q", recovered, buf.String())
	}
}
