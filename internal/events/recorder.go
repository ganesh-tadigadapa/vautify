package events

import (
	"fmt"
	"sync"
	"time"
)

// Event is one recorded structured event.
type Event struct {
	Seq       uint64         `json:"seq"`
	Time      time.Time      `json:"time"`
	Level     string         `json:"level"`
	Event     Name           `json:"event"`
	Component string         `json:"component"`
	Fields    map[string]any `json:"fields,omitempty"`
}

// Recorder keeps the most recent events in a fixed-size ring buffer. It is
// the source of the event timeline exposed over HTTP; the JSON log remains
// the durable record.
type Recorder struct {
	mu   sync.Mutex
	buf  []Event
	next int
	full bool
	seq  uint64
}

// NewRecorder returns a Recorder holding up to capacity events.
func NewRecorder(capacity int) *Recorder {
	if capacity < 1 {
		capacity = 1
	}
	return &Recorder{buf: make([]Event, capacity)}
}

func (r *Recorder) record(level string, name Name, component string, attrs []any) {
	fields := make(map[string]any, len(attrs)/2)
	for i := 0; i+1 < len(attrs); i += 2 {
		k, ok := attrs[i].(string)
		if !ok {
			k = fmt.Sprint(attrs[i])
		}
		fields[k] = attrs[i+1]
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seq++
	r.buf[r.next] = Event{Seq: r.seq, Time: time.Now().UTC(), Level: level, Event: name, Component: component, Fields: fields}
	r.next = (r.next + 1) % len(r.buf)
	if r.next == 0 {
		r.full = true
	}
}

// Since returns up to limit events with Seq > since, oldest first.
// limit <= 0 means no limit.
func (r *Recorder) Since(since uint64, limit int) []Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	var ordered []Event
	if r.full {
		ordered = append(ordered, r.buf[r.next:]...)
	}
	ordered = append(ordered, r.buf[:r.next]...)
	out := make([]Event, 0, len(ordered))
	for _, e := range ordered {
		if e.Seq > since {
			out = append(out, e)
		}
	}
	if limit > 0 && len(out) > limit {
		out = out[len(out)-limit:]
	}
	return out
}
