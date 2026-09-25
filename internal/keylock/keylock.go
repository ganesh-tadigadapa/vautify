// Package keylock provides per-key mutual exclusion.
//
// It lets Vault serialise operations on the same object without serialising
// operations on different objects.
package keylock

import "sync"

type entry struct {
	mu   sync.Mutex
	refs int
}

// Map is a set of lazily created mutexes keyed by string. The zero value is
// ready to use. Entries are removed once no goroutine holds or waits on them,
// so memory is bounded by the number of keys in active use.
type Map struct {
	mu    sync.Mutex
	locks map[string]*entry
}

// Lock acquires the mutex for key and returns a function that releases it.
func (m *Map) Lock(key string) (unlock func()) {
	m.mu.Lock()
	if m.locks == nil {
		m.locks = make(map[string]*entry)
	}
	e, ok := m.locks[key]
	if !ok {
		e = &entry{}
		m.locks[key] = e
	}
	e.refs++
	m.mu.Unlock()

	e.mu.Lock()
	return func() {
		e.mu.Unlock()
		m.mu.Lock()
		e.refs--
		if e.refs == 0 {
			delete(m.locks, key)
		}
		m.mu.Unlock()
	}
}

// Len returns the number of keys currently tracked (held or awaited).
func (m *Map) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.locks)
}
