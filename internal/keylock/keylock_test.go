package keylock

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSameKeyIsExclusive(t *testing.T) {
	var m Map
	var inside, maxInside int32
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			unlock := m.Lock("k")
			n := atomic.AddInt32(&inside, 1)
			for {
				cur := atomic.LoadInt32(&maxInside)
				if n <= cur || atomic.CompareAndSwapInt32(&maxInside, cur, n) {
					break
				}
			}
			time.Sleep(100 * time.Microsecond)
			atomic.AddInt32(&inside, -1)
			unlock()
		}()
	}
	wg.Wait()
	if maxInside != 1 {
		t.Fatalf("max concurrent holders = %d, want 1", maxInside)
	}
	if m.Len() != 0 {
		t.Fatalf("entries leaked: %d", m.Len())
	}
}

func TestDifferentKeysDoNotBlock(t *testing.T) {
	var m Map
	unlockA := m.Lock("a")
	defer unlockA()

	done := make(chan struct{})
	go func() {
		unlock := m.Lock("b")
		unlock()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("lock on key b blocked by lock on key a")
	}
}
