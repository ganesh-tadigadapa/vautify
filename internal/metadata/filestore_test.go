package metadata

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func sample(id string, v uint64) Object {
	now := time.Now().UTC().Truncate(time.Millisecond)
	return Object{
		ID: id, Version: v, Size: 3, SHA256: "abc", CreatedAt: now, UpdatedAt: now,
		ReplicationFactor: 3, Replicas: []string{"node-1", "node-2", "node-3"},
	}
}

func TestCRUDAndPersistence(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s, err := OpenFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetObject(ctx, "a"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("get missing: %v", err)
	}
	if err := s.PutObject(ctx, sample("a", 1)); err != nil {
		t.Fatal(err)
	}
	if err := s.PutObject(ctx, sample("a", 1)); !errors.Is(err, ErrExists) {
		t.Fatalf("duplicate put: %v", err)
	}
	if err := s.UpdateObject(ctx, sample("a", 1)); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("same-version update: %v", err)
	}
	if err := s.UpdateObject(ctx, sample("a", 2)); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateObject(ctx, sample("missing", 2)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("update missing: %v", err)
	}

	// Returned objects must not alias store state.
	got, _ := s.GetObject(ctx, "a")
	got.Replicas[0] = "mutated"
	again, _ := s.GetObject(ctx, "a")
	if again.Replicas[0] != "node-1" {
		t.Fatal("GetObject returned aliased slice")
	}

	// Reopen from disk.
	s2, err := OpenFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	got, err = s2.GetObject(ctx, "a")
	if err != nil || got.Version != 2 || len(got.Replicas) != 3 {
		t.Fatalf("after reopen: %+v, %v", got, err)
	}

	if err := s2.DeleteObject(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	if err := s2.DeleteObject(ctx, "a"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("second delete: %v", err)
	}
	s3, _ := OpenFileStore(dir)
	if list, _ := s3.ListObjects(ctx); len(list) != 0 {
		t.Fatalf("deleted object reappeared after reopen: %+v", list)
	}
}

func TestRecordIntegrityFailure(t *testing.T) {
	ctx := context.Background()
	s, _ := OpenFileStore(t.TempDir())
	s.PutObject(ctx, sample("a", 1))

	f := IntegrityFailure{NodeID: "node-2", Version: 1, Reason: ReasonChecksumMismatch, LastDetectedAt: time.Now().UTC()}
	for i := 0; i < 3; i++ {
		if err := s.RecordIntegrityFailure(ctx, "a", f); err != nil {
			t.Fatal(err)
		}
	}
	got, _ := s.GetObject(ctx, "a")
	if len(got.IntegrityFailures) != 1 || got.IntegrityFailures[0].Occurrences != 3 {
		t.Fatalf("failures = %+v, want one record with 3 occurrences", got.IntegrityFailures)
	}

	stale := f
	stale.Version = 0
	if err := s.RecordIntegrityFailure(ctx, "a", stale); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("stale-version failure: %v", err)
	}
}

func TestConcurrentWritesToDifferentObjects(t *testing.T) {
	ctx := context.Background()
	s, _ := OpenFileStore(t.TempDir())
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("obj-%d", i)
			if err := s.PutObject(ctx, sample(id, 1)); err != nil {
				t.Error(err)
			}
			if err := s.UpdateObject(ctx, sample(id, 2)); err != nil {
				t.Error(err)
			}
			if _, err := s.GetObject(ctx, id); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	list, _ := s.ListObjects(ctx)
	if len(list) != 64 {
		t.Fatalf("len = %d, want 64", len(list))
	}
}

func TestTombstonesAreDurableAndHiddenFromLiveReads(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s, _ := OpenFileStore(dir)
	s.PutObject(ctx, sample("a", 1))
	now := time.Now().UTC()
	tomb := Object{ID: "a", Version: 2, Deleted: true, DeletedAt: &now, UpdatedAt: now}
	if err := s.UpdateObject(ctx, tomb); err != nil {
		t.Fatal(err)
	}
	for name, err := range map[string]error{
		"GetObject": func() error { _, err := s.GetObject(ctx, "a"); return err }(),
		"Modify": func() error {
			_, err := s.Modify(ctx, "a", func(o Object) (Object, error) { return o, nil })
			return err
		}(),
		"Record": s.RecordIntegrityFailure(ctx, "a", IntegrityFailure{NodeID: "node-1", Version: 2}),
	} {
		if !errors.Is(err, ErrDeleted) || !errors.Is(err, ErrNotFound) {
			t.Fatalf("%s on tombstone: %v", name, err)
		}
	}
	if live, _ := s.ListObjects(ctx); len(live) != 0 {
		t.Fatal("tombstone listed as live")
	}
	if err := s.UpdateObject(ctx, sample("a", 2)); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("older-or-equal version over tombstone: %v", err)
	}

	s2, _ := OpenFileStore(dir) // restart
	rec, err := s2.GetRecord(ctx, "a")
	if err != nil || !rec.Deleted || rec.Version != 2 {
		t.Fatalf("tombstone not durable: %+v %v", rec, err)
	}
	if all, _ := s2.ListRecords(ctx); len(all) != 1 {
		t.Fatal("ListRecords lost the tombstone")
	}
	if err := s2.UpdateObject(ctx, sample("a", 3)); err != nil {
		t.Fatalf("new live version after tombstone: %v", err)
	}
	if o, err := s2.GetObject(ctx, "a"); err != nil || o.Version != 3 {
		t.Fatalf("after re-create: %+v %v", o, err)
	}
}
