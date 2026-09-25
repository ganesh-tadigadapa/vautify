package metadata

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"

	"vault/internal/keylock"
	"vault/internal/objectid"
)

// FileStore keeps one JSON file per object under a directory, with an
// in-memory index for reads. Every mutation is written to disk (temp file,
// fsync, rename) before it becomes visible, so metadata survives restarts.
//
// Mutations of the same object are serialised; mutations of different
// objects proceed in parallel.
type FileStore struct {
	dir   string
	locks keylock.Map

	mu   sync.RWMutex
	objs map[string]Object
}

var _ Store = (*FileStore)(nil)

// OpenFileStore loads (creating if needed) the store in dir.
func OpenFileStore(dir string) (*FileStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &FileStore{dir: dir, objs: make(map[string]Object)}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		name := e.Name()
		if strings.HasPrefix(name, ".tmp-") {
			// An interrupted write; the previous committed file is intact.
			if err := os.Remove(filepath.Join(dir, name)); err != nil {
				return nil, err
			}
			continue
		}
		if e.IsDir() || !strings.HasSuffix(name, ".json") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			return nil, err
		}
		var obj Object
		if err := json.Unmarshal(b, &obj); err != nil {
			return nil, fmt.Errorf("metadata file %s: %w", name, err)
		}
		if obj.ID+".json" != name {
			return nil, fmt.Errorf("metadata file %s contains object %q", name, obj.ID)
		}
		s.objs[obj.ID] = obj
	}
	return s, nil
}

func (s *FileStore) path(id string) string { return filepath.Join(s.dir, id+".json") }

func (s *FileStore) GetObject(ctx context.Context, id string) (Object, error) {
	obj, err := s.GetRecord(ctx, id)
	if err == nil && obj.Deleted {
		return Object{}, ErrDeleted
	}
	return obj, err
}

func (s *FileStore) GetRecord(_ context.Context, id string) (Object, error) {
	s.mu.RLock()
	obj, ok := s.objs[id]
	s.mu.RUnlock()
	if !ok {
		return Object{}, ErrNotFound
	}
	return obj.Clone(), nil
}

func (s *FileStore) ListObjects(_ context.Context) ([]Object, error) { return s.list(false), nil }

func (s *FileStore) ListRecords(_ context.Context) ([]Object, error) { return s.list(true), nil }

func (s *FileStore) list(withTombstones bool) []Object {
	s.mu.RLock()
	out := make([]Object, 0, len(s.objs))
	for _, o := range s.objs {
		if withTombstones || !o.Deleted {
			out = append(out, o.Clone())
		}
	}
	s.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// mutate runs fn on the current state of id under that id's lock, persists
// the result, then publishes it to the in-memory index.
func (s *FileStore) mutate(id string, fn func(cur Object, exists bool) (next Object, remove bool, err error)) error {
	if err := objectid.Validate(id); err != nil {
		return err
	}
	unlock := s.locks.Lock(id)
	defer unlock()

	s.mu.RLock()
	cur, exists := s.objs[id]
	s.mu.RUnlock()

	next, remove, err := fn(cur.Clone(), exists)
	if err != nil {
		return err
	}
	if remove {
		if err := os.Remove(s.path(id)); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		s.mu.Lock()
		delete(s.objs, id)
		s.mu.Unlock()
		return nil
	}
	if next.ID != id {
		return fmt.Errorf("metadata: object id %q does not match key %q", next.ID, id)
	}
	if err := s.persist(next); err != nil {
		return err
	}
	s.mu.Lock()
	s.objs[id] = next.Clone()
	s.mu.Unlock()
	return nil
}

func (s *FileStore) persist(obj Object) error {
	b, err := json.MarshalIndent(obj, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(s.dir, ".tmp-*")
	if err != nil {
		return err
	}
	name := f.Name()
	_, werr := f.Write(b)
	if werr == nil {
		werr = f.Sync()
	}
	if cerr := f.Close(); werr == nil {
		werr = cerr
	}
	if werr == nil {
		werr = os.Rename(name, s.path(obj.ID))
	}
	if werr != nil {
		os.Remove(name)
		return fmt.Errorf("persist metadata for %s: %w", obj.ID, werr)
	}
	return nil
}

func (s *FileStore) PutObject(_ context.Context, obj Object) error {
	return s.mutate(obj.ID, func(_ Object, exists bool) (Object, bool, error) {
		if exists {
			return Object{}, false, ErrExists
		}
		return obj, false, nil
	})
}

func (s *FileStore) UpdateObject(_ context.Context, obj Object) error {
	return s.mutate(obj.ID, func(cur Object, exists bool) (Object, bool, error) {
		if !exists {
			return Object{}, false, ErrNotFound
		}
		if obj.Version <= cur.Version {
			return Object{}, false, fmt.Errorf("%w: stored version %d, update carries %d", ErrVersionConflict, cur.Version, obj.Version)
		}
		return obj, false, nil
	})
}

func (s *FileStore) DeleteObject(_ context.Context, id string) error {
	return s.mutate(id, func(_ Object, exists bool) (Object, bool, error) {
		if !exists {
			return Object{}, false, ErrNotFound
		}
		return Object{}, true, nil
	})
}

func (s *FileStore) RecordIntegrityFailure(_ context.Context, id string, f IntegrityFailure) error {
	return s.mutate(id, func(cur Object, exists bool) (Object, bool, error) {
		if !exists {
			return Object{}, false, ErrNotFound
		}
		if cur.Deleted {
			return Object{}, false, ErrDeleted
		}
		if cur.Version != f.Version {
			return Object{}, false, fmt.Errorf("%w: failure is for version %d, current is %d", ErrVersionConflict, f.Version, cur.Version)
		}
		if !slices.Contains(cur.Replicas, f.NodeID) {
			// The node no longer holds a tracked replica (e.g. a repair
			// replaced it); a late observation about it is irrelevant.
			return Object{}, false, ErrVersionConflict
		}
		for i, existing := range cur.IntegrityFailures {
			if existing.NodeID == f.NodeID && existing.Version == f.Version {
				f.FirstDetectedAt = existing.FirstDetectedAt
				f.Occurrences = existing.Occurrences + 1
				cur.IntegrityFailures[i] = f
				return cur, false, nil
			}
		}
		f.FirstDetectedAt = f.LastDetectedAt
		f.Occurrences = 1
		cur.IntegrityFailures = append(cur.IntegrityFailures, f)
		return cur, false, nil
	})
}

func (s *FileStore) Modify(_ context.Context, id string, fn func(Object) (Object, error)) (Object, error) {
	var result Object
	err := s.mutate(id, func(cur Object, exists bool) (Object, bool, error) {
		if !exists {
			return Object{}, false, ErrNotFound
		}
		if cur.Deleted {
			return Object{}, false, ErrDeleted
		}
		next, err := fn(cur)
		if err != nil {
			return Object{}, false, err
		}
		if next.ID != cur.ID || next.Version != cur.Version || next.Deleted {
			return Object{}, false, fmt.Errorf("metadata: Modify may not change id or version (%s v%d -> %s v%d)",
				cur.ID, cur.Version, next.ID, next.Version)
		}
		result = next.Clone()
		return next, false, nil
	})
	return result, err
}
