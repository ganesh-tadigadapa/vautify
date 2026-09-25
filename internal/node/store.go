// Package node implements a Vault storage node: a durable on-disk replica
// store and the HTTP server that exposes it.
package node

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"vault/internal/integrity"
	"vault/internal/keylock"
	"vault/internal/objectid"
	"vault/internal/storage"
)

const (
	identityFile = "node.json"
	objectsDir   = "objects"
	tmpDir       = "tmp"
	dataSuffix   = ".data"
	metaSuffix   = ".json"
)

// ReplicaDataPath returns where a node rooted at root keeps the bytes of
// (objectID, version). It is exported so tests and the demo can inspect and
// deliberately damage on-disk replicas the same way real bit rot would.
func ReplicaDataPath(root, objectID string, version uint64) string {
	return filepath.Join(root, objectsDir, objectID, versionName(version)+dataSuffix)
}

func versionName(v uint64) string { return fmt.Sprintf("%020d", v) }

type identity struct {
	NodeID    string    `json:"node_id"`
	CreatedAt time.Time `json:"created_at"`
}

// replicaRecord is the sidecar written next to each replica's bytes.
type replicaRecord struct {
	ObjectID string    `json:"object_id"`
	Version  uint64    `json:"version"`
	SHA256   string    `json:"sha256"`
	Size     int64     `json:"size"`
	StoredAt time.Time `json:"stored_at"`
}

// Store persists replicas under a root directory. All state needed to serve
// replicas lives on disk; the in-memory counters are rebuilt on Open.
type Store struct {
	root   string
	nodeID string
	locks  keylock.Map

	usedBytes    atomic.Int64
	replicaCount atomic.Int64
}

// OpenStore opens (creating if needed) the store rooted at root for nodeID.
// It refuses to open a directory that belongs to a different node, which
// prevents two node identities from silently sharing one disk.
func OpenStore(root, nodeID string) (*Store, error) {
	if nodeID == "" {
		return nil, errors.New("node id is required")
	}
	for _, d := range []string{root, filepath.Join(root, objectsDir), filepath.Join(root, tmpDir)} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return nil, fmt.Errorf("create %s: %w", d, err)
		}
	}
	if err := claimIdentity(root, nodeID); err != nil {
		return nil, err
	}
	// Anything left in tmp is an interrupted write that was never committed.
	if err := clearDir(filepath.Join(root, tmpDir)); err != nil {
		return nil, fmt.Errorf("clear tmp: %w", err)
	}
	s := &Store{root: root, nodeID: nodeID}
	if err := s.scanUsage(); err != nil {
		return nil, fmt.Errorf("scan replicas: %w", err)
	}
	return s, nil
}

func claimIdentity(root, nodeID string) error {
	path := filepath.Join(root, identityFile)
	b, err := os.ReadFile(path)
	switch {
	case err == nil:
		var id identity
		if err := json.Unmarshal(b, &id); err != nil {
			return fmt.Errorf("read %s: %w", path, err)
		}
		if id.NodeID != nodeID {
			return fmt.Errorf("data directory %s belongs to node %q, not %q", root, id.NodeID, nodeID)
		}
		return nil
	case errors.Is(err, fs.ErrNotExist):
		b, _ := json.MarshalIndent(identity{NodeID: nodeID, CreatedAt: time.Now().UTC()}, "", "  ")
		return writeFileAtomic(filepath.Join(root, tmpDir), path, b)
	default:
		return err
	}
}

func clearDir(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if err := os.RemoveAll(filepath.Join(dir, e.Name())); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) scanUsage() error {
	return filepath.WalkDir(filepath.Join(s.root, objectsDir), func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, dataSuffix) {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		s.usedBytes.Add(info.Size())
		s.replicaCount.Add(1)
		return nil
	})
}

// NodeID returns the identity this store was opened for.
func (s *Store) NodeID() string { return s.nodeID }

// Root returns the store's root directory.
func (s *Store) Root() string { return s.root }

func (s *Store) paths(objectID string, version uint64) (dir, data, meta string, err error) {
	if err := objectid.Validate(objectID); err != nil {
		return "", "", "", err
	}
	dir = filepath.Join(s.root, objectsDir, objectID)
	base := filepath.Join(dir, versionName(version))
	return dir, base + dataSuffix, base + metaSuffix, nil
}

// Put streams r to disk as (objectID, version) and commits it only if the
// bytes match want. On mismatch nothing is committed and the returned error
// wraps both storage.ErrRejected and *integrity.MismatchError.
func (s *Store) Put(objectID string, version uint64, want integrity.Digest, r io.Reader) (storage.ReplicaInfo, error) {
	dir, dataPath, metaPath, err := s.paths(objectID, version)
	if err != nil {
		return storage.ReplicaInfo{}, err
	}

	tmp, err := os.CreateTemp(filepath.Join(s.root, tmpDir), "put-*")
	if err != nil {
		return storage.ReplicaInfo{}, err
	}
	tmpName := tmp.Name()
	committed := false
	defer func() {
		if !committed {
			tmp.Close()
			os.Remove(tmpName)
		}
	}()

	hw := integrity.NewHashingWriter(tmp)
	// Read one byte past the declared size so oversized bodies are detected.
	if _, err := io.Copy(hw, io.LimitReader(r, want.Size+1)); err != nil {
		return storage.ReplicaInfo{}, fmt.Errorf("receive replica: %w", err)
	}
	got := hw.Digest()
	if err := integrity.Verify(want, got); err != nil {
		return storage.ReplicaInfo{}, fmt.Errorf("%w: %w", storage.ErrRejected, err)
	}
	if err := tmp.Sync(); err != nil {
		return storage.ReplicaInfo{}, err
	}
	if err := tmp.Close(); err != nil {
		return storage.ReplicaInfo{}, err
	}

	unlock := s.locks.Lock(objectID)
	defer unlock()

	// Older data never overwrites newer data: refuse a version older than
	// the newest this node already holds. (Rewriting the same version is
	// allowed — that is how a corrupted replica is repaired in place.)
	if newest, err := s.newestVersion(dir); err != nil {
		return storage.ReplicaInfo{}, err
	} else if newest > version {
		return storage.ReplicaInfo{}, fmt.Errorf("%w: holds v%d, refused v%d of %s", storage.ErrStaleVersion, newest, version, objectID)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return storage.ReplicaInfo{}, err
	}
	var replacedSize int64 = -1
	if fi, err := os.Stat(dataPath); err == nil {
		replacedSize = fi.Size()
	}
	if err := os.Rename(tmpName, dataPath); err != nil {
		return storage.ReplicaInfo{}, err
	}
	committed = true

	rec := replicaRecord{ObjectID: objectID, Version: version, SHA256: got.SHA256, Size: got.Size, StoredAt: time.Now().UTC()}
	b, _ := json.Marshal(rec)
	if err := writeFileAtomic(filepath.Join(s.root, tmpDir), metaPath, b); err != nil {
		return storage.ReplicaInfo{}, fmt.Errorf("write replica record: %w", err)
	}
	if err := syncDir(dir); err != nil {
		return storage.ReplicaInfo{}, err
	}

	if replacedSize >= 0 {
		s.usedBytes.Add(got.Size - replacedSize)
	} else {
		s.usedBytes.Add(got.Size)
		s.replicaCount.Add(1)
	}
	return s.info(rec), nil
}

func (s *Store) info(rec replicaRecord) storage.ReplicaInfo {
	return storage.ReplicaInfo{
		NodeID: s.nodeID, ObjectID: rec.ObjectID, Version: rec.Version,
		SHA256: rec.SHA256, Size: rec.Size, StoredAt: rec.StoredAt,
	}
}

// Open returns the stored bytes of (objectID, version) and the record written
// when they were stored. The bytes are returned exactly as found on disk;
// they are NOT re-verified here.
func (s *Store) Open(objectID string, version uint64) (*os.File, storage.ReplicaInfo, error) {
	_, dataPath, metaPath, err := s.paths(objectID, version)
	if err != nil {
		return nil, storage.ReplicaInfo{}, err
	}
	f, err := os.Open(dataPath)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, storage.ReplicaInfo{}, storage.ErrNotFound
	}
	if err != nil {
		return nil, storage.ReplicaInfo{}, err
	}
	rec, err := readRecord(metaPath)
	if err != nil {
		f.Close()
		return nil, storage.ReplicaInfo{}, fmt.Errorf("read replica record: %w", err)
	}
	if rec.ObjectID != objectID || rec.Version != version {
		f.Close()
		return nil, storage.ReplicaInfo{}, fmt.Errorf("replica record mismatch: file for %s v%d describes %s v%d",
			objectID, version, rec.ObjectID, rec.Version)
	}
	return f, s.info(rec), nil
}

func readRecord(path string) (replicaRecord, error) {
	var rec replicaRecord
	b, err := os.ReadFile(path)
	if err != nil {
		return rec, err
	}
	return rec, json.Unmarshal(b, &rec)
}

// Verify re-hashes the replica on disk and compares it to its record.
func (s *Store) Verify(objectID string, version uint64) (storage.VerifyResult, error) {
	f, info, err := s.Open(objectID, version)
	if err != nil {
		return storage.VerifyResult{}, err
	}
	defer f.Close()
	actual, err := integrity.Compute(f)
	if err != nil {
		return storage.VerifyResult{}, err
	}
	recorded := integrity.Digest{SHA256: info.SHA256, Size: info.Size}
	return storage.VerifyResult{
		NodeID: s.nodeID, ObjectID: objectID, Version: version,
		Recorded: recorded, Actual: actual, Valid: integrity.Verify(recorded, actual) == nil,
	}, nil
}

// Delete removes one version of an object. It returns storage.ErrNotFound if
// that version is not stored.
func (s *Store) Delete(objectID string, version uint64) error {
	dir, dataPath, metaPath, err := s.paths(objectID, version)
	if err != nil {
		return err
	}
	unlock := s.locks.Lock(objectID)
	defer unlock()

	fi, err := os.Stat(dataPath)
	if errors.Is(err, fs.ErrNotExist) {
		return storage.ErrNotFound
	}
	if err != nil {
		return err
	}
	if err := os.Remove(dataPath); err != nil {
		return err
	}
	if err := os.Remove(metaPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	s.usedBytes.Add(-fi.Size())
	s.replicaCount.Add(-1)
	os.Remove(dir) // succeeds only if no other versions remain
	return nil
}

// DeleteAll removes every stored version of objectID and reports how many
// replicas were removed. It returns storage.ErrNotFound if there were none.
func (s *Store) DeleteAll(objectID string) (int, error) {
	dir, _, _, err := s.paths(objectID, 0)
	if err != nil {
		return 0, err
	}
	unlock := s.locks.Lock(objectID)
	defer unlock()

	entries, err := os.ReadDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return 0, storage.ErrNotFound
	}
	if err != nil {
		return 0, err
	}
	removed := 0
	for _, e := range entries {
		path := filepath.Join(dir, e.Name())
		if strings.HasSuffix(e.Name(), dataSuffix) {
			fi, err := e.Info()
			if err != nil {
				return removed, err
			}
			if err := os.Remove(path); err != nil {
				return removed, err
			}
			s.usedBytes.Add(-fi.Size())
			s.replicaCount.Add(-1)
			removed++
		} else if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return removed, err
		}
	}
	if err := os.Remove(dir); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return removed, err
	}
	if removed == 0 {
		return 0, storage.ErrNotFound
	}
	return removed, nil
}

// Capacity reports filesystem and replica usage for this node.
func (s *Store) Capacity() (storage.Capacity, error) {
	total, free, err := diskSpace(s.root)
	if err != nil {
		return storage.Capacity{}, err
	}
	return storage.Capacity{
		TotalBytes:   total,
		FreeBytes:    free,
		UsedBytes:    s.usedBytes.Load(),
		ReplicaCount: s.replicaCount.Load(),
	}, nil
}

// writeFileAtomic writes b to a temp file in tmpDir and renames it to path.
func writeFileAtomic(tmpDir, path string, b []byte) error {
	f, err := os.CreateTemp(tmpDir, "atomic-*")
	if err != nil {
		return err
	}
	name := f.Name()
	if _, err := f.Write(b); err != nil {
		f.Close()
		os.Remove(name)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(name)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(name)
		return err
	}
	if err := os.Rename(name, path); err != nil {
		os.Remove(name)
		return err
	}
	return nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// ParseVersion parses a decimal version from a URL path segment.
func ParseVersion(s string) (uint64, error) {
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil || v == 0 {
		return 0, fmt.Errorf("invalid version %q", s)
	}
	return v, nil
}

// versionsIn returns the versions stored in an object directory, ascending.
func versionsIn(dir string) ([]uint64, error) {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var vs []uint64
	for _, e := range entries {
		name, ok := strings.CutSuffix(e.Name(), dataSuffix)
		if !ok {
			continue
		}
		if v, err := strconv.ParseUint(name, 10, 64); err == nil {
			vs = append(vs, v)
		}
	}
	sort.Slice(vs, func(i, j int) bool { return vs[i] < vs[j] })
	return vs, nil
}

func (s *Store) newestVersion(dir string) (uint64, error) {
	vs, err := versionsIn(dir)
	if err != nil || len(vs) == 0 {
		return 0, err
	}
	return vs[len(vs)-1], nil
}

// List returns every stored version of objectID, oldest first. Each entry
// comes from the replica's own record, so it identifies its version.
func (s *Store) List(objectID string) ([]storage.ReplicaInfo, error) {
	dir, _, _, err := s.paths(objectID, 0)
	if err != nil {
		return nil, err
	}
	vs, err := versionsIn(dir)
	if err != nil {
		return nil, err
	}
	out := make([]storage.ReplicaInfo, 0, len(vs))
	for _, v := range vs {
		info := storage.ReplicaInfo{NodeID: s.nodeID, ObjectID: objectID, Version: v}
		if rec, err := readRecord(filepath.Join(dir, versionName(v)+metaSuffix)); err == nil && rec.Version == v {
			info = s.info(rec)
		}
		if fi, err := os.Stat(filepath.Join(dir, versionName(v)+dataSuffix)); err == nil {
			info.Size = fi.Size()
		}
		out = append(out, info)
	}
	return out, nil
}

// Inventory lists every replica on this node, grouped by object ID order.
func (s *Store) Inventory() ([]storage.ReplicaInfo, error) {
	entries, err := os.ReadDir(filepath.Join(s.root, objectsDir))
	if err != nil {
		return nil, err
	}
	var out []storage.ReplicaInfo
	for _, e := range entries {
		if !e.IsDir() || objectid.Validate(e.Name()) != nil {
			continue
		}
		reps, err := s.List(e.Name())
		if err != nil {
			return nil, err
		}
		out = append(out, reps...)
	}
	return out, nil
}
