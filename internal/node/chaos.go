package node

import (
	"errors"
	"io"
	"io/fs"
	"os"

	"vault/internal/integrity"
	"vault/internal/storage"
)

// Corrupt silently damages the stored bytes of (objectID, version): it
// inverts up to 16 bytes in the middle of the file (or, for an empty
// replica, writes one byte). The replica's checksum record is NOT updated,
// so the damage is only discoverable by re-hashing — exactly like bit rot.
// It exists for chaos testing and is only reachable when chaos is enabled.
func (s *Store) Corrupt(objectID string, version uint64) (storage.CorruptResult, error) {
	_, dataPath, _, err := s.paths(objectID, version)
	if err != nil {
		return storage.CorruptResult{}, err
	}
	unlock := s.locks.Lock(objectID)
	defer unlock()

	f, err := os.OpenFile(dataPath, os.O_RDWR, 0)
	if errors.Is(err, fs.ErrNotExist) {
		return storage.CorruptResult{}, storage.ErrNotFound
	}
	if err != nil {
		return storage.CorruptResult{}, err
	}
	defer f.Close()

	before, err := integrity.Compute(f)
	if err != nil {
		return storage.CorruptResult{}, err
	}
	res := storage.CorruptResult{NodeID: s.nodeID, ObjectID: objectID, Version: version, Before: before}
	if before.Size == 0 {
		if _, err := f.WriteAt([]byte{0}, 0); err != nil {
			return res, err
		}
		res.Length = 1
		s.usedBytes.Add(1)
	} else {
		n := int64(16)
		if before.Size < n {
			n = before.Size
		}
		res.Offset, res.Length = (before.Size-n)/2, int(n)
		buf := make([]byte, n)
		if _, err := f.ReadAt(buf, res.Offset); err != nil {
			return res, err
		}
		for i := range buf {
			buf[i] ^= 0xFF
		}
		if _, err := f.WriteAt(buf, res.Offset); err != nil {
			return res, err
		}
	}
	if err := f.Sync(); err != nil {
		return res, err
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return res, err
	}
	if res.After, err = integrity.Compute(f); err != nil {
		return res, err
	}
	return res, nil
}
