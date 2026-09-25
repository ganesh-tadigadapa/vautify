// Package integrity implements SHA-256 based integrity verification.
//
// The central primitive is Spool: bytes are streamed to a temporary file while
// being hashed, so the caller learns the exact digest before deciding whether
// the bytes may be trusted, without holding the whole object in memory.
package integrity

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
)

// Digest identifies content by length and SHA-256.
type Digest struct {
	SHA256 string `json:"sha256"`
	Size   int64  `json:"size"`
}

// ErrTooLarge is returned by Spool when the input exceeds the limit.
var ErrTooLarge = errors.New("content exceeds size limit")

// MismatchError describes content that does not match its expected digest.
type MismatchError struct {
	Expected Digest
	Actual   Digest
}

func (e *MismatchError) Error() string {
	if e.Expected.Size != e.Actual.Size {
		return fmt.Sprintf("size mismatch: expected %d bytes, got %d (sha256 expected %s, got %s)",
			e.Expected.Size, e.Actual.Size, short(e.Expected.SHA256), short(e.Actual.SHA256))
	}
	return fmt.Sprintf("checksum mismatch: expected sha256 %s, got %s", short(e.Expected.SHA256), short(e.Actual.SHA256))
}

func short(s string) string {
	if len(s) > 16 {
		return s[:16] + "…"
	}
	return s
}

// Verify returns nil if actual matches expected, otherwise a *MismatchError.
func Verify(expected, actual Digest) error {
	if expected.SHA256 != actual.SHA256 || expected.Size != actual.Size {
		return &MismatchError{Expected: expected, Actual: actual}
	}
	return nil
}

// ValidSHA256Hex reports whether s is a lowercase hex SHA-256 digest.
func ValidSHA256Hex(s string) bool {
	if len(s) != sha256.Size*2 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

// Compute hashes everything readable from r.
func Compute(r io.Reader) (Digest, error) {
	h := sha256.New()
	n, err := io.Copy(h, r)
	if err != nil {
		return Digest{}, err
	}
	return Digest{SHA256: hex.EncodeToString(h.Sum(nil)), Size: n}, nil
}

// HashingWriter wraps an io.Writer and hashes everything written through it.
type HashingWriter struct {
	w io.Writer
	h hash.Hash
	n int64
}

// NewHashingWriter returns a writer that forwards to w while hashing.
func NewHashingWriter(w io.Writer) *HashingWriter {
	return &HashingWriter{w: w, h: sha256.New()}
}

func (hw *HashingWriter) Write(p []byte) (int, error) {
	n, err := hw.w.Write(p)
	hw.h.Write(p[:n])
	hw.n += int64(n)
	return n, err
}

// Digest returns the digest of the bytes written so far.
func (hw *HashingWriter) Digest() Digest {
	return Digest{SHA256: hex.EncodeToString(hw.h.Sum(nil)), Size: hw.n}
}

// Spooled is content that has been written to a temporary file and hashed.
type Spooled struct {
	Digest Digest
	file   *os.File
}

// Spool streams r into a new temporary file in dir while hashing it.
//
// At most limit bytes are accepted; if r yields more, Spool returns
// ErrTooLarge. A negative limit means no limit. The caller must Close the
// result, which deletes the temporary file.
func Spool(r io.Reader, dir string, limit int64) (*Spooled, error) {
	f, err := os.CreateTemp(dir, "vault-spool-*")
	if err != nil {
		return nil, fmt.Errorf("create spool file: %w", err)
	}
	s := &Spooled{file: f}

	src := r
	if limit >= 0 {
		src = io.LimitReader(r, limit+1)
	}
	hw := NewHashingWriter(f)
	if _, err := io.Copy(hw, src); err != nil {
		s.Close()
		return nil, err
	}
	if limit >= 0 && hw.n > limit {
		s.Close()
		return nil, ErrTooLarge
	}
	s.Digest = hw.Digest()
	return s, nil
}

// Open returns an independent reader positioned at the start of the spooled
// content. Multiple readers may be open concurrently; each must be closed.
func (s *Spooled) Open() (io.ReadCloser, error) {
	return os.Open(s.file.Name())
}

// Close releases and deletes the temporary file.
func (s *Spooled) Close() error {
	name := s.file.Name()
	cerr := s.file.Close()
	rerr := os.Remove(name)
	if cerr != nil {
		return cerr
	}
	if rerr != nil && !errors.Is(rerr, os.ErrNotExist) {
		return rerr
	}
	return nil
}
