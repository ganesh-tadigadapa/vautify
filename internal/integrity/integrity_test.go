package integrity

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
)

func sum(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

func TestSpoolHashesAndRereads(t *testing.T) {
	data := bytes.Repeat([]byte("vault"), 100_000)
	s, err := Spool(bytes.NewReader(data), t.TempDir(), -1)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if s.Digest.SHA256 != sum(data) || s.Digest.Size != int64(len(data)) {
		t.Fatalf("digest = %+v, want sha %s size %d", s.Digest, sum(data), len(data))
	}
	for i := 0; i < 2; i++ { // two independent readers
		r, err := s.Open()
		if err != nil {
			t.Fatal(err)
		}
		got, _ := io.ReadAll(r)
		r.Close()
		if !bytes.Equal(got, data) {
			t.Fatalf("reader %d returned different bytes", i)
		}
	}
}

func TestSpoolLimit(t *testing.T) {
	dir := t.TempDir()
	if _, err := Spool(strings.NewReader("12345"), dir, 4); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("err = %v, want ErrTooLarge", err)
	}
	s, err := Spool(strings.NewReader("1234"), dir, 4)
	if err != nil {
		t.Fatalf("exactly-at-limit input rejected: %v", err)
	}
	s.Close()
	entries, _ := os.ReadDir(dir)
	if len(entries) != 0 {
		t.Fatalf("spool files leaked: %d", len(entries))
	}
}

func TestVerify(t *testing.T) {
	good := Digest{SHA256: sum([]byte("a")), Size: 1}
	if err := Verify(good, good); err != nil {
		t.Fatal(err)
	}
	bad := Digest{SHA256: sum([]byte("b")), Size: 1}
	var mm *MismatchError
	if err := Verify(good, bad); !errors.As(err, &mm) {
		t.Fatalf("err = %v, want *MismatchError", err)
	}
	if err := Verify(good, Digest{SHA256: good.SHA256, Size: 2}); !errors.As(err, &mm) {
		t.Fatal("size mismatch not detected")
	}
}

func TestValidSHA256Hex(t *testing.T) {
	if !ValidSHA256Hex(sum(nil)) {
		t.Fatal("valid digest rejected")
	}
	for _, s := range []string{"", "abc", strings.ToUpper(sum(nil)), sum(nil)[:63] + "g"} {
		if ValidSHA256Hex(s) {
			t.Fatalf("invalid digest %q accepted", s)
		}
	}
}
