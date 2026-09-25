package node

import (
	"bytes"
	"errors"
	"io"
	"os"
	"strings"
	"testing"

	"vault/internal/integrity"
	"vault/internal/storage"
)

func digestOf(t *testing.T, b []byte) integrity.Digest {
	t.Helper()
	d, err := integrity.Compute(bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestPutOpenVerifyDelete(t *testing.T) {
	root := t.TempDir()
	s, err := OpenStore(root, "node-1")
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("hello vault")
	info, err := s.Put("obj", 1, digestOf(t, data), bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if info.NodeID != "node-1" || info.Size != int64(len(data)) {
		t.Fatalf("unexpected info %+v", info)
	}

	onDisk, err := os.ReadFile(ReplicaDataPath(root, "obj", 1))
	if err != nil {
		t.Fatalf("replica bytes not at expected path: %v", err)
	}
	if !bytes.Equal(onDisk, data) {
		t.Fatal("on-disk bytes differ from written bytes")
	}

	f, _, err := s.Open("obj", 1)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(f)
	f.Close()
	if !bytes.Equal(got, data) {
		t.Fatal("Open returned different bytes")
	}

	res, err := s.Verify("obj", 1)
	if err != nil || !res.Valid {
		t.Fatalf("Verify = %+v, %v; want valid", res, err)
	}

	c, _ := s.Capacity()
	if c.ReplicaCount != 1 || c.UsedBytes != int64(len(data)) {
		t.Fatalf("capacity = %+v", c)
	}

	if err := s.Delete("obj", 1); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Open("obj", 1); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("after delete Open err = %v, want ErrNotFound", err)
	}
	if err := s.Delete("obj", 1); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("second delete err = %v, want ErrNotFound", err)
	}
	c, _ = s.Capacity()
	if c.ReplicaCount != 0 || c.UsedBytes != 0 {
		t.Fatalf("capacity after delete = %+v", c)
	}
}

func TestPutRejectsChecksumMismatch(t *testing.T) {
	root := t.TempDir()
	s, _ := OpenStore(root, "node-1")
	want := digestOf(t, []byte("expected"))

	for _, body := range []string{"tampered", "expected-but-longer", "exp"} {
		_, err := s.Put("obj", 1, want, strings.NewReader(body))
		var mm *integrity.MismatchError
		if !errors.Is(err, storage.ErrRejected) || !errors.As(err, &mm) {
			t.Fatalf("body %q: err = %v, want ErrRejected+MismatchError", body, err)
		}
	}
	if _, err := os.Stat(ReplicaDataPath(root, "obj", 1)); !os.IsNotExist(err) {
		t.Fatal("rejected replica was committed to disk")
	}
	tmp, _ := os.ReadDir(root + "/tmp")
	if len(tmp) != 0 {
		t.Fatalf("temp files leaked: %d", len(tmp))
	}
}

func TestVerifyDetectsOnDiskCorruption(t *testing.T) {
	root := t.TempDir()
	s, _ := OpenStore(root, "node-1")
	data := []byte("pristine bytes")
	if _, err := s.Put("obj", 1, digestOf(t, data), bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(ReplicaDataPath(root, "obj", 1), []byte("pristine bytez"), 0o644); err != nil {
		t.Fatal(err)
	}
	res, err := s.Verify("obj", 1)
	if err != nil {
		t.Fatal(err)
	}
	if res.Valid {
		t.Fatal("corruption not detected by Verify")
	}
}

func TestReopenPreservesReplicasAndUsage(t *testing.T) {
	root := t.TempDir()
	s, _ := OpenStore(root, "node-1")
	for i, body := range []string{"one", "two-two"} {
		b := []byte(body)
		if _, err := s.Put("obj", uint64(i+1), digestOf(t, b), bytes.NewReader(b)); err != nil {
			t.Fatal(err)
		}
	}
	s2, err := OpenStore(root, "node-1")
	if err != nil {
		t.Fatal(err)
	}
	c, _ := s2.Capacity()
	if c.ReplicaCount != 2 || c.UsedBytes != int64(len("one")+len("two-two")) {
		t.Fatalf("capacity after reopen = %+v", c)
	}
	res, err := s2.Verify("obj", 2)
	if err != nil || !res.Valid {
		t.Fatalf("verify after reopen = %+v, %v", res, err)
	}
	n, err := s2.DeleteAll("obj")
	if err != nil || n != 2 {
		t.Fatalf("DeleteAll = %d, %v; want 2", n, err)
	}
}

func TestRefusesForeignDataDirectory(t *testing.T) {
	root := t.TempDir()
	if _, err := OpenStore(root, "node-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenStore(root, "node-2"); err == nil {
		t.Fatal("node-2 opened node-1's data directory")
	}
}

func TestRejectsUnsafeObjectIDs(t *testing.T) {
	s, _ := OpenStore(t.TempDir(), "node-1")
	for _, id := range []string{"../escape", "a/b", "..", ""} {
		if _, err := s.Put(id, 1, digestOf(t, nil), bytes.NewReader(nil)); err == nil {
			t.Fatalf("Put accepted unsafe id %q", id)
		}
	}
}

func TestOlderVersionNeverOverwritesNewer(t *testing.T) {
	root := t.TempDir()
	s, _ := OpenStore(root, "node-1")
	put := func(v uint64, body string) error {
		b := []byte(body)
		_, err := s.Put("obj", v, digestOf(t, b), bytes.NewReader(b))
		return err
	}
	if err := put(2, "two"); err != nil {
		t.Fatal(err)
	}
	if err := put(1, "one"); !errors.Is(err, storage.ErrStaleVersion) {
		t.Fatalf("v1 over v2: %v, want ErrStaleVersion", err)
	}
	if _, err := os.Stat(ReplicaDataPath(root, "obj", 1)); !os.IsNotExist(err) {
		t.Fatal("stale write was committed")
	}
	if err := put(2, "two again"); err != nil {
		t.Fatalf("same-version rewrite (in-place repair) refused: %v", err)
	}
	if err := put(3, "three"); err != nil {
		t.Fatal(err)
	}
	list, err := s.List("obj")
	if err != nil || len(list) != 2 || list[0].Version != 2 || list[1].Version != 3 || list[1].SHA256 != digestOf(t, []byte("three")).SHA256 {
		t.Fatalf("List = %+v %v", list, err)
	}
	inv, _ := s.Inventory()
	if len(inv) != 2 {
		t.Fatalf("Inventory = %+v", inv)
	}

	// A data file whose record names a different version is not served.
	os.WriteFile(ReplicaDataPath(root, "obj", 3)[:len(ReplicaDataPath(root, "obj", 3))-len(".data")]+".json",
		[]byte(`{"object_id":"obj","version":2,"sha256":"x","size":3}`), 0o644)
	if _, _, err := s.Open("obj", 3); err == nil {
		t.Fatal("replica with mismatched version record was served")
	}
}
