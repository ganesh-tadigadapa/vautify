package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"vault/internal/cluster"
	"vault/internal/coordinator"
	"vault/internal/events"
	"vault/internal/storage"
)

// fileState is one file or directory as found on disk. Directory size and
// mtime change whenever an entry is created or removed inside it, so even a
// temp file that was written and cleaned up again shows as a change.
type fileState struct {
	Dir     bool
	Size    int64
	ModNS   int64
	Content string // SHA-256 of a file's bytes
}

// diskState records every file and directory under roots, keyed by path.
func diskState(t *testing.T, roots ...string) map[string]fileState {
	t.Helper()
	out := map[string]fileState{}
	for _, root := range roots {
		err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			info, err := d.Info()
			if err != nil {
				return err
			}
			st := fileState{Dir: d.IsDir(), Size: info.Size(), ModNS: info.ModTime().UnixNano()}
			if !d.IsDir() {
				b, err := os.ReadFile(path)
				if err != nil {
					return err
				}
				st.Content = sha(b)
			}
			out[path] = st
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	return out
}

func diffDiskState(before, after map[string]fileState) []string {
	var out []string
	for p, b := range before {
		a, ok := after[p]
		switch {
		case !ok:
			out = append(out, "removed "+p)
		case a != b:
			out = append(out, fmt.Sprintf("changed %s: %+v -> %+v", p, b, a))
		}
	}
	for p := range after {
		if _, ok := before[p]; !ok {
			out = append(out, "created "+p)
		}
	}
	slices.Sort(out)
	return out
}

// A PUT or DELETE whose expected_version is wrong must be refused with 409
// write_conflict and leave everything exactly as it was: no replica bytes
// written, rewritten or removed on any node (every file and directory under
// every node's data dir is compared by content, size and mtime), and the
// object's metadata unchanged both over the API and in its file on disk.
func TestFailedCASWritesNoReplicasAndKeepsMetadata(t *testing.T) {
	e := startCluster(t, 3, 3)
	v2 := randomBytes(64<<10, 2)
	e.put("doc", randomBytes(64<<10, 1))
	obj := e.put("doc", v2)
	if obj.Version != 2 {
		t.Fatalf("setup: doc is v%d, want v2", obj.Version)
	}

	var roots []string
	for _, n := range e.c.Nodes {
		roots = append(roots, n.DataDir)
	}
	metaDir := filepath.Join(e.c.API.DataDir, "metadata")
	roots = append(roots, metaDir)
	before := diskState(t, roots...)
	metaBefore := e.do(http.MethodGet, "/objects/doc/metadata", nil)
	e.mustStatus(metaBefore, http.StatusOK, "metadata before")

	// The snapshot must actually cover the replicas and the metadata file.
	for _, n := range obj.Replicas {
		if st, ok := before[e.replicaPath(n, "doc", 2)]; !ok || st.Content != sha(v2) {
			t.Fatalf("snapshot lacks the v2 replica on %s", n)
		}
	}
	if _, ok := before[filepath.Join(metaDir, "doc.json")]; !ok {
		t.Fatal("snapshot lacks doc.json")
	}

	attempts := []struct {
		name, method, op string
		expected         uint64
		body             []byte
	}{
		{"PUT with stale expected_version", http.MethodPut, "put", 1, randomBytes(64<<10, 3)},
		{"PUT with future expected_version", http.MethodPut, "put", 7, randomBytes(64<<10, 4)},
		{"PUT create-only on a live object", http.MethodPut, "put", 0, randomBytes(64<<10, 5)},
		{"DELETE with stale expected_version", http.MethodDelete, "delete", 1, nil},
		{"DELETE with future expected_version", http.MethodDelete, "delete", 7, nil},
	}
	for _, a := range attempts {
		r := e.do(a.method, "/objects/doc?expected_version="+strconv.FormatUint(a.expected, 10), a.body)
		e.mustStatus(r, http.StatusConflict, a.name)
		var body struct {
			Error struct {
				Code    string
				Details coordinator.ConflictError
			}
		}
		if err := json.Unmarshal(r.Body, &body); err != nil {
			t.Fatalf("%s: %v: %s", a.name, err, r.Body)
		}
		d := body.Error.Details
		if body.Error.Code != "write_conflict" || d.ObjectID != "doc" || d.Operation != a.op ||
			d.ExpectedVersion != a.expected || d.CurrentVersion != 2 || d.CurrentState != "live" {
			t.Fatalf("%s: conflict body %s", a.name, r.Body)
		}
		if diff := diffDiskState(before, diskState(t, roots...)); len(diff) > 0 {
			t.Fatalf("%s changed disk state:\n%s", a.name, strings.Join(diff, "\n"))
		}
		m := e.do(http.MethodGet, "/objects/doc/metadata", nil)
		e.mustStatus(m, http.StatusOK, "metadata after "+a.name)
		if !bytes.Equal(m.Body, metaBefore.Body) {
			t.Fatalf("%s changed metadata:\nbefore %s\nafter  %s", a.name, metaBefore.Body, m.Body)
		}
	}
	if n := len(e.logEvents(e.c.API, "WRITE_CONFLICT")); n != len(attempts) {
		t.Fatalf("WRITE_CONFLICT events = %d, want %d", n, len(attempts))
	}

	r := e.do(http.MethodGet, "/objects/doc", nil)
	e.mustStatus(r, http.StatusOK, "read after failed CAS")
	if !bytes.Equal(r.Body, v2) || r.Header.Get(storage.HeaderVersion) != "2" {
		t.Fatalf("read after failed CAS: v%s, %d bytes", r.Header.Get(storage.HeaderVersion), len(r.Body))
	}

	// Control: a CAS with the right version does change disk state, so the
	// no-change checks above are not vacuous.
	e.mustStatus(e.do(http.MethodPut, "/objects/doc?expected_version=2", randomBytes(64<<10, 6)), http.StatusOK, "correct CAS")
	if len(diffDiskState(before, diskState(t, roots...))) == 0 {
		t.Fatal("a successful CAS write left disk state unchanged; the snapshot sees nothing")
	}
}

// replicaWriteLines counts the log lines of process p that record a replica
// write to object id: a completed write (CHECKSUM_VERIFIED at stage
// replica_write), a rejected one (CHECKSUM_MISMATCH at that stage), or one
// that failed or was cut off mid-stream.
func (e *env) replicaWriteLines(p *cluster.Process, id string) int {
	e.t.Helper()
	b, err := os.ReadFile(p.LogPath)
	if err != nil {
		e.t.Fatal(err)
	}
	n := 0
	for _, line := range bytes.Split(b, []byte("\n")) {
		var m map[string]any
		if json.Unmarshal(line, &m) != nil || m["object_id"] != id {
			continue
		}
		if m["stage"] == "replica_write" || m["msg"] == "replica write failed" || m["msg"] == "replica stream interrupted" {
			n++
		}
	}
	return n
}

// A failed CAS must make zero replica writes. This is asserted directly,
// not inferred from the coordinator's control flow: the coordinator must
// not attempt a single OBJECT_REPLICA_WRITE, no node may log a replica write
// of any outcome, and the object's metadata file must be byte-for-byte
// identical afterwards.
func TestFailedCASMakesZeroReplicaWrites(t *testing.T) {
	e := startCluster(t, 3, 3)
	e.put("doc", randomBytes(32<<10, 1))

	metaFile := filepath.Join(e.c.API.DataDir, "metadata", "doc.json")
	metaBefore, err := os.ReadFile(metaFile)
	if err != nil {
		t.Fatal(err)
	}
	counts := func() (coord int, nodes map[string]int) {
		nodes = map[string]int{}
		for _, n := range e.c.Nodes {
			nodes[n.Name] = e.replicaWriteLines(n, "doc")
		}
		return len(e.logEvents(e.c.API, string(events.ObjectReplicaWrite))), nodes
	}
	coordBefore, nodesBefore := counts()
	if coordBefore != 3 {
		t.Fatalf("setup: %d OBJECT_REPLICA_WRITE events for the first put, want 3", coordBefore)
	}
	total := 0
	for _, n := range nodesBefore {
		total += n
	}
	if total != 3 {
		t.Fatalf("setup: nodes logged %d replica writes for the first put, want 3: %v", total, nodesBefore)
	}

	for _, expected := range []string{"0", "2", "9"} {
		r := e.do(http.MethodPut, "/objects/doc?expected_version="+expected, randomBytes(32<<10, 2))
		e.mustStatus(r, http.StatusConflict, "PUT with expected_version="+expected)

		coord, nodes := counts()
		if coord != coordBefore {
			t.Fatalf("expected_version=%s: coordinator attempted %d replica writes, want 0", expected, coord-coordBefore)
		}
		for name, n := range nodes {
			if n != nodesBefore[name] {
				t.Fatalf("expected_version=%s: %s logged %d replica writes, want 0", expected, name, n-nodesBefore[name])
			}
		}
		metaAfter, err := os.ReadFile(metaFile)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(metaAfter, metaBefore) {
			t.Fatalf("expected_version=%s changed %s:\nbefore %s\nafter  %s", expected, metaFile, metaBefore, metaAfter)
		}
	}

	// Control: the right expected_version writes, and the same counters see
	// it, so the zero-write checks above are not vacuous.
	e.mustStatus(e.do(http.MethodPut, "/objects/doc?expected_version=1", randomBytes(32<<10, 3)), http.StatusOK, "correct CAS")
	coord, nodes := counts()
	total = 0
	for name, n := range nodes {
		total += n - nodesBefore[name]
	}
	if coord-coordBefore != 3 || total != 3 {
		t.Fatalf("correct CAS: coordinator %d, nodes %d replica writes, want 3 and 3", coord-coordBefore, total)
	}
	if metaAfter, _ := os.ReadFile(metaFile); bytes.Equal(metaAfter, metaBefore) {
		t.Fatal("correct CAS left doc.json unchanged; the byte comparison sees nothing")
	}
}
