package integration

import (
	"bytes"
	"os/exec"
	"testing"
)

// The demo is itself a check: it exits non-zero if any step misbehaves.
func TestDemoCommand(t *testing.T) {
	out, err := exec.Command(vaultBin, "demo", "--no-color", "--dir", t.TempDir(), "--size", "262144").CombinedOutput()
	if err != nil {
		t.Fatalf("vault demo failed: %v\n%s", err, out)
	}
	for _, want := range []string{
		"3 / 3 ✓",
		"→ checksum mismatch ✗",
		"→ checksum verified ✓",
		"object retrieved successfully ✓",
		"integrity verified ✓",
		"event=CHECKSUM_MISMATCH",
		"object still retrievable and verified ✓",
	} {
		if !bytes.Contains(out, []byte(want)) {
			t.Errorf("demo output missing %q\n%s", want, out)
		}
	}
}

func TestSelfHealDemoCommands(t *testing.T) {
	cases := map[string][]string{
		"self-heal": {
			"5 / 5 nodes healthy ✓",
			"✓ all ", "CRITICAL objects are ahead of all ",
			"✓ every CRITICAL repair started before any DEGRADED repair",
			"24 / 24 objects read back byte-identical, sha256 verified ✓",
			"72 / 72 replicas present and verified on live nodes' disks ✓",
			"replication restored: 24 / 24 objects at 3/3 ✓",
		},
		"corruption-repair": {
			"NODE ALIVE ✓",
			"CHECKSUM MISMATCH ✗",
			"CORRUPTED   node: HEALTHY",
			"CHECKSUM VERIFIED ✓",
			"RESTORED",
			"HEALTHY 3/3",
		},
	}
	for scenario, wants := range cases {
		t.Run(scenario, func(t *testing.T) {
			out, err := exec.Command(vaultBin, "demo", scenario, "--no-color", "--dir", t.TempDir(), "--size", "131072").CombinedOutput()
			if err != nil {
				t.Fatalf("vault demo %s failed: %v\n%s", scenario, err, out)
			}
			for _, want := range wants {
				if !bytes.Contains(out, []byte(want)) {
					t.Errorf("demo %s output missing %q\n%s", scenario, want, out)
				}
			}
		})
	}
}
