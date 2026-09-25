package placement

import (
	"errors"
	"fmt"
	"slices"
	"testing"
)

func TestPlaceDistinctAndDeterministic(t *testing.T) {
	nodes := []string{"node-1", "node-2", "node-3", "node-4", "node-5"}
	var r Rendezvous
	p1, err := r.Place("obj", nodes, 3)
	if err != nil {
		t.Fatal(err)
	}
	p2, _ := r.Place("obj", []string{"node-5", "node-3", "node-1", "node-4", "node-2"}, 3)
	if !slices.Equal(p1.Targets, p2.Targets) {
		t.Fatalf("placement depends on input order: %v vs %v", p1.Targets, p2.Targets)
	}
	all := append(slices.Clone(p1.Targets), p1.Spares...)
	slices.Sort(all)
	if !slices.Equal(all, nodes) || len(p1.Targets) != 3 {
		t.Fatalf("targets %v spares %v", p1.Targets, p1.Spares)
	}
}

func TestPlaceIgnoresDuplicates(t *testing.T) {
	_, err := Rendezvous{}.Place("obj", []string{"node-1", "node-1", "node-2"}, 3)
	var ie *InsufficientNodesError
	if !errors.As(err, &ie) || ie.Available != 2 || ie.Required != 3 {
		t.Fatalf("err = %v, want insufficient 2/3", err)
	}
}

func TestPlaceRejectsBadFactor(t *testing.T) {
	if _, err := (Rendezvous{}).Place("obj", []string{"node-1"}, 0); err == nil {
		t.Fatal("rf=0 accepted")
	}
}

func TestSpreadsPrimaries(t *testing.T) {
	nodes := []string{"node-1", "node-2", "node-3"}
	counts := map[string]int{}
	for i := 0; i < 3000; i++ {
		counts[Rank(fmt.Sprintf("obj-%d", i), nodes)[0]]++
	}
	for _, n := range nodes {
		if counts[n] < 800 {
			t.Fatalf("poor spread: %v", counts)
		}
	}
}
