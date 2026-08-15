package main

import (
	"fmt"
	"testing"
)

// TestRemoveNodeClearsStaleRingReferences reproduces a memory leak in
// RemoveNode: the in-place filter (kept := s.ring[:0]; append...) compacts the
// surviving entries to the front of the underlying array, but the trailing
// slots between the new length and the old capacity keep their old ringEntry
// values. Because ringEntry holds a `node string` field, those slots retain a
// reference to the removed node's name, pinning the string and preventing the
// GC from reclaiming it. Under repeated add/remove churn this leaks memory.
//
// The expected behavior is that, after a node is removed, no slot in the
// retained backing array (up to its capacity) still references the removed
// node, so the GC is free to collect it.
func TestRemoveNodeClearsStaleRingReferences(t *testing.T) {
	s := NewService()

	// A few survivor nodes plus the one we will remove.
	for _, n := range []string{"keepA", "leak", "keepB"} {
		if _, err := s.AddNode(n, 64); err != nil {
			t.Fatalf("add %s: %v", n, err)
		}
	}

	// Sanity: the ring is populated.
	if got := len(s.ring); got != 192 {
		t.Fatalf("ring length before removal = %d, want 192", got)
	}

	if err := s.RemoveNode("leak"); err != nil {
		t.Fatalf("remove leak: %v", err)
	}

	// The live slice must no longer reference the removed node...
	if len(s.ring) != 128 {
		t.Fatalf("ring length after removal = %d, want 128", len(s.ring))
	}
	for _, e := range s.ring {
		if e.node == "leak" {
			t.Fatalf("live ring still references removed node %q", "leak")
		}
	}

	// ...and neither may the retained backing array (the slots between the new
	// length and the capacity, which the in-place filter leaves untouched).
	// These slots are exactly what keeps the stale string alive.
	full := s.ring[:cap(s.ring)]
	leaked := 0
	for i := len(s.ring); i < len(full); i++ {
		if full[i].node == "leak" {
			leaked++
		}
	}
	if leaked != 0 {
		t.Fatalf("found %d stale references to removed node %q in the backing "+
			"array tail (len=%d cap=%d); RemoveNode must clear discarded slots "+
			"so the GC can reclaim them", leaked, "leak", len(s.ring), cap(s.ring))
	}
}

// TestRemoveNodeChurnNoStaleReferences checks the leak does not accumulate over
// repeated add/remove churn, mirroring the operational scenario described in
// the bug report (frequent node add/remove causing sustained memory growth).
func TestRemoveNodeChurnNoStaleReferences(t *testing.T) {
	s := NewService()

	// Seed survivors so the ring always has live entries to keep around.
	for _, n := range []string{"stable1", "stable2"} {
		if _, err := s.AddNode(n, 32); err != nil {
			t.Fatalf("add %s: %v", n, err)
		}
	}

	for i := 0; i < 50; i++ {
		name := fmt.Sprintf("churn-%d", i)
		if _, err := s.AddNode(name, 32); err != nil {
			t.Fatalf("iter %d add %s: %v", i, name, err)
		}
		if err := s.RemoveNode(name); err != nil {
			t.Fatalf("iter %d remove %s: %v", i, name, err)
		}

		// After every removal the entire retained backing array must be free of
		// references to the node just removed.
		full := s.ring[:cap(s.ring)]
		for j := 0; j < len(full); j++ {
			if full[j].node == name {
				t.Fatalf("iter %d: stale reference to %q survives in backing "+
					"array (slot %d, len=%d cap=%d)", i, name, j, len(s.ring), cap(s.ring))
			}
		}
	}
}

// TestRemoveNodeKeepsRingConsistent ensures the leak fix does not corrupt ring
// ordering or ownership: after removing one node among several, every key that
// was not owned by the removed node keeps its owner (the minimal-migration
// invariant), and the surviving ring is still sorted by hash.
func TestRemoveNodeKeepsRingConsistent(t *testing.T) {
	s := NewService()
	for _, n := range []string{"a", "b", "c", "d", "e"} {
		if _, err := s.AddNode(n, 48); err != nil {
			t.Fatalf("add %s: %v", n, err)
		}
	}

	keys := make([]string, 0, 300)
	for i := 0; i < 300; i++ {
		keys = append(keys, fmt.Sprintf("obj-%d", i))
	}
	before, err := s.Owners(keys)
	if err != nil {
		t.Fatalf("owners before: %v", err)
	}

	if err := s.RemoveNode("c"); err != nil {
		t.Fatalf("remove c: %v", err)
	}
	after, err := s.Owners(keys)
	if err != nil {
		t.Fatalf("owners after: %v", err)
	}

	for _, k := range keys {
		b, a := before[k], after[k]
		if b == "c" {
			if a == "c" {
				t.Fatalf("key %q still owned by removed node c", k)
			}
			continue
		}
		if a != b {
			t.Fatalf("minimal migration violated: key %q was %q, now %q", k, b, a)
		}
	}

	// The surviving ring must remain sorted ascending by hash.
	for i := 1; i < len(s.ring); i++ {
		if s.ring[i-1].hash > s.ring[i].hash {
			t.Fatalf("ring not sorted after removal at index %d: %d > %d",
				i, s.ring[i-1].hash, s.ring[i].hash)
		}
	}
}
