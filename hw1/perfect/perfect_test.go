package perfect

import (
	"errors"
	"fmt"
	"math/rand"
	"testing"
)

func TestBuildAndLookup(t *testing.T) {
	table, err := Build([]Entry{
		{Key: "apple", Value: "1"},
		{Key: "banana", Value: "2"},
		{Key: "cherry", Value: "3"},
	})
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	checks := []struct {
		key   string
		value string
		found bool
	}{
		{key: "apple", value: "1", found: true},
		{key: "banana", value: "2", found: true},
		{key: "cherry", value: "3", found: true},
		{key: "mango", value: "", found: false},
	}

	for _, check := range checks {
		got, found := table.Get(check.key)
		if found != check.found {
			t.Fatalf("Get(%q) found mismatch: got=%v want=%v", check.key, found, check.found)
		}
		if found && got != check.value {
			t.Fatalf("Get(%q) value mismatch: got=%q want=%q", check.key, got, check.value)
		}
	}
}

func TestBuildRejectsInvalidInput(t *testing.T) {
	_, err := Build([]Entry{{Key: "", Value: "v"}})
	if !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("expected ErrEmptyKey, got %v", err)
	}

	_, err = Build([]Entry{
		{Key: "dup", Value: "1"},
		{Key: "dup", Value: "2"},
	})
	if !errors.Is(err, ErrDuplicateKey) {
		t.Fatalf("expected ErrDuplicateKey, got %v", err)
	}
}

func TestBuildEmptySet(t *testing.T) {
	table, err := Build(nil)
	if err != nil {
		t.Fatalf("Build(nil) failed: %v", err)
	}

	if _, found := table.Get("missing"); found {
		t.Fatalf("expected empty table to return not found")
	}

	stats := table.Stats()
	if stats.KeysCount != 0 || stats.TotalSlots != 0 {
		t.Fatalf("unexpected stats for empty table: %+v", stats)
	}
}

func TestBuildRandomAgainstMap(t *testing.T) {
	seeds := []int64{1, 7, 42, 20260325}
	sizes := []int{10, 100, 1000}

	for _, seed := range seeds {
		for _, size := range sizes {
			t.Run(fmt.Sprintf("seed=%d/size=%d", seed, size), func(t *testing.T) {
				entries, reference := randomEntries(seed, size)
				table, err := Build(entries)
				if err != nil {
					t.Fatalf("Build failed: %v", err)
				}

				for key, want := range reference {
					got, found := table.Get(key)
					if !found || got != want {
						t.Fatalf("Get(%q) mismatch: found=%v got=%q want=%q", key, found, got, want)
					}
				}

				if _, found := table.Get("missing-key-123"); found {
					t.Fatalf("expected missing key to be absent")
				}

				stats := table.Stats()
				if stats.KeysCount != size {
					t.Fatalf("KeysCount mismatch: got=%d want=%d", stats.KeysCount, size)
				}
				if stats.MaxBucketSize <= 0 {
					t.Fatalf("expected MaxBucketSize > 0")
				}
				if stats.SpaceAmplification < 1.0 {
					t.Fatalf("expected SpaceAmplification >= 1, got %f", stats.SpaceAmplification)
				}
			})
		}
	}
}

func FuzzTableAgainstMap(f *testing.F) {
	f.Add(int64(1), uint16(64))
	f.Add(int64(7), uint16(200))
	f.Add(int64(20260325), uint16(512))

	f.Fuzz(func(t *testing.T, seed int64, size uint16) {
		n := int(size%1500) + 1
		entries, reference := randomEntries(seed, n)

		table, err := Build(entries)
		if err != nil {
			t.Fatalf("Build failed: %v", err)
		}

		for key, want := range reference {
			got, found := table.Get(key)
			if !found || got != want {
				t.Fatalf("Get(%q) mismatch: found=%v got=%q want=%q", key, found, got, want)
			}
		}
	})
}

func randomEntries(seed int64, size int) ([]Entry, map[string]string) {
	rng := rand.New(rand.NewSource(seed))
	reference := make(map[string]string, size)
	entries := make([]Entry, 0, size)

	for len(entries) < size {
		key := fmt.Sprintf("k-%08d-%016x", len(entries), rng.Uint64())
		if _, exists := reference[key]; exists {
			continue
		}
		value := fmt.Sprintf("v-%08d-%016x", len(entries), rng.Uint64())
		reference[key] = value
		entries = append(entries, Entry{Key: key, Value: value})
	}

	return entries, reference
}
