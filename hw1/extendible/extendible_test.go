package extendible

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

func openTable(t testing.TB, path string, bucketCapacity int) *Table {
	t.Helper()

	table, err := Open(path, bucketCapacity)
	if err != nil {
		t.Fatalf("Open(%q, %d) failed: %v", path, bucketCapacity, err)
	}

	t.Cleanup(func() {
		if err := table.Close(); err != nil {
			t.Fatalf("Close failed: %v", err)
		}
	})

	return table
}

func assertErrIs(t testing.TB, err error, want error) {
	t.Helper()
	if !errors.Is(err, want) {
		t.Fatalf("expected error %v, got %v", want, err)
	}
}

func assertLookup(t testing.TB, table *Table, key string, wantValue string, wantFound bool) {
	t.Helper()

	gotValue, gotFound, err := table.Get(key)
	if err != nil {
		t.Fatalf("Get(%q) failed: %v", key, err)
	}
	if gotFound != wantFound {
		t.Fatalf("Get(%q) found mismatch: want=%v got=%v", key, wantFound, gotFound)
	}
	if wantFound && gotValue != wantValue {
		t.Fatalf("Get(%q) value mismatch: want=%q got=%q", key, wantValue, gotValue)
	}
}

func TestTable_PersistsDataBetweenReopen(t *testing.T) {
	tablePath := filepath.Join(t.TempDir(), "table")

	table, err := Open(tablePath, 2)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	if err := table.Insert("alice", "100"); err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	if err := table.Insert("bob", "200"); err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	if err := table.Insert("carol", "300"); err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	if err := table.Update("alice", "150"); err != nil {
		t.Fatalf("Update failed: %v", err)
	}
	if err := table.Delete("bob"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if err := table.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	reopened := openTable(t, tablePath, 2)
	assertLookup(t, reopened, "alice", "150", true)
	assertLookup(t, reopened, "bob", "", false)
	assertLookup(t, reopened, "carol", "300", true)
}

func TestTable_SplitsWhenBucketOverflows(t *testing.T) {
	table := openTable(t, filepath.Join(t.TempDir(), "table"), 2)

	keys := []string{"aa", "bb", "cc", "dd", "ee", "ff", "gg", "hh"}
	for i, key := range keys {
		if err := table.Insert(key, fmt.Sprintf("v%d", i)); err != nil {
			t.Fatalf("Insert(%q) failed: %v", key, err)
		}
	}

	stats, err := table.Stats()
	if err != nil {
		t.Fatalf("Stats failed: %v", err)
	}
	if stats.GlobalDepth <= 1 {
		t.Fatalf("expected split to increase global depth, got %d", stats.GlobalDepth)
	}
	if stats.MaxBucketLoad > stats.BucketCapacity {
		t.Fatalf("bucket overflow: max=%d capacity=%d", stats.MaxBucketLoad, stats.BucketCapacity)
	}
}

func TestTable_DeletesAndMerges(t *testing.T) {
	table := openTable(t, filepath.Join(t.TempDir(), "table"), 2)
	keys := []string{"aa", "bb", "cc", "dd", "ee", "ff", "gg", "hh"}

	for i, key := range keys {
		if err := table.Insert(key, fmt.Sprintf("v%d", i)); err != nil {
			t.Fatalf("Insert(%q) failed: %v", key, err)
		}
	}

	statsAfterInsert, err := table.Stats()
	if err != nil {
		t.Fatalf("Stats failed: %v", err)
	}

	for _, key := range keys {
		if err := table.Delete(key); err != nil {
			t.Fatalf("Delete(%q) failed: %v", key, err)
		}
	}

	statsAfterDelete, err := table.Stats()
	if err != nil {
		t.Fatalf("Stats failed: %v", err)
	}
	if statsAfterDelete.KeysCount != 0 {
		t.Fatalf("expected empty table, got size=%d", statsAfterDelete.KeysCount)
	}
	if statsAfterDelete.GlobalDepth > statsAfterInsert.GlobalDepth {
		t.Fatalf("expected directory to stay same size or shrink")
	}
	if statsAfterDelete.GlobalDepth != 1 {
		t.Fatalf("expected shrink back to initial depth 1, got %d", statsAfterDelete.GlobalDepth)
	}
	if statsAfterDelete.BucketsCount != 2 {
		t.Fatalf("expected two base buckets after shrink, got %d", statsAfterDelete.BucketsCount)
	}
}

func TestTable_OpenValidation(t *testing.T) {
	t.Run("invalid path", func(t *testing.T) {
		_, err := Open(" ", 2)
		assertErrIs(t, err, ErrInvalidPath)
	})

	t.Run("invalid capacity", func(t *testing.T) {
		_, err := Open(filepath.Join(t.TempDir(), "table"), 0)
		assertErrIs(t, err, ErrInvalidBucketCapacity)
	})

	t.Run("capacity mismatch", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "table")
		table, err := Open(path, 2)
		if err != nil {
			t.Fatalf("Open failed: %v", err)
		}
		if err := table.Close(); err != nil {
			t.Fatalf("Close failed: %v", err)
		}

		_, err = Open(path, 4)
		assertErrIs(t, err, ErrBucketCapacityMismatch)
	})
}

func TestTable_CorruptedBucket(t *testing.T) {
	path := filepath.Join(t.TempDir(), "table")
	table := openTable(t, path, 2)
	if err := table.Insert("broken", "value"); err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	index := table.directoryIndex("broken")
	bucketID := table.meta.Directory[index]
	if err := os.WriteFile(table.bucketPath(bucketID), []byte("{broken"), 0o644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	_, _, err := table.Get("broken")
	assertErrIs(t, err, ErrBucketCorrupted)
}

// Просто длинный случайный сценарий против map.
func TestTable_RandomScenarioAgainstMap(t *testing.T) {
	seeds := []int64{1, 42, 20260325}

	for _, seed := range seeds {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			table := openTable(t, filepath.Join(t.TempDir(), "table"), 4)
			reference := make(map[string]string)
			rng := rand.New(rand.NewSource(seed))

			for step := 0; step < 2000; step++ {
				key := fmt.Sprintf("k_%d", rng.Intn(200))
				value := fmt.Sprintf("v_%d_%d", step, rng.Int())

				switch rng.Intn(4) {
				case 0:
					err := table.Insert(key, value)
					_, exists := reference[key]
					if exists {
						assertErrIs(t, err, ErrKeyExists)
					} else {
						if err != nil {
							t.Fatalf("Insert(%q) failed: %v", key, err)
						}
						reference[key] = value
					}
				case 1:
					err := table.Update(key, value)
					_, exists := reference[key]
					if exists {
						if err != nil {
							t.Fatalf("Update(%q) failed: %v", key, err)
						}
						reference[key] = value
					} else {
						assertErrIs(t, err, ErrKeyNotFound)
					}
				case 2:
					err := table.Delete(key)
					_, exists := reference[key]
					if exists {
						if err != nil {
							t.Fatalf("Delete(%q) failed: %v", key, err)
						}
						delete(reference, key)
					} else {
						assertErrIs(t, err, ErrKeyNotFound)
					}
				case 3:
					got, found, err := table.Get(key)
					if err != nil {
						t.Fatalf("Get(%q) failed: %v", key, err)
					}
					want, exists := reference[key]
					if found != exists {
						t.Fatalf("Get(%q) found mismatch: got=%v want=%v", key, found, exists)
					}
					if exists && got != want {
						t.Fatalf("Get(%q) value mismatch: got=%q want=%q", key, got, want)
					}
				}

				if step%250 == 0 {
					assertTableMatchesReference(t, table, reference)
				}
			}

			assertTableMatchesReference(t, table, reference)
		})
	}
}

func assertTableMatchesReference(t testing.TB, table *Table, reference map[string]string) {
	t.Helper()

	stats, err := table.Stats()
	if err != nil {
		t.Fatalf("Stats failed: %v", err)
	}
	if stats.KeysCount != len(reference) {
		t.Fatalf("table size mismatch: got=%d want=%d", stats.KeysCount, len(reference))
	}
	if stats.MaxBucketLoad > stats.BucketCapacity {
		t.Fatalf("bucket overflow: max=%d capacity=%d", stats.MaxBucketLoad, stats.BucketCapacity)
	}

	for key, want := range reference {
		got, found, err := table.Get(key)
		if err != nil {
			t.Fatalf("Get(%q) failed: %v", key, err)
		}
		if !found || got != want {
			t.Fatalf("Get(%q) mismatch: found=%v got=%q want=%q", key, found, got, want)
		}
	}
}
