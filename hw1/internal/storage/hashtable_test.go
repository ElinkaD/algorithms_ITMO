package storage

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

func openTable(t *testing.T, path string, buckets int) *HashTable {
	t.Helper()

	ht, err := Open(path, buckets)
	if err != nil {
		t.Fatalf("Open(%q, %d) failed: %v", path, buckets, err)
	}

	return ht
}

func assertErrIs(t *testing.T, err error, want error) {
	t.Helper()
	if !errors.Is(err, want) {
		t.Fatalf("expected error %v, got %v", want, err)
	}
}

func assertLookup(t *testing.T, ht *HashTable, key string, wantValue string, wantFound bool) {
	t.Helper()

	gotValue, gotFound, err := ht.Get(key)
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

func TestHashTable_PersistsDataBetweenReopen(t *testing.T) {
	tablePath := filepath.Join(t.TempDir(), "table")

	ht := openTable(t, tablePath, 8)
	if err := ht.Insert("brioshe", "100"); err != nil {
		t.Fatalf("insert brioshe failed: %v", err)
	}
	if err := ht.Insert("elina", "200"); err != nil {
		t.Fatalf("insert elina failed: %v", err)
	}
	if err := ht.Update("brioshe", "150"); err != nil {
		t.Fatalf("update brioshe failed: %v", err)
	}
	if err := ht.Delete("elina"); err != nil {
		t.Fatalf("delete elina failed: %v", err)
	}
	if err := ht.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	reopened := openTable(t, tablePath, 8)
	defer reopened.Close()

	assertLookup(t, reopened, "brioshe", "150", true)
	assertLookup(t, reopened, "elina", "", false)
}

func TestHashTable_ConflictAndMissingCases(t *testing.T) {
	ht := openTable(t, filepath.Join(t.TempDir(), "table"), 4)
	defer ht.Close()

	if err := ht.Insert("k", "v1"); err != nil {
		t.Fatalf("first insert failed: %v", err)
	}
	assertErrIs(t, ht.Insert("k", "v2"), ErrKeyExists)
	assertErrIs(t, ht.Update("missing", "v"), ErrKeyNotFound)
	assertErrIs(t, ht.Delete("missing"), ErrKeyNotFound)
}

func TestHashTable_EmptyKeyRejectedForAllOperations(t *testing.T) {
	ht := openTable(t, filepath.Join(t.TempDir(), "table"), 4)
	defer ht.Close()

	assertErrIs(t, ht.Insert("", "v"), ErrEmptyKey)
	assertErrIs(t, ht.Update("", "v"), ErrEmptyKey)
	assertErrIs(t, ht.Delete(""), ErrEmptyKey)

	_, _, err := ht.Get("")
	assertErrIs(t, err, ErrEmptyKey)
}

func TestHashTable_OpenValidation(t *testing.T) {
	t.Run("invalid buckets", func(t *testing.T) {
		_, err := Open(filepath.Join(t.TempDir(), "table"), 0)
		assertErrIs(t, err, ErrInvalidBucketsCount)
	})

	t.Run("existing table with different buckets count", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "table")
		_ = openTable(t, path, 8)

		_, err := Open(path, 16)
		assertErrIs(t, err, ErrBucketsCountMismatch)
	})
}

func TestHashTable_CloseDisablesFurtherOperations(t *testing.T) {
	ht := openTable(t, filepath.Join(t.TempDir(), "table"), 4)
	if err := ht.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	assertErrIs(t, ht.Insert("k", "v"), ErrTableClosed)
	assertErrIs(t, ht.Update("k", "v2"), ErrTableClosed)
	assertErrIs(t, ht.Delete("k"), ErrTableClosed)

	_, _, err := ht.Get("k")
	assertErrIs(t, err, ErrTableClosed)
}

func TestHashTable_DetectsCorruptedBucket(t *testing.T) {
	tablePath := filepath.Join(t.TempDir(), "table")
	ht := openTable(t, tablePath, 4)
	defer ht.Close()

	key := "broken"
	if err := ht.Insert(key, "value"); err != nil {
		t.Fatalf("insert failed: %v", err)
	}

	bucketPath := filepath.Join(tablePath, bucketFileName(ht.bucketIndex(key)))
	if err := os.WriteFile(bucketPath, []byte("{not-json"), 0o644); err != nil {
		t.Fatalf("failed to corrupt bucket file: %v", err)
	}

	_, _, err := ht.Get(key)
	assertErrIs(t, err, ErrBucketCorrupted)
}

func TestHashTable_RandomScenarioAgainstReferenceMap(t *testing.T) {
	const (
		buckets = 16
		steps   = 1200
		seed    = int64(20260313)
	)

	tablePath := filepath.Join(t.TempDir(), "table")
	ht := openTable(t, tablePath, buckets)

	rng := rand.New(rand.NewSource(seed))
	reference := make(map[string]string)

	for i := 0; i < steps; i++ {
		key := fmt.Sprintf("k_%d", rng.Intn(90))
		value := fmt.Sprintf("v_%d", i)

		switch rng.Intn(5) {
		case 0:
			if _, ok := reference[key]; ok {
				if err := ht.Update(key, value); err != nil {
					t.Fatalf("step=%d update failed for existing key: %v", i, err)
				}
			} else {
				if err := ht.Insert(key, value); err != nil {
					t.Fatalf("step=%d insert failed for new key: %v", i, err)
				}
			}
			reference[key] = value
		case 1:
			err := ht.Insert(key, value)
			_, exists := reference[key]
			if exists {
				assertErrIs(t, err, ErrKeyExists)
			} else {
				if err != nil {
					t.Fatalf("step=%d insert failed: %v", i, err)
				}
				reference[key] = value
			}
		case 2:
			err := ht.Update(key, value)
			_, exists := reference[key]
			if !exists {
				assertErrIs(t, err, ErrKeyNotFound)
			} else {
				if err != nil {
					t.Fatalf("step=%d update failed: %v", i, err)
				}
				reference[key] = value
			}
		case 3:
			err := ht.Delete(key)
			_, exists := reference[key]
			if !exists {
				assertErrIs(t, err, ErrKeyNotFound)
			} else {
				if err != nil {
					t.Fatalf("step=%d delete failed: %v", i, err)
				}
				delete(reference, key)
			}
		case 4:
			gotValue, gotFound, err := ht.Get(key)
			if err != nil {
				t.Fatalf("step=%d get failed: %v", i, err)
			}

			expectedValue, exists := reference[key]
			if gotFound != exists {
				t.Fatalf("step=%d found mismatch for key %q: want=%v got=%v", i, key, exists, gotFound)
			}
			if exists && gotValue != expectedValue {
				t.Fatalf("step=%d value mismatch for key %q: want=%q got=%q", i, key, expectedValue, gotValue)
			}
		}
	}

	if err := ht.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	reopened := openTable(t, tablePath, buckets)
	defer reopened.Close()

	for key, expected := range reference {
		got, found, err := reopened.Get(key)
		if err != nil {
			t.Fatalf("reopened Get(%q) failed: %v", key, err)
		}
		if !found || got != expected {
			t.Fatalf("reopened mismatch for key %q: found=%v expected=%q got=%q", key, found, expected, got)
		}
	}
}
