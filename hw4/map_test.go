package hw4

import (
	"slices"
	"sync"
	"testing"
	"testing/synctest"
)

func TestMapBasicOperations(t *testing.T) {
	m := New[int, string](8, IntHasher)

	if got := m.Size(); got != 0 {
		t.Fatalf("initial Size() = %d, want 0", got)
	}

	m.Put(1, "one")
	m.Put(2, "two")
	m.Put(1, "uno")

	if got := m.Size(); got != 2 {
		t.Fatalf("Size() after Put = %d, want 2", got)
	}

	value, ok := m.Get(1)
	if !ok || value != "uno" {
		t.Fatalf("Get(1) = (%q, %v), want (\"uno\", true)", value, ok)
	}

	if _, ok := m.Get(42); ok {
		t.Fatalf("Get(42) unexpectedly found a value")
	}

	merged := m.Merge(1, "!", func(current, incoming string) string {
		return current + incoming
	})
	if merged != "uno!" {
		t.Fatalf("Merge existing returned %q, want %q", merged, "uno!")
	}

	inserted := m.Merge(3, "three", func(current, incoming string) string {
		return current + incoming
	})
	if inserted != "three" {
		t.Fatalf("Merge insert returned %q, want %q", inserted, "three")
	}

	if got := m.Size(); got != 3 {
		t.Fatalf("Size() after Merge = %d, want 3", got)
	}

	m.Clear()
	if got := m.Size(); got != 0 {
		t.Fatalf("Size() after Clear = %d, want 0", got)
	}

	if _, ok := m.Get(1); ok {
		t.Fatalf("Get(1) unexpectedly found a value after Clear")
	}
}

func TestMapIteratorSnapshot(t *testing.T) {
	m := New[int, string](4, func(key int) uint64 { return uint64(key % 2) })
	m.Put(1, "one")
	m.Put(2, "two")
	m.Put(3, "three")

	it := m.Iterator()
	m.Put(4, "four")
	m.Clear()

	var got []string
	for {
		entry, ok := it.Next()
		if !ok {
			break
		}
		got = append(got, entry.Value)
	}

	slices.Sort(got)
	want := []string{"one", "three", "two"}
	if !slices.Equal(got, want) {
		t.Fatalf("iterator snapshot = %v, want %v", got, want)
	}
}

func TestMapConcurrentPutAndGet(t *testing.T) {
	const workers = 8
	const perWorker = 250

	m := New[int, int](32, IntHasher)

	var writers sync.WaitGroup
	writers.Add(workers)
	for worker := 0; worker < workers; worker++ {
		worker := worker
		go func() {
			defer writers.Done()
			base := worker * perWorker
			for i := 0; i < perWorker; i++ {
				m.Put(base+i, base+i)
			}
		}()
	}
	writers.Wait()

	if got := m.Size(); got != workers*perWorker {
		t.Fatalf("Size() after concurrent Put = %d, want %d", got, workers*perWorker)
	}

	var readers sync.WaitGroup
	readers.Add(workers)
	for worker := 0; worker < workers; worker++ {
		worker := worker
		go func() {
			defer readers.Done()
			base := worker * perWorker
			for i := 0; i < perWorker; i++ {
				key := base + i
				value, ok := m.Get(key)
				if !ok || value != key {
					t.Errorf("Get(%d) = (%d, %v), want (%d, true)", key, value, ok, key)
					return
				}
			}
		}()
	}
	readers.Wait()
}

func TestMapConcurrentMergeSameKey(t *testing.T) {
	const workers = 32

	m := New[int, int](4, IntHasher)

	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			m.Merge(1, 1, func(current, incoming int) int {
				return current + incoming
			})
		}()
	}
	wg.Wait()

	value, ok := m.Get(1)
	if !ok || value != workers {
		t.Fatalf("Get(1) after concurrent Merge = (%d, %v), want (%d, true)", value, ok, workers)
	}
}

func TestMapResizePreservesEntries(t *testing.T) {
	m := New[int, int](2, IntHasher)

	for i := 0; i < 256; i++ {
		m.Put(i, i*i)
	}

	if got := m.Size(); got != 256 {
		t.Fatalf("Size() after resize = %d, want 256", got)
	}

	if buckets := len(m.table.Load().buckets); buckets <= 2 {
		t.Fatalf("bucket count after resize = %d, want > 2", buckets)
	}

	for i := 0; i < 256; i++ {
		value, ok := m.Get(i)
		if !ok || value != i*i {
			t.Fatalf("Get(%d) after resize = (%d, %v), want (%d, true)", i, value, ok, i*i)
		}
	}
}

func TestMapConcurrentResizeAndReads(t *testing.T) {
	const workers = 6
	const perWorker = 200

	m := New[int, int](2, IntHasher)

	var writers sync.WaitGroup
	writers.Add(workers)
	for worker := 0; worker < workers; worker++ {
		worker := worker
		go func() {
			defer writers.Done()
			base := worker * perWorker
			for i := 0; i < perWorker; i++ {
				key := base + i
				m.Put(key, key+1)
				if value, ok := m.Get(key); !ok || value != key+1 {
					t.Errorf("Get(%d) during resize = (%d, %v), want (%d, true)", key, value, ok, key+1)
					return
				}
			}
		}()
	}
	writers.Wait()

	if got := m.Size(); got != workers*perWorker {
		t.Fatalf("Size() after concurrent resize workload = %d, want %d", got, workers*perWorker)
	}
}

func TestMapSynctestMergeAndClear(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m := New[int, int](8, IntHasher)
		start := make(chan struct{})

		var wg sync.WaitGroup
		wg.Add(3)

		go func() {
			defer wg.Done()
			<-start
			m.Merge(7, 2, func(current, incoming int) int { return current + incoming })
		}()

		go func() {
			defer wg.Done()
			<-start
			m.Merge(7, 3, func(current, incoming int) int { return current + incoming })
		}()

		go func() {
			defer wg.Done()
			<-start
			m.Clear()
		}()

		close(start)
		wg.Wait()
		synctest.Wait()

		value, ok := m.Get(7)
		if ok && value != 2 && value != 3 && value != 5 {
			t.Fatalf("unexpected final state after Merge/Clear race: (%d, %v)", value, ok)
		}

		if size := m.Size(); size < 0 || size > 1 {
			t.Fatalf("Size() after Merge/Clear race = %d, want 0 or 1", size)
		}
	})
}

func TestMapSynctestResizeAndClear(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m := New[int, int](2, IntHasher)
		start := make(chan struct{})

		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < 128; i++ {
				m.Put(i, i)
			}
		}()

		go func() {
			defer wg.Done()
			<-start
			m.Clear()
		}()

		close(start)
		wg.Wait()
		synctest.Wait()

		size := m.Size()
		if size < 0 || size > 128 {
			t.Fatalf("Size() after resize/clear race = %d, want in [0, 128]", size)
		}

		if size == 0 {
			return
		}

		for i := 0; i < 128; i++ {
			if value, ok := m.Get(i); ok && value != i {
				t.Fatalf("Get(%d) after resize/clear race = (%d, %v), want (%d, true) or missing", i, value, ok, i)
			}
		}
	})
}

func TestPlainMapBasicOperations(t *testing.T) {
	m := NewPlain[int, int](8, IntHasher)
	m.Put(1, 10)
	m.Put(1, 11)
	m.Put(2, 20)

	if got := m.Size(); got != 2 {
		t.Fatalf("Size() = %d, want 2", got)
	}

	got, ok := m.Get(1)
	if !ok || got != 11 {
		t.Fatalf("Get(1) = (%d, %v), want (11, true)", got, ok)
	}

	merged := m.Merge(2, 5, func(current, incoming int) int { return current + incoming })
	if merged != 25 {
		t.Fatalf("Merge(2) = %d, want 25", merged)
	}

	m.Clear()
	if got := m.Size(); got != 0 {
		t.Fatalf("Size() after Clear = %d, want 0", got)
	}
}

func TestPlainMapResizePreservesEntries(t *testing.T) {
	m := NewPlain[int, int](2, IntHasher)

	for i := 0; i < 256; i++ {
		m.Put(i, i*3)
	}

	if got := m.Size(); got != 256 {
		t.Fatalf("Size() after resize = %d, want 256", got)
	}

	if buckets := len(m.buckets); buckets <= 2 {
		t.Fatalf("bucket count after resize = %d, want > 2", buckets)
	}

	for i := 0; i < 256; i++ {
		value, ok := m.Get(i)
		if !ok || value != i*3 {
			t.Fatalf("Get(%d) after resize = (%d, %v), want (%d, true)", i, value, ok, i*3)
		}
	}
}
