package hw4

import (
	"sync"
	"sync/atomic"
)

const (
	defaultBucketCount = 64
	maxLoadFactor      = 4
)

type Hasher[K comparable] func(K) uint64
type Merger[V any] func(current V, incoming V) V

type Entry[K comparable, V any] struct {
	Key   K
	Value V
}

type Iterator[K comparable, V any] struct {
	entries []Entry[K, V]
	index   int
}

func (it *Iterator[K, V]) Next() (Entry[K, V], bool) {
	if it.index >= len(it.entries) {
		var zero Entry[K, V]
		return zero, false
	}

	entry := it.entries[it.index]
	it.index++
	return entry, true
}

type node[K comparable, V any] struct {
	entry Entry[K, V]
	next  *node[K, V]
}

type bucket[K comparable, V any] struct {
	mu   sync.Mutex
	head atomic.Pointer[node[K, V]]
}

type table[K comparable, V any] struct {
	buckets []bucket[K, V]
}

func newTable[K comparable, V any](bucketCount int) *table[K, V] {
	return &table[K, V]{
		buckets: make([]bucket[K, V], bucketCount),
	}
}

func (t *table[K, V]) bucketIndex(hash uint64) int {
	return int(hash % uint64(len(t.buckets)))
}

type Map[K comparable, V any] struct {
	hasher   Hasher[K]
	size     atomic.Int64
	table    atomic.Pointer[table[K, V]]
	resizeMu sync.Mutex
}

// создает таблицу
func New[K comparable, V any](bucketCount int, hasher Hasher[K]) *Map[K, V] {
	if bucketCount <= 0 {
		bucketCount = defaultBucketCount
	}
	if hasher == nil {
		panic("hw4: hasher must not be nil")
	}

	m := &Map[K, V]{hasher: hasher}
	m.table.Store(newTable[K, V](bucketCount))
	return m
}

func (m *Map[K, V]) Put(key K, value V) {
	inserted := false

	for {
		currentTable := m.table.Load()
		hash := m.hasher(key)
		bucket := &currentTable.buckets[currentTable.bucketIndex(hash)]

		bucket.mu.Lock()
		if m.table.Load() != currentTable {
			bucket.mu.Unlock()
			continue
		}

		oldHead := bucket.head.Load()
		newHead, wasPresent := cloneChainWithPut(oldHead, key, value)
		// публикуем уже полностью собранную цепочку одним CAS
		if !bucket.head.CompareAndSwap(oldHead, newHead) {
			bucket.mu.Unlock()
			continue
		}
		bucket.mu.Unlock()

		if !wasPresent {
			m.size.Add(1)
			inserted = true
		}
		break
	}

	if inserted {
		m.resizeIfNeeded()
	}
}

// возвращает значение и признак того, что ключ есть
func (m *Map[K, V]) Get(key K) (V, bool) {
	currentTable := m.table.Load()
	hash := m.hasher(key)
	head := currentTable.buckets[currentTable.bucketIndex(hash)].head.Load()

	for current := head; current != nil; current = current.next {
		if current.entry.Key == key {
			return current.entry.Value, true
		}
	}

	var zero V
	return zero, false
}

// текущее число элементов
func (m *Map[K, V]) Size() int {
	return int(m.size.Load())
}


func (m *Map[K, V]) Clear() {
	m.resizeMu.Lock()
	defer m.resizeMu.Unlock()

	currentTable := m.table.Load()
	lockBuckets(currentTable)
	defer unlockBuckets(currentTable)

	if m.table.Load() != currentTable {
		return
	}

	nextTable := newTable[K, V](len(currentTable.buckets))
	if m.table.CompareAndSwap(currentTable, nextTable) {
		m.size.Store(0)
	}
}

//  ибо вставляет значение, либо объединяет его с текущим
func (m *Map[K, V]) Merge(key K, value V, merger Merger[V]) V {
	if merger == nil {
		panic("hw4: merger must not be nil")
	}

	inserted := false
	var result V

	for {
		currentTable := m.table.Load()
		hash := m.hasher(key)
		bucket := &currentTable.buckets[currentTable.bucketIndex(hash)]

		bucket.mu.Lock()
		if m.table.Load() != currentTable {
			bucket.mu.Unlock()
			continue
		}

		oldHead := bucket.head.Load()
		newHead, mergedValue, wasPresent := cloneChainWithMerge(oldHead, key, value, merger)
		if !bucket.head.CompareAndSwap(oldHead, newHead) {
			bucket.mu.Unlock()
			continue
		}
		bucket.mu.Unlock()

		result = mergedValue
		if !wasPresent {
			m.size.Add(1)
			inserted = true
		}
		break
	}

	if inserted {
		m.resizeIfNeeded()
	}

	return result
}

func (m *Map[K, V]) Iterator() *Iterator[K, V] {
	currentTable := m.table.Load()
	snapshot := make([]Entry[K, V], 0, m.Size())
	heads := make([]*node[K, V], len(currentTable.buckets))

	for i := range currentTable.buckets {
		heads[i] = currentTable.buckets[i].head.Load()
	}

	// сначала читаем головы bucket-ов, потом уже спокойно собираем snapshot.
	for _, head := range heads {
		for current := head; current != nil; current = current.next {
			snapshot = append(snapshot, current.entry)
		}
	}

	return &Iterator[K, V]{entries: snapshot}
}

func (m *Map[K, V]) resizeIfNeeded() {
	currentTable := m.table.Load()
	if m.Size() <= len(currentTable.buckets)*maxLoadFactor {
		return
	}

	m.resizeMu.Lock()
	defer m.resizeMu.Unlock()

	currentTable = m.table.Load()
	if m.Size() <= len(currentTable.buckets)*maxLoadFactor {
		return
	}

	lockBuckets(currentTable)
	defer unlockBuckets(currentTable)

	if m.table.Load() != currentTable {
		return
	}

	// перекидываем записи в новую таблицу и в конце публикуем ее целиком
	nextTable := newTable[K, V](len(currentTable.buckets) * 2)
	for i := range currentTable.buckets {
		head := currentTable.buckets[i].head.Load()
		for current := head; current != nil; current = current.next {
			index := nextTable.bucketIndex(m.hasher(current.entry.Key))
			nextHead := nextTable.buckets[index].head.Load()
			nextTable.buckets[index].head.Store(&node[K, V]{
				entry: current.entry,
				next:  nextHead,
			})
		}
	}

	m.table.CompareAndSwap(currentTable, nextTable)
}

func lockBuckets[K comparable, V any](t *table[K, V]) {
	for i := range t.buckets {
		t.buckets[i].mu.Lock()
	}
}

func unlockBuckets[K comparable, V any](t *table[K, V]) {
	for i := len(t.buckets) - 1; i >= 0; i-- {
		t.buckets[i].mu.Unlock()
	}
}

func cloneChainWithPut[K comparable, V any](head *node[K, V], key K, value V) (*node[K, V], bool) {
	entries := make([]Entry[K, V], 0, 4)
	found := false

	for current := head; current != nil; current = current.next {
		entry := current.entry
		if entry.Key == key {
			entry.Value = value
			found = true
		}
		entries = append(entries, entry)
	}

	if !found {
		entries = append(entries, Entry[K, V]{Key: key, Value: value})
	}

	return buildChain(entries), found
}

func cloneChainWithMerge[K comparable, V any](
	head *node[K, V],
	key K,
	value V,
	merger Merger[V],
) (*node[K, V], V, bool) {
	entries := make([]Entry[K, V], 0, 4)
	found := false
	result := value

	for current := head; current != nil; current = current.next {
		entry := current.entry
		if entry.Key == key {
			entry.Value = merger(entry.Value, value)
			result = entry.Value
			found = true
		}
		entries = append(entries, entry)
	}

	if !found {
		entries = append(entries, Entry[K, V]{Key: key, Value: value})
	}

	return buildChain(entries), result, found
}

func buildChain[K comparable, V any](entries []Entry[K, V]) *node[K, V] {
	var head *node[K, V]
	for i := len(entries) - 1; i >= 0; i-- {
		head = &node[K, V]{
			entry: entries[i],
			next:  head,
		}
	}
	return head
}

// StringHasher - обычный FNV-1a для строк.
func StringHasher(key string) uint64 {
	var hash uint64 = 14695981039346656037
	const prime uint64 = 1099511628211
	for i := 0; i < len(key); i++ {
		hash ^= uint64(key[i])
		hash *= prime
	}
	return hash
}

// IntHasher размешивает int-ключи в 64-битное пространство.
func IntHasher(key int) uint64 {
	x := uint64(int64(key))
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	x ^= x >> 33
	x *= 0xc4ceb9fe1a85ec53
	x ^= x >> 33
	return x
}
