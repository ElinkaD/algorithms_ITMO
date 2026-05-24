package hw4

// PlainMap - baseline без синхронизации для сравнения.
type PlainMap[K comparable, V any] struct {
	buckets [][]Entry[K, V]
	hasher  Hasher[K]
	size    int
}

// NewPlain создает обычную hash map без thread-safety.
func NewPlain[K comparable, V any](bucketCount int, hasher Hasher[K]) *PlainMap[K, V] {
	if bucketCount <= 0 {
		bucketCount = defaultBucketCount
	}
	if hasher == nil {
		panic("hw4: hasher must not be nil")
	}

	return &PlainMap[K, V]{
		buckets: make([][]Entry[K, V], bucketCount),
		hasher:  hasher,
	}
}

func (m *PlainMap[K, V]) Put(key K, value V) {
	index := m.bucketIndex(key)
	for i := range m.buckets[index] {
		if m.buckets[index][i].Key == key {
			m.buckets[index][i].Value = value
			return
		}
	}

	m.buckets[index] = append(m.buckets[index], Entry[K, V]{Key: key, Value: value})
	m.size++
	m.resizeIfNeeded()
}

func (m *PlainMap[K, V]) Get(key K) (V, bool) {
	index := m.bucketIndex(key)
	for _, entry := range m.buckets[index] {
		if entry.Key == key {
			return entry.Value, true
		}
	}

	var zero V
	return zero, false
}

func (m *PlainMap[K, V]) Size() int {
	return m.size
}

func (m *PlainMap[K, V]) Clear() {
	for i := range m.buckets {
		m.buckets[i] = nil
	}
	m.size = 0
}

func (m *PlainMap[K, V]) Merge(key K, value V, merger Merger[V]) V {
	if merger == nil {
		panic("hw4: merger must not be nil")
	}

	index := m.bucketIndex(key)
	for i := range m.buckets[index] {
		if m.buckets[index][i].Key == key {
			merged := merger(m.buckets[index][i].Value, value)
			m.buckets[index][i].Value = merged
			return merged
		}
	}

	m.buckets[index] = append(m.buckets[index], Entry[K, V]{Key: key, Value: value})
	m.size++
	m.resizeIfNeeded()
	return value
}

func (m *PlainMap[K, V]) Iterator() *Iterator[K, V] {
	snapshot := make([]Entry[K, V], 0, m.size)
	for _, entries := range m.buckets {
		snapshot = append(snapshot, entries...)
	}
	return &Iterator[K, V]{entries: snapshot}
}

func (m *PlainMap[K, V]) bucketIndex(key K) int {
	return int(m.hasher(key) % uint64(len(m.buckets)))
}

func (m *PlainMap[K, V]) resizeIfNeeded() {
	if m.size <= len(m.buckets)*maxLoadFactor {
		return
	}

	nextBuckets := make([][]Entry[K, V], len(m.buckets)*2)
	for _, entries := range m.buckets {
		for _, entry := range entries {
			index := int(m.hasher(entry.Key) % uint64(len(nextBuckets)))
			nextBuckets[index] = append(nextBuckets[index], entry)
		}
	}
	m.buckets = nextBuckets
}
