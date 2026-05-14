package perfect

import (
	"errors"
	"fmt"
	"hash/fnv"
	"math/rand"
)

const prime uint64 = 2305843009213693951

var (
	ErrEmptyKey     = errors.New("key must not be empty")
	ErrDuplicateKey = errors.New("duplicate key")
	ErrBuildFailed  = errors.New("failed to build perfect hash")
)

type Entry struct {
	Key   string
	Value string
}

type Stats struct {
	KeysCount          int
	BucketsCount       int
	TotalSlots         int
	MaxBucketSize      int
	MaxSecondaryTrials int
	SpaceAmplification float64
}

type slot struct {
	Used  bool
	Key   string
	Value string
}

type bucket struct {
	A     uint64
	B     uint64
	Size  int
	Slots []slot
}

type Table struct {
	size    int
	buckets []bucket
	stats   Stats
}

func Build(entries []Entry) (*Table, error) {
	seen := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		if entry.Key == "" {
			return nil, ErrEmptyKey
		}
		if _, exists := seen[entry.Key]; exists {
			return nil, fmt.Errorf("%w: %s", ErrDuplicateKey, entry.Key)
		}
		seen[entry.Key] = struct{}{}
	}

	if len(entries) == 0 {
		return &Table{
			buckets: make([]bucket, 0),
			stats: Stats{
				KeysCount:          0,
				BucketsCount:       0,
				TotalSlots:         0,
				MaxBucketSize:      0,
				MaxSecondaryTrials: 0,
				SpaceAmplification: 0,
			},
		}, nil
	}

	primary := make([][]Entry, len(entries))
	for _, entry := range entries {
		index := int(hashKey(entry.Key) % uint64(len(entries)))
		primary[index] = append(primary[index], entry)
	}

	table := &Table{
		size:    len(entries),
		buckets: make([]bucket, len(entries)),
		stats: Stats{
			KeysCount:    len(entries),
			BucketsCount: len(entries),
		},
	}

	for bucketIndex, entriesInBucket := range primary {
		if len(entriesInBucket) == 0 {
			continue
		}
		if len(entriesInBucket) > table.stats.MaxBucketSize {
			table.stats.MaxBucketSize = len(entriesInBucket)
		}

		if len(entriesInBucket) == 1 {
			table.buckets[bucketIndex] = bucket{
				A:     1,
				B:     0,
				Size:  1,
				Slots: []slot{{Used: true, Key: entriesInBucket[0].Key, Value: entriesInBucket[0].Value}},
			}
			table.stats.TotalSlots++
			continue
		}

		size := len(entriesInBucket) * len(entriesInBucket)
		secondary, tries, err := buildSecondary(bucketIndex, entriesInBucket, size)
		if err != nil {
			return nil, err
		}

		table.buckets[bucketIndex] = secondary
		table.stats.TotalSlots += len(secondary.Slots)
		if tries > table.stats.MaxSecondaryTrials {
			table.stats.MaxSecondaryTrials = tries
		}
	}

	if table.stats.KeysCount > 0 {
		table.stats.SpaceAmplification = float64(table.stats.TotalSlots) / float64(table.stats.KeysCount)
	}

	return table, nil
}

func buildSecondary(bucketIndex int, entries []Entry, size int) (bucket, int, error) {
	rng := rand.New(rand.NewSource(int64(bucketIndex+1) * 9973))

	for tries := 1; tries <= 20000; tries++ {
		a := uint64(rng.Int63n(int64(prime-1))) + 1
		b := uint64(rng.Int63n(int64(prime)))
		slots := make([]slot, size)
		ok := true

		for _, entry := range entries {
			index := secondaryIndex(hashKey(entry.Key), a, b, size)
			if slots[index].Used {
				ok = false
				break
			}
			slots[index] = slot{Used: true, Key: entry.Key, Value: entry.Value}
		}

		if ok {
			return bucket{A: a, B: b, Size: size, Slots: slots}, tries, nil
		}
	}

	return bucket{}, 0, ErrBuildFailed
}

func (t *Table) Get(key string) (string, bool) {
	if key == "" || t.size == 0 {
		return "", false
	}

	bucketIndex := int(hashKey(key) % uint64(t.size))
	b := t.buckets[bucketIndex]
	if len(b.Slots) == 0 {
		return "", false
	}

	index := secondaryIndex(hashKey(key), b.A, b.B, b.Size)
	if index < 0 || index >= len(b.Slots) {
		return "", false
	}

	item := b.Slots[index]
	if !item.Used || item.Key != key {
		return "", false
	}

	return item.Value, true
}

func (t *Table) Stats() Stats {
	return t.stats
}

func hashKey(key string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	return h.Sum64()
}

func secondaryIndex(hash uint64, a uint64, b uint64, size int) int {
	if size == 0 {
		return 0
	}
	return int((((a * (hash % prime)) + b) % prime) % uint64(size))
}
