package hw4

import (
	"strconv"
	"testing"
)

var benchmarkSizes = []int{1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000}

func BenchmarkMapPut(b *testing.B) {
	for _, size := range benchmarkSizes {
		b.Run("concurrent/size="+strconv.Itoa(size), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				m := New[int, int](size/4+1, IntHasher)
				for key := 0; key < size; key++ {
					m.Put(key, key)
				}
			}
		})

		b.Run("plain/size="+strconv.Itoa(size), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				m := NewPlain[int, int](size/4+1, IntHasher)
				for key := 0; key < size; key++ {
					m.Put(key, key)
				}
			}
		})
	}
}

func BenchmarkMapGetHit(b *testing.B) {
	for _, size := range benchmarkSizes {
		b.Run("concurrent/size="+strconv.Itoa(size), func(b *testing.B) {
			m := New[int, int](size/4+1, IntHasher)
			for key := 0; key < size; key++ {
				m.Put(key, key)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := i % size
				if _, ok := m.Get(key); !ok {
					b.Fatalf("key %d missing", key)
				}
			}
		})

		b.Run("plain/size="+strconv.Itoa(size), func(b *testing.B) {
			m := NewPlain[int, int](size/4+1, IntHasher)
			for key := 0; key < size; key++ {
				m.Put(key, key)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := i % size
				if _, ok := m.Get(key); !ok {
					b.Fatalf("key %d missing", key)
				}
			}
		})
	}
}

func BenchmarkMapMergeHit(b *testing.B) {
	for _, size := range benchmarkSizes {
		b.Run("concurrent/size="+strconv.Itoa(size), func(b *testing.B) {
			m := New[int, int](size/4+1, IntHasher)
			for key := 0; key < size; key++ {
				m.Put(key, 1)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := i % size
				m.Merge(key, 1, func(current, incoming int) int { return current + incoming })
			}
		})

		b.Run("plain/size="+strconv.Itoa(size), func(b *testing.B) {
			m := NewPlain[int, int](size/4+1, IntHasher)
			for key := 0; key < size; key++ {
				m.Put(key, 1)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := i % size
				m.Merge(key, 1, func(current, incoming int) int { return current + incoming })
			}
		})
	}
}

func BenchmarkMapReadMostlyParallel(b *testing.B) {
	for _, size := range benchmarkSizes {
		b.Run("size="+strconv.Itoa(size), func(b *testing.B) {
			m := New[int, int](size/4+1, IntHasher)
			for key := 0; key < size; key++ {
				m.Put(key, key)
			}

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				local := 0
				for pb.Next() {
					key := local % size
					if local%16 == 0 {
						m.Put(key, local)
					} else {
						m.Get(key)
					}
					local++
				}
			})
		})
	}
}

func BenchmarkMapPutParallel(b *testing.B) {
	for _, size := range benchmarkSizes {
		b.Run("concurrent/size="+strconv.Itoa(size), func(b *testing.B) {
			m := New[int, int](size/8+1, IntHasher)
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				local := 0
				for pb.Next() {
					key := local % size
					m.Put(key, local)
					local++
				}
			})
		})
	}
}

func BenchmarkMapMergeParallelSameKey(b *testing.B) {
	for _, size := range benchmarkSizes {
		b.Run("size="+strconv.Itoa(size), func(b *testing.B) {
			m := New[int, int](size/8+1, IntHasher)
			for key := 0; key < size; key++ {
				m.Put(key, 0)
			}

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					m.Merge(1, 1, func(current, incoming int) int {
						return current + incoming
					})
				}
			})
		})
	}
}
