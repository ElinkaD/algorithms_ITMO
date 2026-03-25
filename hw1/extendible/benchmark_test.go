package extendible

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

var defaultBenchmarkSizes = []int{1000, 10000, 50000, 100000}

func BenchmarkTableInsert(b *testing.B) {
	for _, size := range benchmarkSizes(b) {
		pairs := benchmarkPairs(size)
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			runBatchBenchmark(b, size, func(b *testing.B, tablePath string) {
				table := openBenchmarkTable(b, tablePath)
				defer table.Close()

				b.StartTimer()
				for _, pair := range pairs {
					if err := table.Insert(pair.key, pair.value); err != nil {
						b.Fatalf("Insert(%q) failed: %v", pair.key, err)
					}
				}
				b.StopTimer()
			})
		})
	}
}

func BenchmarkTableUpdate(b *testing.B) {
	for _, size := range benchmarkSizes(b) {
		pairs := benchmarkPairs(size)
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			runBatchBenchmark(b, size, func(b *testing.B, tablePath string) {
				table := openBenchmarkTable(b, tablePath)
				defer table.Close()

				for _, pair := range pairs {
					if err := table.Insert(pair.key, "initial"); err != nil {
						b.Fatalf("prepare insert failed: %v", err)
					}
				}

				b.StartTimer()
				for _, pair := range pairs {
					if err := table.Update(pair.key, pair.value); err != nil {
						b.Fatalf("Update(%q) failed: %v", pair.key, err)
					}
				}
				b.StopTimer()
			})
		})
	}
}

func BenchmarkTableGet(b *testing.B) {
	for _, size := range benchmarkSizes(b) {
		pairs := benchmarkPairs(size)
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			runBatchBenchmark(b, size, func(b *testing.B, tablePath string) {
				table := openBenchmarkTable(b, tablePath)
				defer table.Close()

				for _, pair := range pairs {
					if err := table.Insert(pair.key, pair.value); err != nil {
						b.Fatalf("prepare insert failed: %v", err)
					}
				}

				b.StartTimer()
				for _, pair := range pairs {
					got, found, err := table.Get(pair.key)
					if err != nil {
						b.Fatalf("Get(%q) failed: %v", pair.key, err)
					}
					if !found || got != pair.value {
						b.Fatalf("Get(%q) mismatch", pair.key)
					}
				}
				b.StopTimer()
			})
		})
	}
}

func BenchmarkTableDelete(b *testing.B) {
	for _, size := range benchmarkSizes(b) {
		pairs := benchmarkPairs(size)
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			runBatchBenchmark(b, size, func(b *testing.B, tablePath string) {
				table := openBenchmarkTable(b, tablePath)
				defer table.Close()

				for _, pair := range pairs {
					if err := table.Insert(pair.key, pair.value); err != nil {
						b.Fatalf("prepare insert failed: %v", err)
					}
				}

				b.StartTimer()
				for _, pair := range pairs {
					if err := table.Delete(pair.key); err != nil {
						b.Fatalf("Delete(%q) failed: %v", pair.key, err)
					}
				}
				b.StopTimer()
			})
		})
	}
}

type benchmarkPair struct {
	key   string
	value string
}

// Делаем один и тот же набор для одинакового size.
func benchmarkPairs(size int) []benchmarkPair {
	rng := rand.New(rand.NewSource(int64(size * 97)))
	pairs := make([]benchmarkPair, size)
	for i := 0; i < size; i++ {
		pairs[i] = benchmarkPair{
			key:   fmt.Sprintf("key-%08d-%016x", i, rng.Uint64()),
			value: fmt.Sprintf("value-%08d-%016x", i, rng.Uint64()),
		}
	}
	return pairs
}

func benchmarkSizes(b testing.TB) []int {
	b.Helper()

	raw := strings.TrimSpace(os.Getenv("SIZES"))
	if raw == "" {
		return defaultBenchmarkSizes
	}

	parts := strings.Split(raw, ",")
	sizes := make([]int, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		value, err := strconv.Atoi(part)
		if err != nil {
			b.Fatalf("invalid SIZES value %q: %v", part, err)
		}
		if value <= 0 {
			b.Fatalf("invalid SIZES value %q", part)
		}
		sizes = append(sizes, value)
	}

	if len(sizes) == 0 {
		b.Fatalf("SIZES=%q did not contain valid values", raw)
	}
	return sizes
}

func openBenchmarkTable(b testing.TB, tablePath string) *Table {
	b.Helper()

	table, err := Open(tablePath, 16)
	if err != nil {
		b.Fatalf("Open benchmark table failed: %v", err)
	}
	return table
}

func runBatchBenchmark(b *testing.B, batchSize int, fn func(b *testing.B, tablePath string)) {
	b.Helper()
	b.ReportAllocs()

	baseDir := b.TempDir()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		tablePath := filepath.Join(baseDir, fmt.Sprintf("run-%d", i))

		b.StopTimer()
		if err := os.RemoveAll(tablePath); err != nil {
			b.Fatalf("RemoveAll failed: %v", err)
		}
		fn(b, tablePath)
		b.StopTimer()
	}

	if b.N == 0 || batchSize == 0 || b.Elapsed() <= 0 {
		return
	}

	totalItems := float64(b.N * batchSize)
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/totalItems, "ns/item")
	b.ReportMetric(totalItems/b.Elapsed().Seconds(), "ops/s")
}
