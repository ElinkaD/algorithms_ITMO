package perfect

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
)

var defaultBenchmarkSizes = []int{1000, 10000, 50000, 100000}

func BenchmarkTableBuild(b *testing.B) {
	for _, size := range benchmarkSizes(b) {
		entries, _ := randomEntries(int64(size*97), size)

		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StartTimer()
				table, err := Build(entries)
				b.StopTimer()
				if err != nil {
					b.Fatalf("Build failed: %v", err)
				}
				if table.Stats().KeysCount != size {
					b.Fatalf("unexpected stats after build")
				}
			}

			reportItemMetrics(b, size)
		})
	}
}

func BenchmarkTableGet(b *testing.B) {
	for _, size := range benchmarkSizes(b) {
		entries, _ := randomEntries(int64(size*97), size)
		table, err := Build(entries)
		if err != nil {
			b.Fatalf("Build failed: %v", err)
		}

		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StartTimer()
				for _, entry := range entries {
					got, found := table.Get(entry.Key)
					if !found || got != entry.Value {
						b.Fatalf("Get(%q) mismatch", entry.Key)
					}
				}
				b.StopTimer()
			}

			reportItemMetrics(b, size)
		})
	}
}

func BenchmarkTableGetMiss(b *testing.B) {
	for _, size := range benchmarkSizes(b) {
		entries, _ := randomEntries(int64(size*97), size)
		table, err := Build(entries)
		if err != nil {
			b.Fatalf("Build failed: %v", err)
		}

		misses := make([]string, size)
		for i := range misses {
			misses[i] = fmt.Sprintf("missing-%08d", i)
		}

		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StartTimer()
				for _, key := range misses {
					if _, found := table.Get(key); found {
						b.Fatalf("expected missing key %q to be absent", key)
					}
				}
				b.StopTimer()
			}

			reportItemMetrics(b, size)
		})
	}
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

func reportItemMetrics(b *testing.B, size int) {
	if b.N == 0 || size == 0 || b.Elapsed() <= 0 {
		return
	}

	totalItems := float64(b.N * size)
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/totalItems, "ns/item")
	b.ReportMetric(totalItems/b.Elapsed().Seconds(), "ops/s")
}
