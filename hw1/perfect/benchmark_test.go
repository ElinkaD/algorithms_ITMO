package perfect

import (
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

var defaultBenchmarkSizes = []int{1000, 10000, 50000, 100000}

const (
	defaultGetBenchIters       = 100
	defaultGetCountPerIter     = 100000
	defaultGeneratedDatasetFmt = "generated:size=%d:seed=%d"
)

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

func TestPerfectGetReport(t *testing.T) {
	if strings.TrimSpace(os.Getenv("RUN_GET_REPORT")) != "1" {
		t.Skip("set RUN_GET_REPORT=1 to generate get report")
	}

	sizes := benchmarkSizes(t)
	iters := readPositiveIntEnv(t, "BENCH_ITERS", defaultGetBenchIters)
	lookupsPerIter := readPositiveIntEnv(t, "GET_COUNT_PER_ITER", defaultGetCountPerIter)
	writer := io.Writer(os.Stdout)

	if outPath := strings.TrimSpace(os.Getenv("GET_REPORT_OUT")); outPath != "" {
		file, err := os.Create(outPath)
		if err != nil {
			t.Fatalf("create get report: %v", err)
		}
		defer file.Close()
		writer = file
	}

	fmt.Fprintln(writer, "file,rows,iterations,get_sec,get_avg_sec,get_ci95_avg_sec,get_ops_sec")
	for _, size := range sizes {
		seed := int64(size * 97)
		entries, _ := randomEntries(seed, size)
		table, err := Build(entries)
		if err != nil {
			t.Fatalf("Build failed: %v", err)
		}

		samples := make([]float64, iters)
		var totalSec float64
		var totalOpsSec float64

		for iter := 0; iter < iters; iter++ {
			start := time.Now()
			for i := 0; i < lookupsPerIter; i++ {
				entry := entries[i%len(entries)]
				got, found := table.Get(entry.Key)
				if !found || got != entry.Value {
					t.Fatalf("Get(%q) mismatch", entry.Key)
				}
			}
			elapsedSec := time.Since(start).Seconds()
			avgSec := elapsedSec / float64(lookupsPerIter)
			samples[iter] = avgSec
			totalSec += elapsedSec
			if elapsedSec > 0 {
				totalOpsSec += float64(lookupsPerIter) / elapsedSec
			}
		}

		meanSec := totalSec / float64(iters)
		meanAvgSec := meanSec / float64(lookupsPerIter)
		ci95AvgSec := ci95Seconds(samples)
		meanOpsSec := totalOpsSec / float64(iters)

		fmt.Fprintf(
			writer,
			defaultGeneratedDatasetFmt+",%d,%d,%.9f,%.12f,%.12f,%.2f\n",
			size, seed,
			size,
			iters,
			meanSec,
			meanAvgSec,
			ci95AvgSec,
			meanOpsSec,
		)
	}
}

func reportItemMetrics(b *testing.B, size int) {
	if b.N == 0 || size == 0 || b.Elapsed() <= 0 {
		return
	}

	totalItems := float64(b.N * size)
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/totalItems, "ns/item")
	b.ReportMetric(totalItems/b.Elapsed().Seconds(), "ops/s")
}

func benchmarkSizes(tb testing.TB) []int {
	tb.Helper()
	return parsePositiveIntListEnv(tb, "SIZES", defaultBenchmarkSizes)
}

func readPositiveIntEnv(tb testing.TB, name string, defaultValue int) int {
	tb.Helper()

	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return defaultValue
	}

	value, err := strconv.Atoi(raw)
	if err != nil {
		tb.Fatalf("invalid %s value %q: %v", name, raw, err)
	}
	if value <= 0 {
		tb.Fatalf("invalid %s value %q", name, raw)
	}

	return value
}

func parsePositiveIntListEnv(tb testing.TB, name string, defaultValues []int) []int {
	tb.Helper()

	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return defaultValues
	}

	parts := strings.Split(raw, ",")
	values := make([]int, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		value, err := strconv.Atoi(part)
		if err != nil {
			tb.Fatalf("invalid %s value %q: %v", name, part, err)
		}
		if value <= 0 {
			tb.Fatalf("invalid %s value %q", name, part)
		}
		values = append(values, value)
	}

	if len(values) == 0 {
		tb.Fatalf("%s=%q did not contain valid values", name, raw)
	}
	return values
}

func ci95Seconds(samples []float64) float64 {
	if len(samples) <= 1 {
		return 0
	}

	var mean float64
	for _, sample := range samples {
		mean += sample
	}
	mean /= float64(len(samples))

	var variance float64
	for _, sample := range samples {
		diff := sample - mean
		variance += diff * diff
	}
	variance /= float64(len(samples) - 1)

	stderr := math.Sqrt(variance / float64(len(samples)))
	return 1.96 * stderr
}
