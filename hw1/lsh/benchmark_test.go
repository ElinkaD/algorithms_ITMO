package lsh

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
)

var defaultBenchmarkSizes = []int{1000, 10000, 50000, 100000}

func BenchmarkTableBuild(b *testing.B) {
	cfg := Config{Tables: 4, CellSize: 0.5, Radius: 0.2, Seed: 1}

	for _, size := range benchmarkSizes(b) {
		points := randomPoints(int64(size*97), size)
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StartTimer()
				index, err := Build(points, cfg)
				b.StopTimer()
				if err != nil {
					b.Fatalf("Build failed: %v", err)
				}
				if index.Stats().PointsCount != size {
					b.Fatalf("unexpected stats after build")
				}
			}

			reportItemMetrics(b, size)
		})
	}
}

func BenchmarkTableAdd(b *testing.B) {
	cfg := Config{Tables: 4, CellSize: 0.5, Radius: 0.2, Seed: 1}

	for _, size := range benchmarkSizes(b) {
		points := randomPoints(int64(size*97), size)
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				index, err := Build(nil, cfg)
				if err != nil {
					b.Fatalf("Build failed: %v", err)
				}

				b.StartTimer()
				for _, point := range points {
					if err := index.Add(point); err != nil {
						b.Fatalf("Add failed: %v", err)
					}
				}
				b.StopTimer()
			}

			reportItemMetrics(b, size)
		})
	}
}

func BenchmarkTableSearch(b *testing.B) {
	cfg := Config{Tables: 4, CellSize: 0.5, Radius: 0.2, Seed: 1}

	for _, size := range benchmarkSizes(b) {
		points := randomPoints(int64(size*97), size)
		index, err := Build(points, cfg)
		if err != nil {
			b.Fatalf("Build failed: %v", err)
		}
		queries := benchmarkQueries(points)

		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StartTimer()
				for _, query := range queries {
					_ = index.Search(query)
				}
				b.StopTimer()
			}

			reportItemMetrics(b, len(queries))
		})
	}
}

func BenchmarkTableFullScan(b *testing.B) {
	cfg := Config{Tables: 4, CellSize: 0.5, Radius: 0.2, Seed: 1}

	for _, size := range benchmarkSizes(b) {
		points := randomPoints(int64(size*97), size)
		index, err := Build(points, cfg)
		if err != nil {
			b.Fatalf("Build failed: %v", err)
		}
		queries := benchmarkQueries(points)

		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StartTimer()
				for _, query := range queries {
					_ = index.FullScan(query)
				}
				b.StopTimer()
			}

			reportItemMetrics(b, len(queries))
		})
	}
}

func benchmarkQueries(points []Point) []Point {
	count := len(points) / 1000
	if count < 50 {
		count = 50
	}
	if count > 200 {
		count = 200
	}

	queries := make([]Point, 0, count)
	step := len(points) / count
	if step == 0 {
		step = 1
	}

	for i := 0; i < len(points) && len(queries) < count; i += step {
		point := points[i]
		queries = append(queries, Point{
			ID: fmt.Sprintf("q-%d", len(queries)),
			X:  point.X + 0.03,
			Y:  point.Y - 0.02,
			Z:  point.Z + 0.01,
		})
	}

	return queries
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
