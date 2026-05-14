package lsh

import (
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"testing"
)

func TestBuildAndSearch(t *testing.T) {
	cfg := Config{Tables: 4, CellSize: 0.5, Radius: 0.2, Seed: 1}
	index, err := Build([]Point{
		{ID: "a", X: 0.00, Y: 0.00, Z: 0.00},
		{ID: "b", X: 0.08, Y: 0.05, Z: 0.04},
		{ID: "c", X: 2.00, Y: 2.00, Z: 2.00},
	}, cfg)
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	searchIDs := matchIDs(index.Search(Point{ID: "a", X: 0.00, Y: 0.00, Z: 0.00}))
	fullScanIDs := matchIDs(index.FullScan(Point{ID: "a", X: 0.00, Y: 0.00, Z: 0.00}))

	if !reflect.DeepEqual(searchIDs, []string{"b"}) {
		t.Fatalf("unexpected search result: %v", searchIDs)
	}
	if !reflect.DeepEqual(fullScanIDs, []string{"b"}) {
		t.Fatalf("unexpected full scan result: %v", fullScanIDs)
	}
}

func TestAddPoint(t *testing.T) {
	cfg := Config{Tables: 3, CellSize: 0.5, Radius: 0.2, Seed: 7}
	index, err := Build([]Point{
		{ID: "a", X: 1.00, Y: 1.00, Z: 1.00},
		{ID: "b", X: 3.00, Y: 3.00, Z: 3.00},
	}, cfg)
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	if err := index.Add(Point{ID: "c", X: 1.10, Y: 1.05, Z: 1.02}); err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	got := matchIDs(index.Search(Point{ID: "a", X: 1.00, Y: 1.00, Z: 1.00}))
	if !reflect.DeepEqual(got, []string{"c"}) {
		t.Fatalf("unexpected search result after add: %v", got)
	}

	stats := index.Stats()
	if stats.PointsCount != 3 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}

func TestBuildRejectsInvalidInput(t *testing.T) {
	_, err := Build(nil, Config{})
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("expected ErrInvalidConfig, got %v", err)
	}

	_, err = Build([]Point{{ID: "", X: 1, Y: 2, Z: 3}}, Config{Tables: 2, CellSize: 1, Radius: 0.2})
	if !errors.Is(err, ErrEmptyPointID) {
		t.Fatalf("expected ErrEmptyPointID, got %v", err)
	}

	_, err = Build([]Point{
		{ID: "dup", X: 0, Y: 0, Z: 0},
		{ID: "dup", X: 1, Y: 1, Z: 1},
	}, Config{Tables: 2, CellSize: 1, Radius: 0.2})
	if !errors.Is(err, ErrDuplicatePoint) {
		t.Fatalf("expected ErrDuplicatePoint, got %v", err)
	}
}

func TestSearchRandomAgainstFullScan(t *testing.T) {
	cfg := Config{Tables: 4, CellSize: 0.5, Radius: 0.2, Seed: 42}
	seeds := []int64{1, 42, 20260325}

	for _, seed := range seeds {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			points := randomPoints(seed, 300)
			index, err := Build(points, cfg)
			if err != nil {
				t.Fatalf("Build failed: %v", err)
			}

			rng := rand.New(rand.NewSource(seed + 99))
			for step := 0; step < 100; step++ {
				base := points[rng.Intn(len(points))]
				query := Point{
					ID: fmt.Sprintf("q-%d", step),
					X:  base.X + (rng.Float64()-0.5)*0.1,
					Y:  base.Y + (rng.Float64()-0.5)*0.1,
					Z:  base.Z + (rng.Float64()-0.5)*0.1,
				}

				searchIDs := matchIDs(index.Search(query))
				fullScanIDs := matchIDs(index.FullScan(query))
				referenceIDs := exactMatchIDs(query, points, cfg.Radius)

				if !reflect.DeepEqual(searchIDs, referenceIDs) {
					t.Fatalf("Search mismatch: got=%v want=%v", searchIDs, referenceIDs)
				}
				if !reflect.DeepEqual(fullScanIDs, referenceIDs) {
					t.Fatalf("FullScan mismatch: got=%v want=%v", fullScanIDs, referenceIDs)
				}
			}
		})
	}
}

func matchIDs(matches []Match) []string {
	ids := make([]string, len(matches))
	for i := range matches {
		ids[i] = matches[i].ID
	}
	sort.Strings(ids)
	return ids
}

func exactMatchIDs(query Point, points []Point, radius float64) []string {
	ids := make([]string, 0)
	for _, point := range points {
		if query.ID != "" && point.ID == query.ID {
			continue
		}
		if pointDistance(query, point) <= radius {
			ids = append(ids, point.ID)
		}
	}
	sort.Strings(ids)
	return ids
}

func randomPoints(seed int64, size int) []Point {
	rng := rand.New(rand.NewSource(seed))
	centers := make([][3]float64, 12)
	for i := range centers {
		centers[i] = [3]float64{
			rng.Float64() * 20,
			rng.Float64() * 20,
			rng.Float64() * 20,
		}
	}

	points := make([]Point, size)
	for i := 0; i < size; i++ {
		center := centers[rng.Intn(len(centers))]
		points[i] = Point{
			ID: fmt.Sprintf("p-%05d", i),
			X:  center[0] + (rng.Float64()-0.5)*0.2,
			Y:  center[1] + (rng.Float64()-0.5)*0.2,
			Z:  center[2] + (rng.Float64()-0.5)*0.2,
		}
	}

	return points
}
