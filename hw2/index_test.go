package geosearch

import (
	"math"
	"math/rand"
	"testing"
)

func TestSearchNearbyMatchesFullScanOnRandomData(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	points := GenerateRandomPoints(rng, 3000)

	index, err := NewIndex(4)
	if err != nil {
		t.Fatalf("NewIndex() error = %v", err)
	}

	for _, point := range points {
		if err := index.Insert(point); err != nil {
			t.Fatalf("Insert() error = %v", err)
		}
	}

	for attempt := 0; attempt < 100; attempt++ {
		query := GenerateRandomPoints(rng, 1)[0]
		radius := 5000 + rng.Float64()*500000

		got, err := index.SearchNearby(query.Lat, query.Lng, radius)
		if err != nil {
			t.Fatalf("SearchNearby() error = %v", err)
		}

		want, err := index.FullScan(query.Lat, query.Lng, radius)
		if err != nil {
			t.Fatalf("FullScan() error = %v", err)
		}

		assertResultsEqual(t, want, got)
	}
}

func TestSearchNearbyCornerCases(t *testing.T) {
	t.Run("empty index", func(t *testing.T) {
		index, err := NewIndex(5)
		if err != nil {
			t.Fatalf("NewIndex() error = %v", err)
		}

		got, err := index.SearchNearby(59.93, 30.31, 1000)
		if err != nil {
			t.Fatalf("SearchNearby() error = %v", err)
		}
		if len(got) != 0 {
			t.Fatalf("expected empty result, got %d items", len(got))
		}
	})

	t.Run("single point", func(t *testing.T) {
		index := mustIndex(t, 6)
		point := GeoObject{ID: "p1", Lat: 59.93, Lng: 30.31}
		if err := index.Insert(point); err != nil {
			t.Fatalf("Insert() error = %v", err)
		}

		got, err := index.SearchNearby(59.93, 30.31, 10)
		if err != nil {
			t.Fatalf("SearchNearby() error = %v", err)
		}
		if len(got) != 1 || got[0].Object.ID != "p1" {
			t.Fatalf("unexpected result: %+v", got)
		}
	})

	t.Run("same coordinates", func(t *testing.T) {
		index := mustIndex(t, 6)
		for i := 0; i < 3; i++ {
			if err := index.Insert(GeoObject{ID: string(rune('a' + i)), Lat: 10, Lng: 20}); err != nil {
				t.Fatalf("Insert() error = %v", err)
			}
		}

		got, err := index.SearchNearby(10, 20, 1)
		if err != nil {
			t.Fatalf("SearchNearby() error = %v", err)
		}
		if len(got) != 3 {
			t.Fatalf("expected 3 results, got %d", len(got))
		}
	})

	t.Run("boundary coordinates", func(t *testing.T) {
		index := mustIndex(t, 5)
		points := []GeoObject{
			{ID: "sw", Lat: -90, Lng: -180},
			{ID: "nw", Lat: 90, Lng: -180},
			{ID: "ne", Lat: 90, Lng: 180},
			{ID: "se", Lat: -90, Lng: 180},
		}

		for _, point := range points {
			if err := index.Insert(point); err != nil {
				t.Fatalf("Insert() error = %v", err)
			}
		}

		for _, point := range points {
			got, err := index.SearchExact(point.Lat, point.Lng)
			if err != nil {
				t.Fatalf("SearchExact() error = %v", err)
			}
			if len(got) == 0 {
				t.Fatalf("expected non-empty bucket for %+v", point)
			}
		}
	})
}

func TestSearchExactReturnsBucket(t *testing.T) {
	index := mustIndex(t, 4)
	first := GeoObject{ID: "a", Lat: 55.75, Lng: 37.62}
	second := GeoObject{ID: "b", Lat: 55.751, Lng: 37.621}

	if err := index.Insert(first); err != nil {
		t.Fatalf("Insert() error = %v", err)
	}
	if err := index.Insert(second); err != nil {
		t.Fatalf("Insert() error = %v", err)
	}

	got, err := index.SearchExact(first.Lat, first.Lng)
	if err != nil {
		t.Fatalf("SearchExact() error = %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 objects in bucket, got %d", len(got))
	}
}

func TestEncodeGeohashRejectsInvalidValues(t *testing.T) {
	if _, err := EncodeGeohash(91, 0, 5); err == nil {
		t.Fatal("expected latitude validation error")
	}
	if _, err := EncodeGeohash(0, 181, 5); err == nil {
		t.Fatal("expected longitude validation error")
	}
	if _, err := EncodeGeohash(0, 0, 0); err == nil {
		t.Fatal("expected precision validation error")
	}
}

func mustIndex(t *testing.T, precision int) *Index {
	t.Helper()
	index, err := NewIndex(precision)
	if err != nil {
		t.Fatalf("NewIndex() error = %v", err)
	}
	return index
}

func assertResultsEqual(t *testing.T, want, got []SearchResult) {
	t.Helper()

	if len(want) != len(got) {
		t.Fatalf("different lengths: want=%d got=%d", len(want), len(got))
	}

	for i := range want {
		if want[i].Object.ID != got[i].Object.ID {
			t.Fatalf("different object at %d: want=%s got=%s", i, want[i].Object.ID, got[i].Object.ID)
		}
		if math.Abs(want[i].Distance-got[i].Distance) > 1e-6 {
			t.Fatalf("different distance at %d: want=%f got=%f", i, want[i].Distance, got[i].Distance)
		}
	}
}
