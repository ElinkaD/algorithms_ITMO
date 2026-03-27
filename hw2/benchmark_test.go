package geosearch

import (
	"fmt"
	"math/rand"
	"testing"
)

func BenchmarkInsert(b *testing.B) { //// смотрим, как меняется стоимость построения индекса при росте числа объектов
	sizes := []int{1000, 10000, 50000, 100000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			rng := rand.New(rand.NewSource(42))
			points := GenerateRandomPoints(rng, size)

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				index, err := NewIndex(5)
				if err != nil {
					b.Fatalf("NewIndex() error = %v", err)
				}

				// тут каждый прогон строит индекс заново,
				// чтобы замерять именно стоимость вставок, а не повторный поиск.
				for _, point := range points {
					if err := index.Insert(point); err != nil {
						b.Fatalf("Insert() error = %v", err)
					}
				}
			}
		})
	}
}

// проверяем, как быстро находится точка по exact match
func BenchmarkSearchExact(b *testing.B) {
	sizes := []int{1000, 10000, 50000, 100000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			index, points := buildBenchmarkIndex(b, size, 5)
			// берем одну из уже вставленных точек как запрос,
			// чтобы точно искать существующий объект
			query := points[len(points)/2]

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if _, err := index.SearchExact(query.Lat, query.Lng); err != nil {
					b.Fatalf("SearchExact() error = %v", err)
				}
			}
		})
	}
}

// влияние сразу трех факторов: размера данных, точности geohash и радиуса поиска
func BenchmarkSearchNearby(b *testing.B) {
	sizes := []int{1000, 10000, 50000, 100000}
	precisions := []int{4, 5}
	radii := []float64{1000, 50000}

	for _, size := range sizes {
		for _, precision := range precisions {
			for _, radius := range radii {
				name := fmt.Sprintf("size=%d/precision=%d/radius=%.0f", size, precision, radius)
				b.Run(name, func(b *testing.B) {
					index, points := buildBenchmarkIndex(b, size, precision)
					// запрос берем из уже существующих точек
					query := points[len(points)/3]

					b.ReportAllocs()
					b.ResetTimer()

					for i := 0; i < b.N; i++ {
						if _, err := index.SearchNearby(query.Lat, query.Lng, radius); err != nil {
							b.Fatalf("SearchNearby() error = %v", err)
						}
					}
				})
			}
		}
	}
}

func BenchmarkFullScan(b *testing.B) {
	sizes := []int{1000, 10000, 50000, 100000}
	radii := []float64{1000, 50000}

	for _, size := range sizes {
		for _, radius := range radii {
			name := fmt.Sprintf("size=%d/radius=%.0f", size, radius)
			b.Run(name, func(b *testing.B) {
				index, points := buildBenchmarkIndex(b, size, 5)
				query := points[len(points)/3]

				b.ReportAllocs()
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					if _, err := index.FullScan(query.Lat, query.Lng, radius); err != nil {
						b.Fatalf("FullScan() error = %v", err)
					}
				}
			})
		}
	}
}

// заранее строит индекс, чтобы потом замерять только сам поиск
func buildBenchmarkIndex(b *testing.B, size, precision int) (*Index, []GeoObject) {
	b.Helper()

	rng := rand.New(rand.NewSource(int64(size*100 + precision)))
	points := GenerateRandomPoints(rng, size)

	index, err := NewIndex(precision)
	if err != nil {
		b.Fatalf("NewIndex() error = %v", err)
	}

	// индекс собираю один раз до запуска таймера, чтобы не смешивать стоимость построения и стоимость поиска
	for _, point := range points {
		if err := index.Insert(point); err != nil {
			b.Fatalf("Insert() error = %v", err)
		}
	}

	return index, points
}
