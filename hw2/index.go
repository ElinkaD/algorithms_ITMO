package geosearch

import "errors"

type Index struct {
	precision int        // (размер ячейки)
	tree      *btree     
	buckets   map[string][]GeoObject // geohash -> список объектов в этой ячейке

	// objects нужен только для baseline FullScan.
	objects []GeoObject
}

func NewIndex(precision int) (*Index, error) {
	if precision <= 0 {
		return nil, errors.New("precision must be positive")
	}

	return &Index{
		precision: precision,
		tree:      newBTree(8), 
		buckets:   make(map[string][]GeoObject),
	}, nil
}

func (i *Index) Insert(object GeoObject) error {
	hash, err := EncodeGeohash(object.Lat, object.Lng, i.precision)
	if err != nil {
		return err
	}

	// B-tree хранит только уникальные geohash, сами объекты складываем в bucket
	i.tree.Insert(hash)
	i.buckets[hash] = append(i.buckets[hash], object)

	// сохраняем в общий список для FullScan (baseline)
	i.objects = append(i.objects, object)

	return nil
}

// поиск точного совпадения координат
func (i *Index) SearchExact(lat, lng float64) ([]GeoObject, error) {
	hash, err := EncodeGeohash(lat, lng, i.precision)
	if err != nil {
		return nil, err
	}

	if !i.tree.Contains(hash) {
		return nil, nil
	}

	bucket := i.buckets[hash]

	result := make([]GeoObject, 0, len(bucket))

	for _, object := range bucket {
		// точное совпадение координат внутри ячейки
		if object.Lat == lat && object.Lng == lng {
			result = append(result, object)
		}
	}

	return result, nil
}

// поиск ближайших объектов в радиусе
func (i *Index) SearchNearby(lat, lng, radiusMeters float64) ([]SearchResult, error) {
	if radiusMeters < 0 {
		return nil, errors.New("radius must be non-negative")
	}

	// получаем geohash-ячейки, которые покрывают область поиска
	hashes, err := neighboringHashes(lat, lng, radiusMeters, i.precision)
	if err != nil {
		return nil, err
	}

	candidates := make([]GeoObject, 0)

	// собираем кандидатов только из нужных bucket
	for _, hash := range hashes {
		if bucket, ok := i.buckets[hash]; ok {
			candidates = append(candidates, bucket...)
		}
	}
	return filterByDistance(candidates, lat, lng, radiusMeters), nil
}

func (i *Index) FullScan(lat, lng, radiusMeters float64) ([]SearchResult, error) {
	if radiusMeters < 0 {
		return nil, errors.New("radius must be non-negative")
	}

	return filterByDistance(i.objects, lat, lng, radiusMeters), nil
}

func filterByDistance(objects []GeoObject, lat, lng, radiusMeters float64) []SearchResult {
	results := make([]SearchResult, 0)

	for _, object := range objects {
		distance := haversineMeters(lat, lng, object.Lat, object.Lng)

		// даже если точка попала в нужную geohash-ячейку,
		// она может быть вне радиуса → проверяем точно
		if distance <= radiusMeters {
			results = append(results, SearchResult{
				Object:   object,
				Distance: distance,
			})
		}
	}
	// сначала ближайшие
	sortSearchResults(results)
	return results
}