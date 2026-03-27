package geosearch

import "errors"

type Index struct {
	precision int
	tree      *btree
	buckets   map[string][]GeoObject
	// objects нужен только для baseline FullScan.
	// Это отдельный in-memory срез, чтобы было удобно сравнивать индексный поиск с полным перебором.
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

	// B-tree хранит только уникальные ключи geohash,
	// а сами объекты живут в buckets по этому ключу.
	i.tree.Insert(hash)
	i.buckets[hash] = append(i.buckets[hash], object)
	i.objects = append(i.objects, object)
	return nil
}

func (i *Index) SearchExact(lat, lng float64) ([]GeoObject, error) {
	hash, err := EncodeGeohash(lat, lng, i.precision)
	if err != nil {
		return nil, err
	}

	if !i.tree.Contains(hash) {
		return nil, nil
	}

	bucket := i.buckets[hash]
	result := make([]GeoObject, len(bucket))
	copy(result, bucket)
	return result, nil
}

func (i *Index) SearchNearby(lat, lng, radiusMeters float64) ([]SearchResult, error) {
	if radiusMeters < 0 {
		return nil, errors.New("radius must be non-negative")
	}

	hashes, err := neighboringHashes(lat, lng, radiusMeters, i.precision)
	if err != nil {
		return nil, err
	}

	candidates := make([]GeoObject, 0)
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
		// Даже если объект попал в подходящую geohash-ячейку,
		// окончательное решение все равно принимаем по реальному расстоянию.
		if distance <= radiusMeters {
			results = append(results, SearchResult{
				Object:   object,
				Distance: distance,
			})
		}
	}
	sortSearchResults(results)
	return results
}
