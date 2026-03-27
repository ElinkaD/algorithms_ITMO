package geosearch

import (
	"fmt"
	"math/rand"
)

func GenerateRandomPoints(rng *rand.Rand, count int) []GeoObject {
	points := make([]GeoObject, 0, count)
	for idx := 0; idx < count; idx++ {
		points = append(points, GeoObject{
			ID:  fmt.Sprintf("point-%d", idx),
			Lat: -90 + rng.Float64()*180,
			Lng: -180 + rng.Float64()*360,
		})
	}
	return points
}
