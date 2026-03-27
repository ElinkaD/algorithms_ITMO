package geosearch

import (
	"math"
	"sort"
)

const earthRadiusMeters = 6371000.0

type GeoObject struct {
	ID  string
	Lat float64
	Lng float64
}

type SearchResult struct {
	Object   GeoObject
	Distance float64
}

func haversineMeters(lat1, lng1, lat2, lng2 float64) float64 {
	lat1Rad := degreesToRadians(lat1)
	lng1Rad := degreesToRadians(lng1)
	lat2Rad := degreesToRadians(lat2)
	lng2Rad := degreesToRadians(lng2)

	dLat := lat2Rad - lat1Rad
	dLng := lng2Rad - lng1Rad

	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1Rad)*math.Cos(lat2Rad)*math.Sin(dLng/2)*math.Sin(dLng/2)
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))

	return earthRadiusMeters * c
}

func sortSearchResults(results []SearchResult) {
	sort.Slice(results, func(i, j int) bool {
		if results[i].Distance == results[j].Distance {
			return results[i].Object.ID < results[j].Object.ID
		}
		return results[i].Distance < results[j].Distance
	})
}

func degreesToRadians(v float64) float64 {
	return v * math.Pi / 180.0
}

func radiansToDegrees(v float64) float64 {
	return v * 180.0 / math.Pi
}

func clampLat(lat float64) float64 {
	return math.Max(-90, math.Min(90, lat))
}

func normalizeLng(lng float64) float64 {
	for lng < -180 {
		lng += 360
	}
	for lng > 180 {
		lng -= 360
	}
	if lng == -180 {
		return 180
	}
	return lng
}
