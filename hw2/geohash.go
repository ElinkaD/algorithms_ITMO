package geosearch

import (
	"errors"
	"math"
)

const geohashBase32 = "0123456789bcdefghjkmnpqrstuvwxyz"

var geohashDecodeMap = buildDecodeMap()

type boundingBox struct {
	minLat float64
	maxLat float64
	minLng float64
	maxLng float64
}

func EncodeGeohash(lat, lng float64, precision int) (string, error) {
	if precision <= 0 {
		return "", errors.New("precision must be positive")
	}
	if lat < -90 || lat > 90 {
		return "", errors.New("latitude must be in [-90, 90]")
	}
	if lng < -180 || lng > 180 {
		return "", errors.New("longitude must be in [-180, 180]")
	}

	var hash []byte
	minLat, maxLat := -90.0, 90.0
	minLng, maxLng := -180.0, 180.0
	bit, ch := 0, 0
	evenBit := true

	for len(hash) < precision {
		if evenBit {
			// В geohash биты широты и долготы чередуются.
			// Для evenBit делим текущий диапазон по долготе.
			mid := (minLng + maxLng) / 2
			if lng >= mid {
				ch = (ch << 1) | 1
				minLng = mid
			} else {
				ch <<= 1
				maxLng = mid
			}
		} else {
			// Для oddBit делим диапазон по широте.
			mid := (minLat + maxLat) / 2
			if lat >= mid {
				ch = (ch << 1) | 1
				minLat = mid
			} else {
				ch <<= 1
				maxLat = mid
			}
		}

		evenBit = !evenBit
		bit++

		if bit == 5 {
			hash = append(hash, geohashBase32[ch])
			bit, ch = 0, 0
		}
	}

	return string(hash), nil
}

func DecodeBoundingBox(hash string) (boundingBox, error) {
	if hash == "" {
		return boundingBox{}, errors.New("geohash must not be empty")
	}

	box := boundingBox{
		minLat: -90,
		maxLat: 90,
		minLng: -180,
		maxLng: 180,
	}

	evenBit := true
	for _, r := range hash {
		value, ok := geohashDecodeMap[r]
		if !ok {
			return boundingBox{}, errors.New("invalid geohash symbol")
		}

		for mask := 16; mask != 0; mask >>= 1 {
			if evenBit {
				mid := (box.minLng + box.maxLng) / 2
				if value&mask != 0 {
					box.minLng = mid
				} else {
					box.maxLng = mid
				}
			} else {
				mid := (box.minLat + box.maxLat) / 2
				if value&mask != 0 {
					box.minLat = mid
				} else {
					box.maxLat = mid
				}
			}
			evenBit = !evenBit
		}
	}

	return box, nil
}

func (b boundingBox) center() (float64, float64) {
	return (b.minLat + b.maxLat) / 2, (b.minLng + b.maxLng) / 2
}

func (b boundingBox) latHeightMeters() float64 {
	return haversineMeters(b.minLat, b.minLng, b.maxLat, b.minLng)
}

func (b boundingBox) lngWidthMeters() float64 {
	centerLat, _ := b.center()
	return haversineMeters(centerLat, b.minLng, centerLat, b.maxLng)
}

func neighboringHashes(lat, lng, radiusMeters float64, precision int) ([]string, error) {
	window := searchBoundingBox(lat, lng, radiusMeters)

	cellHeight, cellWidth, err := geohashCellSizeDegrees(precision)
	if err != nil {
		return nil, err
	}

	// тут я специально иду по окну поиска с шагом размера geohash-ячейки.
	// так мы трогаем только соседние ячейки запроса, а не все buckets индекса.
	seen := make(map[string]struct{})
	result := make([]string, 0)
	epsilonLat := math.Min(cellHeight/10, 1e-9)
	epsilonLng := math.Min(cellWidth/10, 1e-9)

	for sampleLat := window.minLat; sampleLat <= window.maxLat+epsilonLat; sampleLat += cellHeight {
		clampedLat := clampLat(sampleLat)
		for _, lngInterval := range splitLongitudeInterval(window.minLng, window.maxLng) {
			for sampleLng := lngInterval[0]; sampleLng <= lngInterval[1]+epsilonLng; sampleLng += cellWidth {
				normalizedLng := normalizeLng(sampleLng)

				hash, err := EncodeGeohash(clampedLat, normalizedLng, precision)
				if err != nil {
					return nil, err
				}
				if _, ok := seen[hash]; ok {
					continue
				}
				seen[hash] = struct{}{}
				result = append(result, hash)
			}
		}
	}

	// на всякий случай добавляю geohash самой точки запроса.
	// это помогает не зависеть от того, как именно легли границы шага.
	centerHash, err := EncodeGeohash(clampLat(lat), normalizeLng(lng), precision)
	if err != nil {
		return nil, err
	}
	if _, ok := seen[centerHash]; !ok {
		result = append(result, centerHash)
	}

	return result, nil
}

func geohashCellSizeDegrees(precision int) (float64, float64, error) {
	sampleHash, err := EncodeGeohash(0, 0, precision)
	if err != nil {
		return 0, 0, err
	}

	box, err := DecodeBoundingBox(sampleHash)
	if err != nil {
		return 0, 0, err
	}

	return box.maxLat - box.minLat, box.maxLng - box.minLng, nil
}

func searchBoundingBox(lat, lng, radiusMeters float64) boundingBox {
	latDelta := radiansToDegrees(radiusMeters / earthRadiusMeters)
	minLat := clampLat(lat - latDelta)
	maxLat := clampLat(lat + latDelta)

	// Если окно поиска цепляет полюс, то по долготе оно фактически покрывает весь мир.
	if minLat <= -90 || maxLat >= 90 {
		return boundingBox{
			minLat: minLat,
			maxLat: maxLat,
			minLng: -180,
			maxLng: 180,
		}
	}

	maxAbsLat := math.Max(math.Abs(minLat), math.Abs(maxLat))
	cosLat := math.Cos(degreesToRadians(maxAbsLat))
	lngDelta := 180.0
	if math.Abs(cosLat) > 1e-12 {
		// Берем худший случай по широте внутри всего окна,
		// иначе можно занизить охват по долготе и потерять точки.
		lngDelta = radiansToDegrees(radiusMeters / (earthRadiusMeters * cosLat))
	}

	// Если радиус по долготе стал слишком большим, опять же считаем,
	// что окно охватывает весь диапазон долгот.
	if lngDelta >= 180 {
		return boundingBox{
			minLat: minLat,
			maxLat: maxLat,
			minLng: -180,
			maxLng: 180,
		}
	}

	return boundingBox{
		minLat: minLat,
		maxLat: maxLat,
		minLng: normalizeLng(lng - lngDelta),
		maxLng: normalizeLng(lng + lngDelta),
	}
}

func (b boundingBox) intersects(other boundingBox) bool {
	if b.maxLat < other.minLat || other.maxLat < b.minLat {
		return false
	}

	// По долготе интервал может "разрываться" на границе -180/180,
	// поэтому обычного сравнения min/max недостаточно.
	return longitudeIntervalsIntersect(b.minLng, b.maxLng, other.minLng, other.maxLng)
}

func buildDecodeMap() map[rune]int {
	decodeMap := make(map[rune]int, len(geohashBase32))
	for i, r := range geohashBase32 {
		decodeMap[r] = i
	}
	return decodeMap
}

func longitudeIntervalsIntersect(aMin, aMax, bMin, bMax float64) bool {
	aIntervals := splitLongitudeInterval(aMin, aMax)
	bIntervals := splitLongitudeInterval(bMin, bMax)

	for _, a := range aIntervals {
		for _, b := range bIntervals {
			if a[1] >= b[0] && b[1] >= a[0] {
				return true
			}
		}
	}

	return false
}

func splitLongitudeInterval(minLng, maxLng float64) [][2]float64 {
	if minLng <= maxLng {
		return [][2]float64{{minLng, maxLng}}
	}

	// Если min > max, значит интервал перешел через линию перемены дат
	// и его нужно рассматривать как два куска.
	return [][2]float64{
		{minLng, 180},
		{-180, maxLng},
	}
}
