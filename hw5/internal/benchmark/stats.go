package benchmark

import "math"

func mean(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	var sum float64
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}

func stddevSample(values []float64) float64 {
	if len(values) < 2 {
		return 0
	}
	m := mean(values)
	var sum float64
	for _, v := range values {
		d := v - m
		sum += d * d
	}
	return math.Sqrt(sum / float64(len(values)-1))
}

func ci95(values []float64) (low, high float64) {
	if len(values) == 0 {
		return 0, 0
	}
	m := mean(values)
	if len(values) == 1 {
		return m, m
	}
	se := stddevSample(values) / math.Sqrt(float64(len(values)))
	t := 1.96
	if len(values) == 5 {
		t = 2.776
	}
	margin := t * se
	return m - margin, m + margin
}

func Mean(values []float64) float64 {
	return mean(values)
}

func CI95(values []float64) (low, high float64) {
	return ci95(values)
}
