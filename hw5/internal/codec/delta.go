package codec

func DeltaEncode(values []uint32) []uint32 {
	if len(values) == 0 {
		return nil
	}
	out := make([]uint32, len(values))
	var prev uint32
	for i, v := range values {
		out[i] = v - prev
		prev = v
	}
	return out
}

func DeltaDecode(gaps []uint32) []uint32 {
	if len(gaps) == 0 {
		return nil
	}
	out := make([]uint32, len(gaps))
	var sum uint32
	for i, gap := range gaps {
		sum += gap
		out[i] = sum
	}
	return out
}
