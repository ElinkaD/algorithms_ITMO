package codec

func BitWidth(max uint32) int {
	if max == 0 {
		return 0
	}
	w := 0
	for max > 0 {
		w++
		max >>= 1
	}
	return w
}

func Pack(values []uint32, width int) []byte {
	if width <= 0 || len(values) == 0 {
		return nil
	}
	out := make([]byte, (len(values)*width+7)/8)
	bit := 0
	var mask uint32
	if width >= 32 {
		mask = ^uint32(0)
	} else {
		mask = (uint32(1) << width) - 1
	}
	for _, v := range values {
		v &= mask
		for i := 0; i < width; i++ {
			if (v>>i)&1 == 1 {
				out[bit/8] |= byte(1 << (bit % 8))
			}
			bit++
		}
	}
	return out
}

func Unpack(data []byte, width int, count int) []uint32 {
	out := make([]uint32, count)
	if width <= 0 || count == 0 {
		return out
	}
	bit := 0
	for i := 0; i < count; i++ {
		var v uint32
		for j := 0; j < width; j++ {
			if bit/8 < len(data) && (data[bit/8]&(1<<(bit%8))) != 0 {
				v |= 1 << j
			}
			bit++
		}
		out[i] = v
	}
	return out
}
