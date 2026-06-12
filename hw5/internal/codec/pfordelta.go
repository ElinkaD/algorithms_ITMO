package codec

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sort"
)

const PForBlockSize = 128

func Encode(values []uint32) []byte {
	var out bytes.Buffer
	for start := 0; start < len(values); start += PForBlockSize {
		end := start + PForBlockSize
		if end > len(values) {
			end = len(values)
		}
		encodeBlock(&out, values[start:end])
	}
	return out.Bytes()
}

func Decode(data []byte) []uint32 {
	var out []uint32
	r := bytes.NewReader(data)
	for r.Len() > 0 {
		block, err := decodeBlock(r)
		if err != nil {
			return out
		}
		out = append(out, block...)
	}
	return out
}

func encodeBlock(out *bytes.Buffer, block []uint32) {
	width := percentileWidth(block, 90)
	count := len(block)
	putUvarint(out, uint64(count))
	out.WriteByte(byte(width))

	var maxBase uint32
	if width >= 32 {
		maxBase = ^uint32(0)
	} else if width == 0 {
		maxBase = 0
	} else {
		maxBase = (uint32(1) << width) - 1
	}
	base := make([]uint32, count)
	var exIdx []uint32
	var exVal []uint32
	for i, v := range block {
		if v > maxBase {
			exIdx = append(exIdx, uint32(i))
			exVal = append(exVal, v)
			base[i] = maxBase
		} else {
			base[i] = v
		}
	}
	packed := Pack(base, width)
	putUvarint(out, uint64(len(packed)))
	out.Write(packed)
	putUvarint(out, uint64(len(exIdx)))
	for _, v := range exIdx {
		putUvarint(out, uint64(v))
	}
	for _, v := range exVal {
		putUvarint(out, uint64(v))
	}
}

func decodeBlock(r *bytes.Reader) ([]uint32, error) {
	count64, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, err
	}
	widthByte, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	width := int(widthByte)
	packedLen64, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, err
	}
	packedLen := int(packedLen64)
	if packedLen > r.Len() {
		return nil, io.ErrUnexpectedEOF
	}
	packed := make([]byte, packedLen)
	if _, err := io.ReadFull(r, packed); err != nil {
		return nil, err
	}
	count := int(count64)
	values := Unpack(packed, width, count)
	exCount64, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, err
	}
	exCount := int(exCount64)
	indexes := make([]uint32, exCount)
	for i := range indexes {
		v, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, err
		}
		indexes[i] = uint32(v)
	}
	for i := 0; i < exCount; i++ {
		v, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, err
		}
		if indexes[i] >= uint32(len(values)) {
			return nil, fmt.Errorf("bad exception index %d", indexes[i])
		}
		values[indexes[i]] = uint32(v)
	}
	return values, nil
}

func percentileWidth(values []uint32, percentile int) int {
	if len(values) == 0 {
		return 0
	}
	cp := append([]uint32(nil), values...)
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	pos := (len(cp)*percentile + 99) / 100
	if pos <= 0 {
		pos = 1
	}
	if pos > len(cp) {
		pos = len(cp)
	}
	return BitWidth(cp[pos-1])
}

func putUvarint(out *bytes.Buffer, v uint64) {
	var buf [10]byte
	n := binary.PutUvarint(buf[:], v)
	out.Write(buf[:n])
}
