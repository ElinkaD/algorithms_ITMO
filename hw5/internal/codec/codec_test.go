package codec

import "testing"

func TestDeltaEncodeDecode(t *testing.T) {
	values := []uint32{10, 15, 21}
	gaps := DeltaEncode(values)
	want := []uint32{10, 5, 6}
	for i := range want {
		if gaps[i] != want[i] {
			t.Fatalf("gap %d=%d want %d", i, gaps[i], want[i])
		}
	}
	decoded := DeltaDecode(gaps)
	for i := range values {
		if decoded[i] != values[i] {
			t.Fatalf("decoded %d=%d want %d", i, decoded[i], values[i])
		}
	}
}

func TestBitpackRoundTrip(t *testing.T) {
	cases := []struct {
		width  int
		values []uint32
	}{
		{0, []uint32{0, 0, 0}},
		{1, []uint32{0, 1, 1, 0}},
		{3, []uint32{0, 7, 3, 5}},
		{8, []uint32{1, 255, 17}},
		{13, []uint32{8191, 17, 4096}},
		{31, []uint32{1<<31 - 1, 17, 1024}},
	}
	for _, tc := range cases {
		got := Unpack(Pack(tc.values, tc.width), tc.width, len(tc.values))
		for i := range tc.values {
			if got[i] != tc.values[i] {
				t.Fatalf("width=%d got[%d]=%d want %d", tc.width, i, got[i], tc.values[i])
			}
		}
	}
}

func TestPForDeltaRoundTrip(t *testing.T) {
	var values []uint32
	for i := uint32(0); i < 400; i++ {
		v := i % 31
		if i%37 == 0 {
			v = 100000 + i
		}
		values = append(values, v)
	}
	got := Decode(Encode(values))
	if len(got) != len(values) {
		t.Fatalf("len=%d want=%d", len(got), len(values))
	}
	for i := range values {
		if got[i] != values[i] {
			t.Fatalf("got[%d]=%d want %d", i, got[i], values[i])
		}
	}
}
