package storage

const (
	SegmentMagic   = "HW5IDX01"
	SegmentVersion = uint32(1)
	CodecPForDelta = uint32(1)
	headerSize     = 80
)

type SegmentStats struct {
	DocCount       int
	TermCount      int
	AvgDocLen      float64
	SegmentBytes   int64
	PostingsOffset int64
	DocNormsOffset int64
	TitlesOffset   int64
}

type termEntry struct {
	Term           string
	DF             int
	TTF            int
	PostingsOffset int64
	PostingsLength int64
}
