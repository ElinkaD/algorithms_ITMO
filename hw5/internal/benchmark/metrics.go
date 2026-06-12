package benchmark

type CorpusStats struct {
	CorpusName      string
	Docs            int
	TotalTokens     int
	UniqueTerms     int
	AvgDocLen       float64
	MinDocLen       int
	MaxDocLen       int
	TotalPostings   int
	TotalPositions  int
	VocabularySize  int
	Top10TermsByDF  string
	Top10TermsByTTF string
}

type BuildStats struct {
	CorpusName           string
	Docs                 int
	TotalBuildTimeMS     float64
	SegmentWriteTimeMS   float64
	TotalTimeMS          float64
	DocsPerSec           float64
	TokensPerSec         float64
	UniqueTerms          int
	TotalPostings        int
	SegmentFileSizeBytes int64
}

type QueryLatency struct {
	CorpusName         string
	Docs               int
	Query              string
	OperatorType       string
	RankMode           string
	Backend            string
	Iterations         int
	Hits               int
	AvgLatencyMS       float64
	P50LatencyMS       float64
	P95LatencyMS       float64
	MinLatencyMS       float64
	MaxLatencyMS       float64
	QPS                float64
	AllocBytesPerQuery uint64
	AllocsPerQuery     uint64
}

type MmapVsMemory struct {
	Docs              int
	Query             string
	OperatorType      string
	MemoryLatencyMS   float64
	MmapLatencyMS     float64
	MmapToMemoryRatio float64
	MemoryHits        int
	MmapHits          int
	ResultsEqual      bool
}

type RankingStats struct {
	Query                string
	Hits                 int
	TopK                 int
	BooleanOnlyLatencyMS float64
	TFIDFLatencyMS       float64
	BM25LatencyMS        float64
	TFIDFOverheadPercent float64
	BM25OverheadPercent  float64
	Top1DocIDTFIDF       int
	Top1DocIDBM25        int
	Top1TitleTFIDF       string
	Top1TitleBM25        string
}
