package benchmark

import (
	"hw5/internal/benchdata"
	"hw5/internal/storage"
)

func SyntheticCompressionStats(docs int, segmentPath string) (storage.SegmentSizeStats, error) {
	idx := benchdata.BuildIndex(benchdata.GenerateDocuments(docs))
	if err := storage.Write(idx, segmentPath); err != nil {
		return storage.SegmentSizeStats{}, err
	}
	return storage.ComputeSizeStats("synthetic", idx, segmentPath)
}
