package storage

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"

	"hw5/internal/codec"
	"hw5/internal/index"
)

type SegmentSizeStats struct {
	CorpusName                string
	Docs                      int
	RawDocIDsBytes            int64
	RawFreqsBytes             int64
	RawPositionsBytes         int64
	RawTotalBytes             int64
	CompressedDocIDsBytes     int64
	CompressedFreqsBytes      int64
	CompressedPositionsBytes  int64
	CompressedSkipBytes       int64
	CompressedDictionaryBytes int64
	CompressedDocNormsBytes   int64
	CompressedTitlesBytes     int64
	CompressedTotalBytes      int64
	SegmentFileSizeBytes      int64
	CompressionRatio          float64
	SpaceSavingPercent        float64
}

func ComputeSizeStats(corpusName string, idx *index.MemoryIndex, segmentPath string) (SegmentSizeStats, error) {
	stats := SegmentSizeStats{CorpusName: corpusName, Docs: idx.DocCount}
	for _, term := range idx.Terms() {
		pl := idx.Lookup(term)
		postings := int64(len(pl.Postings))
		stats.RawDocIDsBytes += postings * 4
		stats.RawFreqsBytes += postings * 4
		docIDs := make([]uint32, len(pl.Postings))
		for i, p := range pl.Postings {
			docIDs[i] = uint32(p.DocID)
			stats.RawPositionsBytes += int64(len(p.Positions) * 4)
			stats.CompressedFreqsBytes += int64(uvarintLen(uint64(p.TF)))
			positions := make([]uint32, len(p.Positions))
			for j, pos := range p.Positions {
				positions[j] = uint32(pos)
			}
			stats.CompressedPositionsBytes += int64(len(codec.Encode(codec.DeltaEncode(positions))))
		}
		stats.CompressedDocIDsBytes += int64(len(codec.Encode(codec.DeltaEncode(docIDs))))
		stats.CompressedSkipBytes += int64(uvarintLen(uint64(len(pl.Skips))))
		for _, sp := range pl.Skips {
			stats.CompressedSkipBytes += int64(uvarintLen(uint64(sp.FromIndex)))
			stats.CompressedSkipBytes += int64(uvarintLen(uint64(sp.ToIndex)))
			stats.CompressedSkipBytes += int64(uvarintLen(uint64(sp.TargetDocID)))
			stats.CompressedSkipBytes += int64(uvarintLen(uint64(sp.Offset)))
		}
		stats.CompressedDictionaryBytes += int64(uvarintLen(uint64(len(term))) + len(term))
		stats.CompressedDictionaryBytes += int64(uvarintLen(uint64(pl.DF)))
		stats.CompressedDictionaryBytes += int64(uvarintLen(uint64(pl.TTF)))
		stats.CompressedDictionaryBytes += 20
	}
	stats.RawTotalBytes = stats.RawDocIDsBytes + stats.RawFreqsBytes + stats.RawPositionsBytes
	stats.CompressedDocNormsBytes += int64(uvarintLen(uint64(len(idx.DocLens))))
	for docID, docLen := range idx.DocLens {
		stats.CompressedDocNormsBytes += int64(uvarintLen(uint64(docID)) + uvarintLen(uint64(docLen)))
	}
	stats.CompressedTitlesBytes += int64(uvarintLen(uint64(len(idx.Titles))))
	for docID, title := range idx.Titles {
		stats.CompressedTitlesBytes += int64(uvarintLen(uint64(docID)) + uvarintLen(uint64(len(title))) + len(title))
	}
	stats.CompressedTotalBytes = stats.CompressedDocIDsBytes +
		stats.CompressedFreqsBytes +
		stats.CompressedPositionsBytes +
		stats.CompressedSkipBytes +
		stats.CompressedDictionaryBytes +
		stats.CompressedDocNormsBytes +
		stats.CompressedTitlesBytes
	if segmentPath != "" {
		if st, err := os.Stat(segmentPath); err == nil {
			stats.SegmentFileSizeBytes = st.Size()
		} else if !os.IsNotExist(err) {
			return stats, err
		}
	}
	if stats.RawTotalBytes > 0 && stats.CompressedTotalBytes > 0 {
		stats.CompressionRatio = float64(stats.RawTotalBytes) / float64(stats.CompressedTotalBytes)
		stats.SpaceSavingPercent = 100 * (1 - float64(stats.CompressedTotalBytes)/float64(stats.RawTotalBytes))
	}
	return stats, nil
}

func WriteCompressionCSV(path string, rows []SegmentSizeStats) error {
	if err := os.MkdirAll(dirname(path), 0o755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	header := []string{
		"corpus_name", "docs",
		"raw_docids_bytes", "raw_freqs_bytes", "raw_positions_bytes", "raw_total_bytes",
		"compressed_docids_bytes", "compressed_freqs_bytes", "compressed_positions_bytes",
		"compressed_skip_bytes", "compressed_dictionary_bytes", "compressed_docnorms_bytes",
		"compressed_titles_bytes", "compressed_total_bytes", "segment_file_size_bytes",
		"compression_ratio", "space_saving_percent",
	}
	if err := w.Write(header); err != nil {
		return err
	}
	for _, row := range rows {
		record := []string{
			row.CorpusName,
			strconv.Itoa(row.Docs),
			i64(row.RawDocIDsBytes),
			i64(row.RawFreqsBytes),
			i64(row.RawPositionsBytes),
			i64(row.RawTotalBytes),
			i64(row.CompressedDocIDsBytes),
			i64(row.CompressedFreqsBytes),
			i64(row.CompressedPositionsBytes),
			i64(row.CompressedSkipBytes),
			i64(row.CompressedDictionaryBytes),
			i64(row.CompressedDocNormsBytes),
			i64(row.CompressedTitlesBytes),
			i64(row.CompressedTotalBytes),
			i64(row.SegmentFileSizeBytes),
			fmt.Sprintf("%.4f", row.CompressionRatio),
			fmt.Sprintf("%.2f", row.SpaceSavingPercent),
		}
		if err := w.Write(record); err != nil {
			return err
		}
	}
	return w.Error()
}

func uvarintLen(v uint64) int {
	n := 1
	for v >= 0x80 {
		v >>= 7
		n++
	}
	return n
}

func i64(v int64) string {
	return strconv.FormatInt(v, 10)
}

func dirname(path string) string {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' {
			return path[:i]
		}
	}
	return "."
}
