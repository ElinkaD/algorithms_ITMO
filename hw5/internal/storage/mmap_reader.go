package storage

import (
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"syscall"

	"hw5/internal/codec"
	"hw5/internal/index"
)

type MmapSegmentReader struct {
	file           *os.File
	data           []byte
	dictionary     map[string]termEntry
	stats          SegmentStats
	postingsOffset int64
	docLens        map[int]int
	titles         map[int]string
}

func Open(path string) (*MmapSegmentReader, error) {
	r := &MmapSegmentReader{}
	return r, r.Open(path)
}

func (r *MmapSegmentReader) Open(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return err
	}
	data, err := syscall.Mmap(int(f.Fd()), 0, int(st.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		_ = f.Close()
		return err
	}
	r.file = f
	r.data = data
	r.stats.SegmentBytes = st.Size()
	return r.parse()
}

func (r *MmapSegmentReader) Close() error {
	var err error
	if r.data != nil {
		err = syscall.Munmap(r.data)
		r.data = nil
	}
	if r.file != nil {
		if e := r.file.Close(); err == nil {
			err = e
		}
		r.file = nil
	}
	return err
}

func (r *MmapSegmentReader) Stats() SegmentStats {
	return r.stats
}

func (r *MmapSegmentReader) LookupTerm(term string) (*index.PostingList, error) {
	e, ok := r.dictionary[term]
	if !ok {
		pl := index.PostingList{Term: term}
		return &pl, nil
	}
	start := r.postingsOffset + e.PostingsOffset
	end := start + e.PostingsLength
	if start < 0 || end > int64(len(r.data)) || start > end {
		return nil, fmt.Errorf("bad postings range for %q", term)
	}
	pl, err := decodePostingList(term, r.data[start:end])
	if err != nil {
		return nil, err
	}
	pl.DF = e.DF
	pl.TTF = e.TTF
	return &pl, nil
}

func (r *MmapSegmentReader) Materialize(terms []string) (*index.MemoryIndex, error) {
	idx := &index.MemoryIndex{
		Postings:  make(map[string]index.PostingList),
		DocLens:   make(map[int]int, len(r.docLens)),
		Titles:    make(map[int]string, len(r.titles)),
		DocCount:  r.stats.DocCount,
		AvgDocLen: r.stats.AvgDocLen,
		TermStats: make(map[string]index.TermStat),
	}
	for docID, l := range r.docLens {
		idx.DocLens[docID] = l
	}
	for docID, title := range r.titles {
		idx.Titles[docID] = title
	}
	seen := make(map[string]struct{})
	for _, term := range terms {
		if _, ok := seen[term]; ok {
			continue
		}
		seen[term] = struct{}{}
		pl, err := r.LookupTerm(term)
		if err != nil {
			return nil, err
		}
		idx.Postings[term] = *pl
		idx.TermStats[term] = index.TermStat{DF: pl.DF, TTF: pl.TTF}
	}
	return idx, nil
}

func (r *MmapSegmentReader) parse() error {
	if len(r.data) < headerSize {
		return fmt.Errorf("segment too small")
	}
	if string(r.data[:8]) != SegmentMagic {
		return fmt.Errorf("bad magic")
	}
	version := binary.LittleEndian.Uint32(r.data[8:12])
	codecID := binary.LittleEndian.Uint32(r.data[12:16])
	if version != SegmentVersion || codecID != CodecPForDelta {
		return fmt.Errorf("unsupported segment version/codec")
	}
	docCount := int(binary.LittleEndian.Uint64(r.data[16:24]))
	termCount := int(binary.LittleEndian.Uint64(r.data[24:32]))
	dictOffset := int64(binary.LittleEndian.Uint64(r.data[32:40]))
	postingsOffset := int64(binary.LittleEndian.Uint64(r.data[40:48]))
	docNormsOffset := int64(binary.LittleEndian.Uint64(r.data[48:56]))
	titlesOffset := int64(binary.LittleEndian.Uint64(r.data[56:64]))
	avgDocLen := math.Float64frombits(binary.LittleEndian.Uint64(r.data[64:72]))
	r.stats.DocCount = docCount
	r.stats.TermCount = termCount
	r.stats.AvgDocLen = avgDocLen
	r.stats.PostingsOffset = postingsOffset
	r.stats.DocNormsOffset = docNormsOffset
	r.stats.TitlesOffset = titlesOffset
	r.postingsOffset = postingsOffset
	if dictOffset < headerSize || postingsOffset < dictOffset || docNormsOffset < postingsOffset || titlesOffset < docNormsOffset || titlesOffset > int64(len(r.data)) {
		return fmt.Errorf("bad segment offsets")
	}
	if err := r.parseDictionary(r.data[dictOffset:postingsOffset], termCount); err != nil {
		return err
	}
	if err := r.parseDocNorms(r.data[docNormsOffset:titlesOffset]); err != nil {
		return err
	}
	return r.parseTitles(r.data[titlesOffset:])
}

func (r *MmapSegmentReader) parseDictionary(data []byte, termCount int) error {
	r.dictionary = make(map[string]termEntry, termCount)
	off := 0
	for i := 0; i < termCount; i++ {
		termLen, err := readUvarint(data, &off)
		if err != nil {
			return err
		}
		if off+int(termLen) > len(data) {
			return fmt.Errorf("bad term length")
		}
		term := string(data[off : off+int(termLen)])
		off += int(termLen)
		df, err := readUvarint(data, &off)
		if err != nil {
			return err
		}
		ttf, err := readUvarint(data, &off)
		if err != nil {
			return err
		}
		postingsOffset, err := readUvarint(data, &off)
		if err != nil {
			return err
		}
		postingsLength, err := readUvarint(data, &off)
		if err != nil {
			return err
		}
		r.dictionary[term] = termEntry{Term: term, DF: int(df), TTF: int(ttf), PostingsOffset: int64(postingsOffset), PostingsLength: int64(postingsLength)}
	}
	return nil
}

func (r *MmapSegmentReader) parseDocNorms(data []byte) error {
	r.docLens = make(map[int]int)
	off := 0
	count, err := readUvarint(data, &off)
	if err != nil {
		return err
	}
	for i := 0; i < int(count); i++ {
		docID, err := readUvarint(data, &off)
		if err != nil {
			return err
		}
		l, err := readUvarint(data, &off)
		if err != nil {
			return err
		}
		r.docLens[int(docID)] = int(l)
	}
	return nil
}

func (r *MmapSegmentReader) parseTitles(data []byte) error {
	r.titles = make(map[int]string)
	off := 0
	count, err := readUvarint(data, &off)
	if err != nil {
		return err
	}
	for i := 0; i < int(count); i++ {
		docID, err := readUvarint(data, &off)
		if err != nil {
			return err
		}
		titleLen, err := readUvarint(data, &off)
		if err != nil {
			return err
		}
		if off+int(titleLen) > len(data) {
			return fmt.Errorf("bad title length")
		}
		r.titles[int(docID)] = string(data[off : off+int(titleLen)])
		off += int(titleLen)
	}
	return nil
}

func decodePostingList(term string, data []byte) (index.PostingList, error) {
	off := 0
	count64, err := readUvarint(data, &off)
	if err != nil {
		return index.PostingList{}, err
	}
	count := int(count64)
	docBytes, err := readBytes(data, &off)
	if err != nil {
		return index.PostingList{}, err
	}
	docIDs := codec.DeltaDecode(codec.Decode(docBytes))
	if len(docIDs) != count {
		return index.PostingList{}, fmt.Errorf("decoded docIDs=%d want=%d", len(docIDs), count)
	}
	freqs := make([]uint32, count)
	for i := range freqs {
		tf, err := readUvarint(data, &off)
		if err != nil {
			return index.PostingList{}, err
		}
		freqs[i] = uint32(tf)
	}
	postings := make([]index.Posting, count)
	for i := 0; i < count; i++ {
		posCount, err := readUvarint(data, &off)
		if err != nil {
			return index.PostingList{}, err
		}
		posBytes, err := readBytes(data, &off)
		if err != nil {
			return index.PostingList{}, err
		}
		decoded := codec.DeltaDecode(codec.Decode(posBytes))
		if len(decoded) != int(posCount) {
			return index.PostingList{}, fmt.Errorf("positions=%d want=%d", len(decoded), posCount)
		}
		positions := make([]int, len(decoded))
		for j, pos := range decoded {
			positions[j] = int(pos)
		}
		postings[i] = index.Posting{DocID: int(docIDs[i]), TF: int(freqs[i]), Positions: positions}
	}
	skipCount, err := readUvarint(data, &off)
	if err != nil {
		return index.PostingList{}, err
	}
	skips := make([]index.SkipPointer, int(skipCount))
	for i := range skips {
		from, err := readUvarint(data, &off)
		if err != nil {
			return index.PostingList{}, err
		}
		to, err := readUvarint(data, &off)
		if err != nil {
			return index.PostingList{}, err
		}
		target, err := readUvarint(data, &off)
		if err != nil {
			return index.PostingList{}, err
		}
		offset, err := readUvarint(data, &off)
		if err != nil {
			return index.PostingList{}, err
		}
		skips[i] = index.SkipPointer{FromIndex: int(from), ToIndex: int(to), TargetDocID: int(target), Offset: int64(offset)}
	}
	return index.PostingList{Term: term, Postings: postings, Skips: skips, DF: count}, nil
}

func readBytes(data []byte, off *int) ([]byte, error) {
	n, err := readUvarint(data, off)
	if err != nil {
		return nil, err
	}
	if *off+int(n) > len(data) {
		return nil, fmt.Errorf("bad byte slice length")
	}
	out := data[*off : *off+int(n)]
	*off += int(n)
	return out, nil
}

func readUvarint(data []byte, off *int) (uint64, error) {
	v, n := binary.Uvarint(data[*off:])
	if n <= 0 {
		return 0, fmt.Errorf("bad uvarint at offset %d", *off)
	}
	*off += n
	return v, nil
}
