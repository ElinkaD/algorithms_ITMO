package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"

	"hw5/internal/codec"
	"hw5/internal/index"
)

func Write(idx *index.MemoryIndex, path string) error {
	return SegmentWriter{}.Write(idx, path)
}

type SegmentWriter struct{}

func (SegmentWriter) Write(idx *index.MemoryIndex, path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	terms := idx.Terms()
	var postings bytes.Buffer
	entries := make([]termEntry, 0, len(terms))
	for _, term := range terms {
		pl := idx.Lookup(term)
		start := postings.Len()
		if err := writePostingList(&postings, pl); err != nil {
			return err
		}
		entries = append(entries, termEntry{
			Term:           term,
			DF:             pl.DF,
			TTF:            pl.TTF,
			PostingsOffset: int64(start),
			PostingsLength: int64(postings.Len() - start),
		})
	}

	var dict bytes.Buffer
	for _, e := range entries {
		putUvarint(&dict, uint64(len(e.Term)))
		dict.WriteString(e.Term)
		putUvarint(&dict, uint64(e.DF))
		putUvarint(&dict, uint64(e.TTF))
		putUvarint(&dict, uint64(e.PostingsOffset))
		putUvarint(&dict, uint64(e.PostingsLength))
	}

	var norms bytes.Buffer
	docIDs := make([]int, 0, len(idx.DocLens))
	for docID := range idx.DocLens {
		docIDs = append(docIDs, docID)
	}
	sort.Ints(docIDs)
	putUvarint(&norms, uint64(len(docIDs)))
	for _, docID := range docIDs {
		putUvarint(&norms, uint64(docID))
		putUvarint(&norms, uint64(idx.DocLens[docID]))
	}

	var titles bytes.Buffer
	putUvarint(&titles, uint64(len(idx.Titles)))
	for _, docID := range docIDs {
		title := idx.Titles[docID]
		putUvarint(&titles, uint64(docID))
		putUvarint(&titles, uint64(len(title)))
		titles.WriteString(title)
	}

	dictOffset := int64(headerSize)
	postingsOffset := dictOffset + int64(dict.Len())
	docNormsOffset := postingsOffset + int64(postings.Len())
	titlesOffset := docNormsOffset + int64(norms.Len())

	var out bytes.Buffer
	out.Write(make([]byte, headerSize))
	out.Write(dict.Bytes())
	out.Write(postings.Bytes())
	out.Write(norms.Bytes())
	out.Write(titles.Bytes())
	writeHeader(out.Bytes()[:headerSize], idx, len(terms), dictOffset, postingsOffset, docNormsOffset, titlesOffset)
	return os.WriteFile(path, out.Bytes(), 0o644)
}

func writePostingList(out *bytes.Buffer, pl index.PostingList) error {
	putUvarint(out, uint64(len(pl.Postings)))
	docIDs := make([]uint32, len(pl.Postings))
	freqs := make([]uint32, len(pl.Postings))
	for i, p := range pl.Postings {
		if p.DocID < 0 || p.TF < 0 {
			return fmt.Errorf("negative posting field")
		}
		docIDs[i] = uint32(p.DocID)
		freqs[i] = uint32(p.TF)
	}
	docBytes := codec.Encode(codec.DeltaEncode(docIDs))
	putBytes(out, docBytes)
	for _, tf := range freqs {
		putUvarint(out, uint64(tf))
	}
	for _, p := range pl.Postings {
		positions := make([]uint32, len(p.Positions))
		for i, pos := range p.Positions {
			positions[i] = uint32(pos)
		}
		putUvarint(out, uint64(len(positions)))
		putBytes(out, codec.Encode(codec.DeltaEncode(positions)))
	}
	putUvarint(out, uint64(len(pl.Skips)))
	for _, sp := range pl.Skips {
		putUvarint(out, uint64(sp.FromIndex))
		putUvarint(out, uint64(sp.ToIndex))
		putUvarint(out, uint64(sp.TargetDocID))
		putUvarint(out, uint64(sp.Offset))
	}
	return nil
}

func writeHeader(dst []byte, idx *index.MemoryIndex, termCount int, dictOffset, postingsOffset, docNormsOffset, titlesOffset int64) {
	copy(dst[:8], []byte(SegmentMagic))
	binary.LittleEndian.PutUint32(dst[8:12], SegmentVersion)
	binary.LittleEndian.PutUint32(dst[12:16], CodecPForDelta)
	binary.LittleEndian.PutUint64(dst[16:24], uint64(idx.DocCount))
	binary.LittleEndian.PutUint64(dst[24:32], uint64(termCount))
	binary.LittleEndian.PutUint64(dst[32:40], uint64(dictOffset))
	binary.LittleEndian.PutUint64(dst[40:48], uint64(postingsOffset))
	binary.LittleEndian.PutUint64(dst[48:56], uint64(docNormsOffset))
	binary.LittleEndian.PutUint64(dst[56:64], uint64(titlesOffset))
	binary.LittleEndian.PutUint64(dst[64:72], math.Float64bits(idx.AvgDocLen))
}

func putBytes(out *bytes.Buffer, b []byte) {
	putUvarint(out, uint64(len(b)))
	out.Write(b)
}

func putUvarint(out *bytes.Buffer, v uint64) {
	var buf [10]byte
	n := binary.PutUvarint(buf[:], v)
	out.Write(buf[:n])
}
