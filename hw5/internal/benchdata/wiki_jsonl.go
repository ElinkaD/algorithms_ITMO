package benchdata

import (
	"bufio"
	"encoding/json"
	"os"

	"hw5/internal/index"
)

type WikiRecord struct {
	ID    int    `json:"id"`
	Title string `json:"title"`
	Text  string `json:"text"`
}

func ReadWikiJSONL(path string, limit int) ([]index.Document, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var docs []index.Document
	sc := bufio.NewScanner(f)
	buf := make([]byte, 1024*1024)
	sc.Buffer(buf, 64*1024*1024)
	for sc.Scan() {
		var rec WikiRecord
		if err := json.Unmarshal(sc.Bytes(), &rec); err != nil {
			return nil, err
		}
		docs = append(docs, index.Document{ID: rec.ID, Title: rec.Title, Text: rec.Text})
		if limit > 0 && len(docs) >= limit {
			break
		}
	}
	return docs, sc.Err()
}

func ReadWikiJSONLChunks(path string, limit int, chunkSize int, fn func(chunkIndex int, docs []index.Document) error) error {
	if chunkSize <= 0 {
		chunkSize = 50000
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	buf := make([]byte, 1024*1024)
	sc.Buffer(buf, 64*1024*1024)
	chunk := make([]index.Document, 0, chunkSize)
	chunkIndex := 0
	total := 0
	flush := func() error {
		if len(chunk) == 0 {
			return nil
		}
		docs := append([]index.Document(nil), chunk...)
		chunk = chunk[:0]
		if err := fn(chunkIndex, docs); err != nil {
			return err
		}
		chunkIndex++
		return nil
	}
	for sc.Scan() {
		var rec WikiRecord
		if err := json.Unmarshal(sc.Bytes(), &rec); err != nil {
			return err
		}
		chunk = append(chunk, index.Document{ID: rec.ID, Title: rec.Title, Text: rec.Text})
		total++
		if len(chunk) >= chunkSize {
			if err := flush(); err != nil {
				return err
			}
		}
		if limit > 0 && total >= limit {
			break
		}
	}
	if err := sc.Err(); err != nil {
		return err
	}
	return flush()
}
