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
