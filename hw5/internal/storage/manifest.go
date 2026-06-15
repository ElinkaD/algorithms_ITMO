package storage

import (
	"encoding/json"
	"os"
)

type Manifest struct {
	Codec    string   `json:"codec"`
	Segments []string `json:"segments"`
}

func WriteManifest(path string, segments []string) error {
	b, err := json.MarshalIndent(Manifest{Codec: "pfordelta-bitpacking", Segments: segments}, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}

func ReadManifest(path string) (Manifest, error) {
	var m Manifest
	b, err := os.ReadFile(path)
	if err != nil {
		return m, err
	}
	err = json.Unmarshal(b, &m)
	return m, err
}
