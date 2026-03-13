package storage

import (
	"encoding/json"
	"fmt"
	"os"
)

type Record struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type Bucket struct {
	Records []Record `json:"records"`
}

func loadBucket(path string) (Bucket, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Bucket{}, err
	}

	if len(data) == 0 {
		return Bucket{Records: make([]Record, 0)}, nil
	}

	var bucket Bucket
	if err := json.Unmarshal(data, &bucket); err != nil {
		return Bucket{}, fmt.Errorf("%w: %s: %v", ErrBucketCorrupted, path, err)
	}
	if bucket.Records == nil {
		bucket.Records = make([]Record, 0)
	}

	return bucket, nil
}

func saveBucket(path string, bucket Bucket) error {
	if bucket.Records == nil {
		bucket.Records = make([]Record, 0)
	}

	data, err := json.Marshal(bucket)
	if err != nil {
		return err
	}

	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}

	return os.Rename(tmp, path)
}
