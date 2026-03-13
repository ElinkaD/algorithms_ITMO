package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const (
	metadataFileName = "metadata.json"
	formatVersion    = 1
)

type metadata struct {
	Version      int `json:"version"`
	BucketsCount int `json:"bucketsCount"`
}

type HashTable struct {
	path         string
	bucketsCount int
	closed       bool

	// - параллельные операции чтения (Get)
	// - эксклюзивный доступ для операций записи
	mu sync.RWMutex
}

func Open(path string, bucketsCount int) (*HashTable, error) {
	if strings.TrimSpace(path) == "" {
		return nil, ErrInvalidPath
	}
	if bucketsCount <= 0 {
		return nil, ErrInvalidBucketsCount
	}

	if err := os.MkdirAll(path, 0o755); err != nil {
		return nil, err
	}

	ht := &HashTable{path: path, bucketsCount: bucketsCount}
	metaPath := ht.metadataPath()

	if _, err := os.Stat(metaPath); err == nil {
		meta, err := readMetadata(metaPath)
		if err != nil {
			return nil, err
		}
		if meta.Version != formatVersion {
			return nil, fmt.Errorf("%w: got=%d expected=%d", ErrFormatVersionMismatch, meta.Version, formatVersion)
		}
		if meta.BucketsCount <= 0 {
			return nil, fmt.Errorf("%w: bucketsCount=%d", ErrMetadataCorrupted, meta.BucketsCount)
		}
		if meta.BucketsCount != bucketsCount {
			return nil, fmt.Errorf("%w: got=%d expected=%d", ErrBucketsCountMismatch, bucketsCount, meta.BucketsCount)
		}
	} else if os.IsNotExist(err) {
		dirEntries, readErr := os.ReadDir(path)
		if readErr != nil {
			return nil, readErr
		}
		if len(dirEntries) != 0 {
			return nil, fmt.Errorf("%w: directory is not empty: %s", ErrMetadataMissing, path)
		}

		if err := ht.writeMetadata(); err != nil {
			return nil, err
		}
	} else {
		return nil, err
	}

	for i := 0; i < ht.bucketsCount; i++ {
		if err := ht.ensureBucketFile(i); err != nil {
			return nil, err
		}
	}

	return ht, nil
}

func (ht *HashTable) Insert(key string, value string) error {
	if err := validateKey(key); err != nil {
		return err
	}

	ht.mu.Lock()
	defer ht.mu.Unlock()

	if err := ht.ensureOpen(); err != nil {
		return err
	}

	index := ht.bucketIndex(key)
	bucket, path, err := ht.readBucket(index)
	if err != nil {
		return err
	}

	if findRecordIndex(bucket, key) >= 0 {
		return ErrKeyExists
	}

	bucket.Records = append(bucket.Records, Record{Key: key, Value: value})
	return saveBucket(path, bucket)
}

func (ht *HashTable) Update(key string, value string) error {
	if err := validateKey(key); err != nil {
		return err
	}

	ht.mu.Lock()
	defer ht.mu.Unlock()

	if err := ht.ensureOpen(); err != nil {
		return err
	}

	index := ht.bucketIndex(key)
	bucket, path, err := ht.readBucket(index)
	if err != nil {
		return err
	}

	recordIndex := findRecordIndex(bucket, key)
	if recordIndex < 0 {
		return ErrKeyNotFound
	}

	bucket.Records[recordIndex].Value = value
	return saveBucket(path, bucket)
}

func (ht *HashTable) Get(key string) (string, bool, error) {
	if err := validateKey(key); err != nil {
		return "", false, err
	}

	ht.mu.RLock()
	defer ht.mu.RUnlock()

	if err := ht.ensureOpen(); err != nil {
		return "", false, err
	}

	index := ht.bucketIndex(key)
	bucket, _, err := ht.readBucket(index)
	if err != nil {
		return "", false, err
	}

	recordIndex := findRecordIndex(bucket, key)
	if recordIndex < 0 {
		return "", false, nil
	}

	return bucket.Records[recordIndex].Value, true, nil
}

func (ht *HashTable) Delete(key string) error {
	if err := validateKey(key); err != nil {
		return err
	}

	ht.mu.Lock()
	defer ht.mu.Unlock()

	if err := ht.ensureOpen(); err != nil {
		return err
	}

	index := ht.bucketIndex(key)
	bucket, path, err := ht.readBucket(index)
	if err != nil {
		return err
	}

	recordIndex := findRecordIndex(bucket, key)
	if recordIndex < 0 {
		return ErrKeyNotFound
	}

	bucket.Records = append(bucket.Records[:recordIndex], bucket.Records[recordIndex+1:]...)
	return saveBucket(path, bucket)
}

func (ht *HashTable) Close() error {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	ht.closed = true
	return nil
}

func (ht *HashTable) ensureOpen() error {
	if ht.closed {
		return ErrTableClosed
	}
	return nil
}

func validateKey(key string) error {
	if key == "" {
		return ErrEmptyKey
	}
	return nil
}

//ищет запись с данным ключом внутри бакета
func findRecordIndex(bucket Bucket, key string) int {
	for i := range bucket.Records {
		if bucket.Records[i].Key == key {
			return i
		}
	}
	return -1
}

func (ht *HashTable) bucketIndex(key string) int {
	return int(hashKey(key) % uint64(ht.bucketsCount))
}

func bucketFileName(index int) string {
	return fmt.Sprintf("bucket_%03d.dat", index)
}

func (ht *HashTable) bucketPath(index int) string {
	return filepath.Join(ht.path, bucketFileName(index))
}

func (ht *HashTable) readBucket(index int) (Bucket, string, error) {
	path := ht.bucketPath(index)
	bucket, err := loadBucket(path)
	if err != nil {
		return Bucket{}, "", err
	}
	return bucket, path, nil
}

func (ht *HashTable) ensureBucketFile(index int) error {
	path := ht.bucketPath(index)
	if _, err := os.Stat(path); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}

	return saveBucket(path, Bucket{Records: make([]Record, 0)})
}


func (ht *HashTable) metadataPath() string {
	return filepath.Join(ht.path, metadataFileName)
}

func readMetadata(path string) (metadata, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return metadata{}, err
	}

	var meta metadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return metadata{}, fmt.Errorf("%w: %s: %v", ErrMetadataCorrupted, path, err)
	}

	return meta, nil
}

func (ht *HashTable) writeMetadata() error {
	meta := metadata{Version: formatVersion, BucketsCount: ht.bucketsCount}
	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}

	metaPath := ht.metadataPath()
	tmp := metaPath + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}

	return os.Rename(tmp, metaPath)
}
