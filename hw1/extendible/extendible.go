package extendible

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

var (
	ErrInvalidPath            = errors.New("table path must not be empty")
	ErrInvalidBucketCapacity  = errors.New("bucket capacity must be greater than 0")
	ErrBucketCapacityMismatch = errors.New("bucket capacity does not match existing table")
	ErrEmptyKey               = errors.New("key must not be empty")
	ErrKeyExists              = errors.New("key already exists")
	ErrKeyNotFound            = errors.New("key not found")
	ErrTableClosed            = errors.New("extendible hash table is closed")
	ErrMetadataMissing        = errors.New("metadata file is missing")
	ErrMetadataCorrupted      = errors.New("metadata file is corrupted")
	ErrFormatVersionMismatch  = errors.New("unsupported table format version")
	ErrBucketCorrupted        = errors.New("bucket file is corrupted")
)

const (
	metadataFileName = "metadata.json"
	formatVersion    = 1
)

type Record struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type Bucket struct {
	ID         int      `json:"id"`
	LocalDepth int      `json:"localDepth"`
	Records    []Record `json:"records"`
}

type metadata struct {
	Version        int   `json:"version"`
	GlobalDepth    int   `json:"globalDepth"`
	BucketCapacity int   `json:"bucketCapacity"`
	NextBucketID   int   `json:"nextBucketId"`
	Directory      []int `json:"directory"`
}

type Stats struct {
	GlobalDepth    int
	DirectorySize  int
	BucketsCount   int
	BucketCapacity int
	KeysCount      int
	MaxBucketLoad  int
	DiskBytes      int64
}

type Table struct {
	path   string
	meta   metadata
	closed bool
	mu     sync.Mutex
}

func Open(path string, bucketCapacity int) (*Table, error) {
	if strings.TrimSpace(path) == "" {
		return nil, ErrInvalidPath
	}
	if bucketCapacity <= 0 {
		return nil, ErrInvalidBucketCapacity
	}
	if err := os.MkdirAll(path, 0o755); err != nil {
		return nil, err
	}

	table := &Table{path: path}
	metaPath := table.metadataPath()

	if _, err := os.Stat(metaPath); err == nil {
		meta, err := readMetadata(metaPath)
		if err != nil {
			return nil, err
		}
		if meta.Version != formatVersion {
			return nil, fmt.Errorf("%w: got=%d expected=%d", ErrFormatVersionMismatch, meta.Version, formatVersion)
		}
		if meta.BucketCapacity != bucketCapacity {
			return nil, fmt.Errorf("%w: got=%d expected=%d", ErrBucketCapacityMismatch, bucketCapacity, meta.BucketCapacity)
		}
		if meta.GlobalDepth <= 0 || len(meta.Directory) == 0 || meta.NextBucketID <= 0 {
			return nil, ErrMetadataCorrupted
		}
		table.meta = meta
	} else if os.IsNotExist(err) {
		entries, readErr := os.ReadDir(path)
		if readErr != nil {
			return nil, readErr
		}
		if len(entries) != 0 {
			return nil, fmt.Errorf("%w: directory is not empty: %s", ErrMetadataMissing, path)
		}

		table.meta = metadata{
			Version:        formatVersion,
			GlobalDepth:    1,
			BucketCapacity: bucketCapacity,
			NextBucketID:   2,
			Directory:      []int{0, 1},
		}

		if err := saveBucket(table.bucketPath(0), Bucket{ID: 0, LocalDepth: 1, Records: make([]Record, 0)}); err != nil {
			return nil, err
		}
		if err := saveBucket(table.bucketPath(1), Bucket{ID: 1, LocalDepth: 1, Records: make([]Record, 0)}); err != nil {
			return nil, err
		}
		if err := table.saveMetadata(); err != nil {
			return nil, err
		}
	} else {
		return nil, err
	}

	return table, nil
}

func (t *Table) Insert(key string, value string) error {
	if err := validateKey(key); err != nil {
		return err
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return ErrTableClosed
	}

	for {
		index := t.directoryIndex(key)
		bucket, err := loadBucket(t.bucketPath(t.meta.Directory[index]))
		if err != nil {
			return err
		}

		if findRecordIndex(bucket.Records, key) >= 0 {
			return ErrKeyExists
		}

		if len(bucket.Records) < t.meta.BucketCapacity {
			bucket.Records = append(bucket.Records, Record{Key: key, Value: value})
			return saveBucket(t.bucketPath(bucket.ID), bucket)
		}

		if err := t.splitBucket(bucket); err != nil {
			return err
		}
	}
}

func (t *Table) Update(key string, value string) error {
	if err := validateKey(key); err != nil {
		return err
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return ErrTableClosed
	}

	index := t.directoryIndex(key)
	bucket, err := loadBucket(t.bucketPath(t.meta.Directory[index]))
	if err != nil {
		return err
	}

	recordIndex := findRecordIndex(bucket.Records, key)
	if recordIndex < 0 {
		return ErrKeyNotFound
	}

	bucket.Records[recordIndex].Value = value
	return saveBucket(t.bucketPath(bucket.ID), bucket)
}

func (t *Table) Get(key string) (string, bool, error) {
	if err := validateKey(key); err != nil {
		return "", false, err
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return "", false, ErrTableClosed
	}

	index := t.directoryIndex(key)
	bucket, err := loadBucket(t.bucketPath(t.meta.Directory[index]))
	if err != nil {
		return "", false, err
	}

	recordIndex := findRecordIndex(bucket.Records, key)
	if recordIndex < 0 {
		return "", false, nil
	}

	return bucket.Records[recordIndex].Value, true, nil
}

func (t *Table) Delete(key string) error {
	if err := validateKey(key); err != nil {
		return err
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return ErrTableClosed
	}

	index := t.directoryIndex(key)
	bucket, err := loadBucket(t.bucketPath(t.meta.Directory[index]))
	if err != nil {
		return err
	}

	recordIndex := findRecordIndex(bucket.Records, key)
	if recordIndex < 0 {
		return ErrKeyNotFound
	}

	bucket.Records = append(bucket.Records[:recordIndex], bucket.Records[recordIndex+1:]...)
	if err := saveBucket(t.bucketPath(bucket.ID), bucket); err != nil {
		return err
	}

	return t.tryMerge(bucket.ID)
}

func (t *Table) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.closed = true
	return nil
}

func (t *Table) Stats() (Stats, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return Stats{}, ErrTableClosed
	}

	unique := make(map[int]struct{})
	stats := Stats{
		GlobalDepth:    t.meta.GlobalDepth,
		DirectorySize:  len(t.meta.Directory),
		BucketCapacity: t.meta.BucketCapacity,
	}

	for _, bucketID := range t.meta.Directory {
		unique[bucketID] = struct{}{}
	}
	stats.BucketsCount = len(unique)

	for bucketID := range unique {
		bucket, err := loadBucket(t.bucketPath(bucketID))
		if err != nil {
			return Stats{}, err
		}

		load := len(bucket.Records)
		stats.KeysCount += load
		if load > stats.MaxBucketLoad {
			stats.MaxBucketLoad = load
		}

		info, err := os.Stat(t.bucketPath(bucketID))
		if err != nil {
			return Stats{}, err
		}
		stats.DiskBytes += info.Size()
	}

	metaInfo, err := os.Stat(t.metadataPath())
	if err != nil {
		return Stats{}, err
	}
	stats.DiskBytes += metaInfo.Size()

	return stats, nil
}

func validateKey(key string) error {
	if key == "" {
		return ErrEmptyKey
	}
	return nil
}

func findRecordIndex(records []Record, key string) int {
	for i := range records {
		if records[i].Key == key {
			return i
		}
	}
	return -1
}

func (t *Table) splitBucket(bucket Bucket) error {
	// если бакет упёрся в global depth, сначала удваиваем директорию
	if bucket.LocalDepth == t.meta.GlobalDepth {
		t.meta.Directory = append(t.meta.Directory, append([]int(nil), t.meta.Directory...)...)
		t.meta.GlobalDepth++
	}

	newBucket := Bucket{
		ID:         t.meta.NextBucketID,
		LocalDepth: bucket.LocalDepth + 1,
		Records:    make([]Record, 0),
	}
	t.meta.NextBucketID++

	bucket.LocalDepth++
	oldRecords := append([]Record(nil), bucket.Records...)
	bucket.Records = make([]Record, 0)

	// раскидываем указатели на старый и новый бакет
	splitBit := 1 << (bucket.LocalDepth - 1)
	for i := range t.meta.Directory {
		if t.meta.Directory[i] == bucket.ID && (i&splitBit) != 0 {
			t.meta.Directory[i] = newBucket.ID
		}
	}

	for _, record := range oldRecords {
		targetIndex := t.directoryIndex(record.Key)
		if t.meta.Directory[targetIndex] == newBucket.ID {
			newBucket.Records = append(newBucket.Records, record)
		} else {
			bucket.Records = append(bucket.Records, record)
		}
	}

	if err := saveBucket(t.bucketPath(bucket.ID), bucket); err != nil {
		return err
	}
	if err := saveBucket(t.bucketPath(newBucket.ID), newBucket); err != nil {
		return err
	}

	return t.saveMetadata()
}

func (t *Table) tryMerge(bucketID int) error {
	for {
		bucket, err := loadBucket(t.bucketPath(bucketID))
		if err != nil {
			return err
		}
		if bucket.LocalDepth <= 1 {
			break
		}

		index := -1
		for i, currentBucketID := range t.meta.Directory {
			if currentBucketID == bucketID {
				index = i
				break
			}
		}
		if index < 0 {
			break
		}

		// ищем "соседа" по последнему значимому биту
		buddyIndex := index ^ (1 << (bucket.LocalDepth - 1))
		if buddyIndex < 0 || buddyIndex >= len(t.meta.Directory) {
			break
		}
		buddyID := t.meta.Directory[buddyIndex]
		if buddyID == bucketID {
			break
		}

		buddy, err := loadBucket(t.bucketPath(buddyID))
		if err != nil {
			return err
		}
		if buddy.LocalDepth != bucket.LocalDepth {
			break
		}
		if len(bucket.Records)+len(buddy.Records) > t.meta.BucketCapacity {
			break
		}

		survivor := buddy
		victim := bucket
		if bucket.ID < buddy.ID {
			survivor = bucket
			victim = buddy
		}

		survivor.LocalDepth--
		survivor.Records = append(survivor.Records, victim.Records...)

		// перепривязываем директорию к оставшемуся бакету
		for i := range t.meta.Directory {
			if t.meta.Directory[i] == victim.ID || t.meta.Directory[i] == survivor.ID {
				prefixMask := (1 << survivor.LocalDepth) - 1
				baseIndex := i & prefixMask
				if baseIndex == (index & prefixMask) {
					t.meta.Directory[i] = survivor.ID
				}
			}
		}

		if err := saveBucket(t.bucketPath(survivor.ID), survivor); err != nil {
			return err
		}
		if err := os.Remove(t.bucketPath(victim.ID)); err != nil && !os.IsNotExist(err) {
			return err
		}

		bucketID = survivor.ID
		if err := t.shrinkDirectory(); err != nil {
			return err
		}
	}

	return t.saveMetadata()
}

func (t *Table) shrinkDirectory() error {
	for t.meta.GlobalDepth > 1 {
		half := len(t.meta.Directory) / 2
		canShrink := true

		for i := 0; i < half; i++ {
			if t.meta.Directory[i] != t.meta.Directory[i+half] {
				canShrink = false
				break
			}
		}

		if !canShrink {
			return nil
		}

		t.meta.Directory = append([]int(nil), t.meta.Directory[:half]...)
		t.meta.GlobalDepth--
	}

	return nil
}

func (t *Table) directoryIndex(key string) int {
	mask := (1 << t.meta.GlobalDepth) - 1
	return int(hashKey(key) & uint64(mask))
}

func hashKey(key string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	return h.Sum64()
}

func loadBucket(path string) (Bucket, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Bucket{}, err
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

func bucketFileName(id int) string {
	return fmt.Sprintf("bucket_%03d.json", id)
}

func (t *Table) bucketPath(id int) string {
	return filepath.Join(t.path, bucketFileName(id))
}

func (t *Table) metadataPath() string {
	return filepath.Join(t.path, metadataFileName)
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

func (t *Table) saveMetadata() error {
	data, err := json.Marshal(t.meta)
	if err != nil {
		return err
	}

	tmp := t.metadataPath() + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}

	return os.Rename(tmp, t.metadataPath())
}
