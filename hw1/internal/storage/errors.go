package storage

import "errors"

var (
	ErrInvalidPath           = errors.New("table path must not be empty")
	ErrInvalidBucketsCount   = errors.New("bucketsCount must be greater than 0")
	ErrEmptyKey              = errors.New("key must not be empty")
	ErrKeyExists             = errors.New("key already exists")
	ErrKeyNotFound           = errors.New("key not found")
	ErrTableClosed           = errors.New("hash table is closed")
	ErrMetadataMissing       = errors.New("metadata file is missing")
	ErrMetadataCorrupted     = errors.New("metadata file is corrupted")
	ErrFormatVersionMismatch = errors.New("unsupported table format version")
	ErrBucketsCountMismatch  = errors.New("bucketsCount does not match existing table")
	ErrBucketCorrupted       = errors.New("bucket file is corrupted")
)
