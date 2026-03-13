package storage

import "hash/fnv"

// FNV-1a: для каждого байта строки выполняется
// XOR с текущим хэшем и умножение на специальное простое число.
func hashKey(key string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	return h.Sum64()
}
