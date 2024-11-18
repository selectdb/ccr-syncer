package utils

// CopyMap returns a new map with the same key-value pairs as the input map.
// The input map must have keys and values of comparable types.
// but key and value is not deep copy
func CopyMap[K, V comparable](m map[K]V) map[K]V {
	result := make(map[K]V)
	for k, v := range m {
		result[k] = v
	}
	return result
}

// MergeMap returns a new map with all key-value pairs from both input maps.
func MergeMap[K comparable, V any](m1, m2 map[K]V) map[K]V {
	if m1 == nil {
		m1 = make(map[K]V, len(m2))
	}
	for k, v := range m2 {
		m1[k] = v
	}
	return m1
}
