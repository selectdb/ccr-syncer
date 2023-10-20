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
