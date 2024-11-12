package utils

func FirstOr[T any](array []T, def T) T {
	if len(array) == 0 {
		return def
	}
	return array[0]
}
