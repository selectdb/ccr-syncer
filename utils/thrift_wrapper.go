package utils

type WrapperType interface {
	~int64 | ~string
}

func ThriftValueWrapper[T WrapperType](value T) *T {
	return &value
}
