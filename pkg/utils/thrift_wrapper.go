package utils

import (
	"context"

	"github.com/apache/thrift/lib/go/thrift"
)

type WrapperType interface {
	~int64 | ~string | ~bool
}

func ThriftValueWrapper[T WrapperType](value T) *T {
	return &value
}

func ThriftToJsonStr(obj thrift.TStruct) (string, error) {
	transport := thrift.NewTMemoryBuffer()
	protocol := thrift.NewTJSONProtocolFactory().GetProtocol(transport)
	ts := &thrift.TSerializer{Transport: transport, Protocol: protocol}
	if jsonBytes, err := ts.Write(context.Background(), obj); err != nil {
		return "", nil
	} else {
		return string(jsonBytes), nil
	}
}
