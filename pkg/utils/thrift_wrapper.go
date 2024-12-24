// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License
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
