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
package ccr

import (
	"github.com/selectdb/ccr_syncer/pkg/ccr/base"
	"github.com/selectdb/ccr_syncer/pkg/rpc"
)

type Factory struct {
	rpc.IRpcFactory
	MetaerFactory
	base.SpecerFactory
	ThriftMetaFactory
}

func NewFactory(rpcFactory rpc.IRpcFactory, metaFactory MetaerFactory, ISpecFactory base.SpecerFactory, thriftMetaFactory ThriftMetaFactory) *Factory {
	return &Factory{
		IRpcFactory:       rpcFactory,
		MetaerFactory:     metaFactory,
		SpecerFactory:     ISpecFactory,
		ThriftMetaFactory: thriftMetaFactory,
	}
}
