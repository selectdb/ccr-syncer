// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License
package main

import (
	"fmt"

	"github.com/selectdb/ccr_syncer/pkg/ccr/base"
	"github.com/selectdb/ccr_syncer/pkg/rpc"
)

func test_get_master_token(spec *base.Spec) {
	rpcFactory := rpc.NewRpcFactory()
	rpc, err := rpcFactory.NewFeRpc(spec)
	if err != nil {
		panic(err)
	}
	token, err := rpc.GetMasterToken(spec)
	if err != nil {
		panic(err)
	}
	fmt.Printf("token: %v\n", token)
}

func main() {
	// init_log()

	src := &base.Spec{
		Frontend: base.Frontend{
			Host:       "localhost",
			Port:       "9030",
			ThriftPort: "9020",
		},
		User:     "root",
		Password: "",
		Cluster:  "",
		Database: "ccr",
		Table:    "src_1",
	}

	test_get_master_token(src)
}
