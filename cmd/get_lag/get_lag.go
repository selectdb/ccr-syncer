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
	"flag"

	"github.com/selectdb/ccr_syncer/pkg/ccr/base"
	"github.com/selectdb/ccr_syncer/pkg/rpc"
	u "github.com/selectdb/ccr_syncer/pkg/utils"
	log "github.com/sirupsen/logrus"
)

// commit_seq flag default 0
var (
	commitSeq int64
)

func init_flags() {
	flag.Int64Var(&commitSeq, "commit_seq", 0, "commit_seq")
	flag.Parse()
}

func init() {
	init_flags()
	u.InitLog()
}

func test_get_lag(spec *base.Spec) {
	rpcFactory := rpc.NewRpcFactory()
	rpc, err := rpcFactory.NewFeRpc(spec)
	if err != nil {
		panic(err)
	}
	resp, err := rpc.GetBinlogLag(spec, commitSeq)
	// resp, err := rpc.GetBinlog(spec, commitSeq)
	if err != nil {
		panic(err)
	}
	log.Infof("resp: %v", resp)
	log.Infof("lag: %d", resp.GetLag())
}

func main() {
	src := &base.Spec{
		Frontend: base.Frontend{
			Host:       "localhost",
			Port:       "9030",
			ThriftPort: "9020",
		},
		User:     "root",
		Password: "",
		Database: "ccr",
		Table:    "",
	}

	test_get_lag(src)
}
