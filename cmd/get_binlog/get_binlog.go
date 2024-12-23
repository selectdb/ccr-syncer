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
	"encoding/json"
	"flag"

	"github.com/selectdb/ccr_syncer/pkg/ccr/base"
	"github.com/selectdb/ccr_syncer/pkg/ccr/record"
	"github.com/selectdb/ccr_syncer/pkg/rpc"
	festruct "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/frontendservice"
	u "github.com/selectdb/ccr_syncer/pkg/utils"
	log "github.com/sirupsen/logrus"
)

// commit_seq flag default 0
var (
	commitSeq int64
	db        string
	table     string
)

func init_flags() {
	flag.Int64Var(&commitSeq, "commit_seq", 0, "commit_seq")
	flag.StringVar(&db, "db", "ccr", "db")
	flag.StringVar(&table, "table", "src_1", "table")

	flag.Parse()
}

func init() {
	init_flags()
	u.InitLog()
}

func test_get_binlog(spec *base.Spec) {
	rpcFactory := rpc.NewRpcFactory()
	rpc, err := rpcFactory.NewFeRpc(spec)
	if err != nil {
		panic(err)
	}
	t_spec := *spec
	resp, err := rpc.GetBinlog(&t_spec, commitSeq)
	// resp, err := rpc.GetBinlog(spec, commitSeq)
	if err != nil {
		panic(err)
	}
	log.Infof("resp: %v", resp)
	for _, binlog := range resp.GetBinlogs() {
		log.Infof("binlog: %v", binlog)
	}
	log.Infof("resp binlogs: %v", resp.GetBinlogs())

	binlog := resp.GetBinlogs()[0]
	jsonData := binlog.GetData()
	log.Infof("first resp commit seq: %d, binlog data: %v", binlog.GetCommitSeq(), jsonData)
	switch binlog.GetType() {
	case festruct.TBinlogType_UPSERT:
		if upsert, err := record.NewUpsertFromJson(jsonData); err != nil {
			panic(err)
		} else {
			log.Infof("upsert: %s", upsert)
		}
	case festruct.TBinlogType_ADD_PARTITION:
		var info map[string]interface{}
		if err := json.Unmarshal([]byte(jsonData), &info); err != nil {
			panic(err)
		} else {
			log.Infof("sql: %s, type: %T", info["sql"], info["sql"])
			log.Infof("tableId: %v", info["tableId"])
			if tableId, ok := info["tableId"].(int64); !ok {
				log.Fatalf("table_id not int64: %v, type: %T", info["tableId"], info["tableId"])
			} else {
				log.Infof("table_id: %v", tableId)
			}
		}
	case festruct.TBinlogType_CREATE_TABLE:
		if createTableRecord, err := record.NewCreateTableFromJson(jsonData); err != nil {
			panic(err)
		} else {
			log.Infof("createTableRecord: %s", createTableRecord)
		}
	case festruct.TBinlogType_ALTER_JOB:
		if alterJobRecord, err := record.NewAlterJobV2FromJson(jsonData); err != nil {
			panic(err)
		} else {
			log.Infof("alterJobRecord: %s", alterJobRecord)
		}
	case festruct.TBinlogType_DUMMY:
		s, _ := json.Marshal(&binlog)
		log.Infof("dummy: %s", s)
	}
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
		Database: db,
		Table:    table,
	}

	test_get_binlog(src)
}
