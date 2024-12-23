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

	log "github.com/sirupsen/logrus"

	"github.com/selectdb/ccr_syncer/pkg/ccr"
	"github.com/selectdb/ccr_syncer/pkg/ccr/base"
	"github.com/selectdb/ccr_syncer/pkg/rpc"
	"github.com/selectdb/ccr_syncer/pkg/utils"
)

var (
	host       string
	port       string
	thriftPort string
	user       string
	password   string
	dbName     string
	tableName  string
)

func init() {
	flag.StringVar(&host, "host", "localhost", "host")
	flag.StringVar(&port, "port", "9030", "port")
	flag.StringVar(&thriftPort, "thrift_port", "9020", "thrift port")
	flag.StringVar(&user, "user", "root", "user")
	flag.StringVar(&password, "password", "", "password")
	flag.StringVar(&dbName, "db", "ccr", "database name")
	flag.StringVar(&tableName, "table", "src_1", "table name")
	flag.Parse()

	utils.InitLog()
}

func test_get_table_meta(m ccr.Metaer, spec *base.Spec) {
	if dbId, err := m.GetDbId(); err != nil {
		panic(err)
	} else {
		spec.DbId = dbId
		log.Infof("found db: %s, dbId: %d", spec.Database, dbId)
	}

	if tableId, err := m.GetTableId(spec.Table); err != nil {
		panic(err)
	} else {
		spec.TableId = tableId
		log.Infof("found table: %s, tableId: %d", spec.Table, tableId)
	}

	rpcFactory := rpc.NewRpcFactory()
	feRpc, err := rpcFactory.NewFeRpc(spec)
	if err != nil {
		panic(err)
	}

	tableIds := make([]int64, 0)
	tableIds = append(tableIds, spec.TableId)
	result, err := feRpc.GetTableMeta(spec, tableIds)
	if err != nil {
		panic(err)
	}
	// toJson
	s, err := json.Marshal(&result)
	if err != nil {
		panic(err)
	}
	log.Infof("found db meta: %s", s)

	thriftMeta, err := ccr.NewThriftMeta(spec, rpcFactory, tableIds)
	if err != nil {
		panic(err)
	}
	log.Infof("found thrift meta: %+v", thriftMeta)
}

func test_get_db_meta(m ccr.Metaer, spec *base.Spec) {
	if dbId, err := m.GetDbId(); err != nil {
		panic(err)
	} else {
		spec.DbId = dbId
		log.Infof("found db: %s, dbId: %d", spec.Database, dbId)
	}

	rpcFactory := rpc.NewRpcFactory()
	feRpc, err := rpcFactory.NewFeRpc(spec)
	if err != nil {
		panic(err)
	}

	result, err := feRpc.GetDbMeta(spec)
	if err != nil {
		panic(err)
	}
	// toJson
	s, err := json.Marshal(result)
	if err != nil {
		panic(err)
	}
	log.Infof("found db meta: %s", s)
}

func test_get_backends(m ccr.Metaer, spec *base.Spec) {
	rpcFactory := rpc.NewRpcFactory()
	feRpc, err := rpcFactory.NewFeRpc(spec)
	if err != nil {
		panic(err)
	}

	result, err := feRpc.GetBackends(spec)
	if err != nil {
		panic(err)
	}
	// toJson
	s, err := json.Marshal(&result)
	if err != nil {
		panic(err)
	}
	log.Infof("found backends: %s", s)
}

func main() {
	src := &base.Spec{
		Frontend: base.Frontend{
			Host:       host,
			Port:       port,
			ThriftPort: thriftPort,
		},
		User:     user,
		Password: password,
		Database: dbName,
		Table:    tableName,
	}

	metaFactory := ccr.NewMetaFactory()
	meta := metaFactory.NewMeta(src)

	if tableName != "" {
		test_get_table_meta(meta, src)
	} else {
		test_get_db_meta(meta, src)
	}
	test_get_backends(meta, src)
}
