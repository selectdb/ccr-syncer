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
	"github.com/selectdb/ccr_syncer/pkg/ccr/base"
	"github.com/selectdb/ccr_syncer/pkg/utils"
	log "github.com/sirupsen/logrus"
)

func checkDBEnableBinlog(db string) {
	src := &base.Spec{
		Frontend: base.Frontend{
			Host:       "localhost",
			Port:       "9030",
			ThriftPort: "9020",
		},
		User:     "root",
		Password: "",
		Database: db,
		Table:    "enable_binlog",
	}

	if dbEnableBinlog, err := src.IsDatabaseEnableBinlog(); err != nil {
		panic(err)
	} else {
		log.Infof("db: %v enable binlog: %v", db, dbEnableBinlog)
	}
}

func CheckTableProperty(table string) {
	src := &base.Spec{
		Frontend: base.Frontend{
			Host:       "localhost",
			Port:       "9030",
			ThriftPort: "9020",
		},
		User:     "root",
		Password: "",
		Database: "ccr",
		Table:    table,
	}

	if dbEnableBinlog, err := src.CheckTablePropertyValid(); err != nil {
		panic(err)
	} else {
		log.Infof("table: ccr.%v enable binlog: %v", table, dbEnableBinlog)
	}
}

func testDBEnableBinlog() {
	checkDBEnableBinlog("ccr")
	checkDBEnableBinlog("regression_test")
}

func testTableEnableBinlog() {
	CheckTableProperty("src_1")
	CheckTableProperty("tbl_day")
}

func testGetAllTables() {
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

	tables, err := src.GetAllTables()
	if err != nil {
		panic(err)
	}
	log.Infof("tables: %v", tables)
}

func main() {
	utils.InitLog()

	testGetAllTables()
}
