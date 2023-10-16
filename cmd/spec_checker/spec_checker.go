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

func checkTableEnableBinlog(table string) {
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

	if dbEnableBinlog, err := src.IsTableEnableBinlog(); err != nil {
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
	checkTableEnableBinlog("src_1")
	checkTableEnableBinlog("tbl_day")
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
