package main

import (
	log "github.com/sirupsen/logrus"

	"github.com/selectdb/ccr_syncer/ccr"
	"github.com/selectdb/ccr_syncer/ccr/base"
	"github.com/selectdb/ccr_syncer/utils"
)

func init() {
	utils.InitLog()
}

func test_init_meta(m *ccr.Meta) {
	if dbId, err := m.GetDbId(m.Database); err != nil {
		panic(err)
	} else {
		log.Infof("found db: %s, dbId: %d\n", m.Database, dbId)
	}

	if tableId, err := m.GetTableId(m.Table); err != nil {
		panic(err)
	} else {
		log.Infof("found table: %s, tableId: %d\n", m.Table, tableId)
	}

	if partitionIds, err := m.GetPartitions(m.Table); err != nil {
		panic(err)
	} else {
		log.Infof("found partitions: %v\n", partitionIds)
	}

	if backends, err := m.GetBackends(); err != nil {
		panic(err)
	} else {
		log.Infof("found backends: %v\n", backends)
	}

	if err := m.UpdateBackends(); err != nil {
		panic(err)
	} else {
		log.Infof("update backends success\n")
	}
	log.Infof("meta: %#v", m)
}

func main() {
	src := &base.Spec{
		Host:       "localhost",
		Port:       "9030",
		ThriftPort: "9020",
		User:       "root",
		Password:   "",
		Database:   "ccr",
		Table:      "enable_binlog",
	}

	meta := ccr.NewMeta(src)

	test_init_meta(meta)
}
