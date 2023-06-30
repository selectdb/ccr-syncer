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
	if dbId, err := m.GetDbId(); err != nil {
		panic(err)
	} else {
		log.Infof("found db: %s, dbId: %d", m.Database, dbId)
	}

	tableId, err := m.GetTableId(m.Table)
	if err != nil {
		panic(err)
	} else {
		log.Infof("found table: %s, tableId: %d", m.Table, tableId)
	}

	var partitionId int64
	if partitions, err := m.GetPartitions(m.Table); err != nil {
		panic(err)
	} else {
		if len(partitions) == 0 {
			panic("no partitions")
		}
		for k := range partitions {
			partitionId = k
		}
		log.Infof("found partitions: %v", partitions)
	}

	if backends, err := m.GetBackends(); err != nil {
		panic(err)
	} else {
		log.Infof("found backends: %v", backends)
	}

	if err := m.UpdateBackends(); err != nil {
		panic(err)
	} else {
		log.Infof("update backends success")
	}
	log.Infof("meta: %#v", m)

	if indexes, err := m.GetIndexes(m.Table, partitionId); err != nil {
		panic(err)
	} else {
		log.Infof("partitionid: %d, found indexes: %v", partitionId, indexes)
	}

	if tablets, err := m.GetTabletList(m.Table, partitionId); err != nil {
		panic(err)
	} else {
		log.Infof("partitionid: %d, found tablets: %v", partitionId, tablets)
		for _, tablet := range tablets {
			log.Infof("tablet: %d, replica len: %d", tablet.Id, tablet.ReplicaMetas.Len())
		}
	}

	if replicas, err := m.GetReplicas(m.Table, partitionId); err != nil {
		panic(err)
	} else {
		log.Infof("partitionid: %d, found replicas: %v, len %d", partitionId, replicas, replicas.Len())
	}

	if tableName, err := m.GetTableNameById(tableId); err != nil {
		panic(err)
	} else {
		log.Infof("tableId: %d, tableName: %s", tableId, tableName)
	}

	if tables, err := m.GetTables(); err != nil {
		panic(err)
	} else {
		for _, table := range tables {
			log.Infof("table: %v", table)
		}
	}
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
