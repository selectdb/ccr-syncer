package main

import (
	"flag"

	log "github.com/sirupsen/logrus"

	"github.com/selectdb/ccr_syncer/ccr"
	"github.com/selectdb/ccr_syncer/ccr/base"
	"github.com/selectdb/ccr_syncer/utils"
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
	flag.StringVar(&tableName, "table", "enable_binlog", "table name")
	flag.Parse()

	utils.InitLog()
}

func test_init_meta(m ccr.IMeta, spec *base.Spec) {
	if dbId, err := m.GetDbId(); err != nil {
		panic(err)
	} else {
		log.Infof("found db: %s, dbId: %d", spec.Database, dbId)
	}

	tableId, err := m.GetTableId(spec.Table)
	if err != nil {
		panic(err)
	} else {
		log.Infof("found table: %s, tableId: %d", spec.Table, tableId)
	}

	var partitionId int64
	if partitions, err := m.GetPartitionIdMap(tableId); err != nil {
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

	if indexes, err := m.GetIndexIdMap(tableId, partitionId); err != nil {
		panic(err)
	} else {
		log.Infof("partitionid: %d, found indexes: %v", partitionId, indexes)
	}

	if replicas, err := m.GetReplicas(tableId, partitionId); err != nil {
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

	test_init_meta(meta, src)
}
