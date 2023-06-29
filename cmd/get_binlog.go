package main

import (
	"encoding/json"
	"flag"

	"github.com/selectdb/ccr_syncer/ccr/base"
	"github.com/selectdb/ccr_syncer/ccr/record"
	"github.com/selectdb/ccr_syncer/rpc"
	festruct "github.com/selectdb/ccr_syncer/rpc/kitex_gen/frontendservice"
	u "github.com/selectdb/ccr_syncer/utils"
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

func test_get_binlog(spec *base.Spec) {
	rpc, err := rpc.NewThriftRpc(spec)
	if err != nil {
		panic(err)
	}
	t_spec := *spec
	t_spec.Table = ""
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
	log.Infof("first resp binlog data: %v", jsonData)
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
				log.Fatalf("table_id not int64: %v", info["table_id"])
			} else {
				log.Infof("table_id: %v", tableId)
			}
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

	test_get_binlog(src)
}
