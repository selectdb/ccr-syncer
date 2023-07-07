package main

import (
	"flag"

	"github.com/selectdb/ccr_syncer/ccr/base"
	"github.com/selectdb/ccr_syncer/rpc"
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

func test_get_lag(spec *base.Spec) {
	rpc, err := rpc.NewThriftRpc(spec)
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
		Host:       "localhost",
		Port:       "9030",
		ThriftPort: "9020",
		User:       "root",
		Password:   "",
		Database:   "ccr",
		Table:      "",
	}

	test_get_lag(src)
}
