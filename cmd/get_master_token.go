package main

import (
	"fmt"

	"github.com/selectdb/ccr_syncer/ccr/base"
	"github.com/selectdb/ccr_syncer/rpc"
)

func test_get_master_token(spec *base.Spec) {
	rpc, err := rpc.NewThriftRpc(spec)
	if err != nil {
		panic(err)
	}
	token, err := rpc.GetMasterToken(spec)
	if err != nil {
		panic(err)
	}
	fmt.Printf("token: %v\n", token)
}

func main() {
	// init_log()

	src := &base.Spec{
		Host:       "localhost",
		Port:       "9030",
		ThriftPort: "9020",
		User:       "root",
		Password:   "",
		Cluster:    "",
		Database:   "ccr",
		Table:      "src_1",
	}

	test_get_master_token(src)
}
