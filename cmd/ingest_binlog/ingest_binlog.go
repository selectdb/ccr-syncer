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
	"flag"
	"fmt"
	"os"

	"github.com/selectdb/ccr_syncer/pkg/ccr/base"
	"github.com/selectdb/ccr_syncer/pkg/rpc"
	bestruct "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/backendservice"
	festruct_types "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/types"
	u "github.com/selectdb/ccr_syncer/pkg/utils"

	log "github.com/sirupsen/logrus"
)

// commit_seq flag default 0
var (
	commitSeq     int64
	txnId         int64
	action        string
	binlogVersion int64
	tabletId      int64
	backendId     int64
)

func init_flags() {
	flag.Int64Var(&commitSeq, "commit_seq", 0, "commit_seq")
	flag.Int64Var(&txnId, "txn_id", 0, "txn_id")
	flag.StringVar(&action, "action", "begin", "action")
	flag.Int64Var(&tabletId, "tablet_id", 0, "tablet id")
	flag.Int64Var(&backendId, "backend_id", 0, "backend id")
	flag.Int64Var(&binlogVersion, "binlog_version", 0, "binlog_version")
	flag.Parse()
}

func newCommitInfos() []*festruct_types.TTabletCommitInfo {
	commitInfo := festruct_types.TTabletCommitInfo{
		TabletId:  tabletId,
		BackendId: backendId,
	}
	commitInfos := make([]*festruct_types.TTabletCommitInfo, 0, 1)
	commitInfos = append(commitInfos, &commitInfo)
	return commitInfos
}

func test_get(t *base.Spec) {
	rpcFactory := rpc.NewRpcFactory()
	rpc, err := rpcFactory.NewFeRpc(t)
	if err != nil {
		panic(err)
	}
	resp, err := rpc.GetBinlog(t, commitSeq)
	if err != nil {
		panic(err)
	}
	fmt.Printf("resp: %v\n", resp)
}

func new_label(t *base.Spec, commitSeq int64) string {
	// label "ccr_sync_job:${db}:${table}:${commit_seq}"
	return fmt.Sprintf("ccr_sync_job:%s:%s:%d", t.Database, t.Table, commitSeq)
}

func test_begin(t *base.Spec) {
	rpcFactory := rpc.NewRpcFactory()
	rpc, err := rpcFactory.NewFeRpc(t)
	if err != nil {
		panic(err)
	}

	label := new_label(t, commitSeq)

	tableIds := make([]int64, 0, 1)
	tableIds = append(tableIds, t.TableId)
	resp, err := rpc.BeginTransaction(t, label, tableIds)
	if err != nil {
		panic(err)
	}
	fmt.Printf("resp: %v\n", resp)
	log.Infof("TxnId: %d, DbId: %d\n", resp.GetTxnId(), resp.GetDbId())
}

func test_commit(t *base.Spec) {
	rpcFactory := rpc.NewRpcFactory()
	rpc, err := rpcFactory.NewFeRpc(t)
	if err != nil {
		panic(err)
	}

	resp, err := rpc.CommitTransaction(t, txnId, newCommitInfos())
	if err != nil {
		panic(err)
	}
	fmt.Printf("resp: %v\n", resp)
}

// struct TIngestBinlogRequest {
//     1: required i64 txn_id;
//     2: required i64 remote_tablet_id;
//     3: required i64 binlog_version;
//     4: required string remote_host;
//     5: required string remote_port;
//     6: required i64 partition_id;
//     7: required i64 local_tablet_id;
//     8: required Types.TUniqueId load_id;
// }

func test_ingest_be() {
	backend := base.Backend{
		Id:       10028,
		Host:     "127.0.0.1",
		BePort:   9060,
		HttpPort: 8040,
		BrpcPort: 8060,
	}
	rpcFactory := rpc.NewRpcFactory()
	rpc, err := rpcFactory.NewBeRpc(&backend)
	if err != nil {
		panic(err)
	}

	loadId := festruct_types.NewTUniqueId()
	loadId.SetHi(-1)
	loadId.SetLo(-1)

	req := &bestruct.TIngestBinlogRequest{
		TxnId:          &txnId,
		RemoteTabletId: u.ThriftValueWrapper[int64](21014),
		BinlogVersion:  u.ThriftValueWrapper(binlogVersion),
		RemoteHost:     u.ThriftValueWrapper(backend.Host),
		RemotePort:     u.ThriftValueWrapper(backend.GetHttpPortStr()),
		PartitionId:    u.ThriftValueWrapper[int64](21011),
		LocalTabletId:  u.ThriftValueWrapper[int64](21019),
		LoadId:         loadId,
	}

	resp, err := rpc.IngestBinlog(req)
	if err != nil {
		panic(err)
	}
	fmt.Printf("ingest resp: %v\n", resp)
}

func test_ingrest_binlog(src *base.Spec, dest *base.Spec) {
	switch action {
	case "get":
		test_get(src)
	case "begin":
		test_begin(dest)
	case "commit":
		test_commit(dest)
	case "abort":
		panic("unknown abort action")
	case "ingest_be":
		test_ingest_be()
	default:
		panic("unknown action")
	}
}

func init_log() {
	// TODO(Drogon): config logrus
	// init logrus
	// Log as JSON instead of the default ASCII formatter.
	//   log.SetFormatter(&log.JSONFormatter{})
	log.SetFormatter(&log.TextFormatter{})

	// Output to stdout instead of the default stderr
	// Can be any io.Writer, see below for File example
	log.SetOutput(os.Stdout)

	// log the debug severity or above.
	log.SetLevel(log.TraceLevel)
}

func main() {
	init_flags()
	init_log()

	src := &base.Spec{
		Frontend: base.Frontend{
			Host:       "localhost",
			Port:       "9030",
			ThriftPort: "9020",
		},
		User:     "root",
		Password: "",
		Database: "ccr",
		Table:    "src_1",
	}

	dest := &base.Spec{
		Frontend: base.Frontend{
			Host:       "localhost",
			Port:       "9030",
			ThriftPort: "9020",
		},
		User:     "root",
		Password: "",
		Database: "ccr",
		Table:    "dest_1",
	}

	test_ingrest_binlog(src, dest)
}
