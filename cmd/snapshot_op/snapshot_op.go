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

	"github.com/selectdb/ccr_syncer/pkg/ccr"
	"github.com/selectdb/ccr_syncer/pkg/ccr/base"
	"github.com/selectdb/ccr_syncer/pkg/rpc"
	"github.com/selectdb/ccr_syncer/pkg/utils"

	log "github.com/sirupsen/logrus"

	tstatus "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/status"
)

// commit_seq flag default 0
var (
	action    string
	labelName string
	token     string
)

func init_flags() {
	flag.StringVar(&action, "action", "get", "action")
	flag.StringVar(&labelName, "label", "ccr_snapshot_20230605", "label")
	flag.StringVar(&token, "token", "5ff161c3-2c08-4079-b108-26c8850b6598", "token")
	flag.Parse()
}

func test_get_snapshot(spec *base.Spec) {
	rpcFactory := rpc.NewRpcFactory()
	rpc, err := rpcFactory.NewFeRpc(spec)
	if err != nil {
		panic(err)
	}

	snapshotResp, err := rpc.GetSnapshot(spec, labelName)
	if err != nil {
		panic(err)
	}

	if snapshotResp.Status.GetStatusCode() != tstatus.TStatusCode_OK {
		log.Panicf("get snapshot failed, status: %v", snapshotResp.Status)
	}

	jobInfo := snapshotResp.GetJobInfo()
	log.Infof("resp: %v\n", snapshotResp)
	log.Infof("job: %s\n", string(jobInfo))

	var jobInfoMap map[string]interface{}
	// json umarshal jobInfo
	err = json.Unmarshal(jobInfo, &jobInfoMap)
	if err != nil {
		panic(err)
	}
	log.Infof("jobInfo: %v\n", jobInfoMap)

	if tableCommitSeqMap, err := ccr.ExtractTableCommitSeqMap(jobInfo); err != nil {
		panic(err)
	} else {
		log.Infof("tableCommitSeqMap: %v\n", tableCommitSeqMap)
	}
}

func test_restore_snapshot(src *base.Spec, dest *base.Spec) {
	// Get snapshot from src
	rpcFactory := rpc.NewRpcFactory()
	srcRpc, err := rpcFactory.NewFeRpc(src)
	if err != nil {
		panic(err)
	}
	snapshotResp, err := srcRpc.GetSnapshot(src, labelName)
	if err != nil {
		panic(err)
	}
	// log.Infof("resp: %v\n", snapshotResp)
	log.Infof("job: %s\n", string(snapshotResp.GetJobInfo()))

	var jobInfo map[string]interface{}
	// json umarshal jobInfo
	err = json.Unmarshal(snapshotResp.GetJobInfo(), &jobInfo)
	if err != nil {
		panic(err)
	}
	log.Infof("jobInfo: %v\n", jobInfo)

	extraInfo := genExtraInfo(src, token)
	log.Infof("extraInfo: %v\n", extraInfo)

	jobInfo["extra_info"] = extraInfo

	// marshal jobInfo
	jobInfoBytes, err := json.Marshal(jobInfo)
	if err != nil {
		panic(err)
	}
	log.Infof("jobInfoBytes: %s\n", string(jobInfoBytes))
	snapshotResp.SetJobInfo(jobInfoBytes)

	// Restore snapshot to det
	destRpc, err := rpcFactory.NewFeRpc(dest)
	if err != nil {
		panic(err)
	}
	restoreResp, err := destRpc.RestoreSnapshot(dest, nil, labelName, snapshotResp, false, false)
	if err != nil {
		panic(err)
	}
	log.Infof("resp: %v\n", restoreResp)
}

func test_snapshot_op(src *base.Spec, dest *base.Spec) {
	switch action {
	case "get":
		test_get_snapshot(src)
	case "restore":
		test_restore_snapshot(src, dest)
	default:
		panic("unknown action")
	}
}

func init() {
	init_flags()
	utils.InitLog()
}

type JobInfo map[string]interface{}

func genExtraInfo(src *base.Spec, token string) *base.ExtraInfo {
	metaFactory := ccr.NewMetaFactory()
	meta := metaFactory.NewMeta(src)
	backends, err := meta.GetBackends()
	if err != nil {
		panic(err)
	} else {
		log.Infof("found backends: %v\n", backends)
	}

	beNetworkMap := make(map[int64]base.NetworkAddr)
	for _, backend := range backends {
		log.Infof("backend: %v\n", backend)
		addr := base.NetworkAddr{
			Ip:   backend.Host,
			Port: backend.HttpPort,
		}
		beNetworkMap[backend.Id] = addr
	}

	return &base.ExtraInfo{
		BeNetworkMap: beNetworkMap,
		Token:        token,
	}
}

func main() {
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
			Port:       "29030",
			ThriftPort: "29020",
		},
		User:     "root",
		Password: "",
		Database: "ccr",
		Table:    "dest_1",
	}

	test_snapshot_op(src, dest)
}
