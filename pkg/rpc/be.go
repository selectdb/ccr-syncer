// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License
package rpc

import (
	"context"
	"time"

	"github.com/cloudwego/kitex/client/callopt"
	"github.com/selectdb/ccr_syncer/pkg/ccr/base"
	"github.com/selectdb/ccr_syncer/pkg/xerror"
	"github.com/selectdb/ccr_syncer/pkg/xmetrics"

	bestruct "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/backendservice"
	beservice "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/backendservice/backendservice"

	log "github.com/sirupsen/logrus"
)

type BeRpcOption func() callopt.Option

type IBeRpc interface {
	IngestBinlog(*bestruct.TIngestBinlogRequest, ...BeRpcOption) (*bestruct.TIngestBinlogResult_, error)
}

type BeRpc struct {
	backend *base.Backend
	client  beservice.Client
}

func (beRpc *BeRpc) IngestBinlog(req *bestruct.TIngestBinlogRequest, beOptions ...BeRpcOption) (*bestruct.TIngestBinlogResult_, error) {
	log.Tracef("IngestBinlog req: %+v, txnId: %d, be: %v", req, req.GetTxnId(), beRpc.backend)

	defer xmetrics.RecordBeRpc("IngestBinlog", beRpc.backend.Host, beRpc.backend.BePort)()

	var options []callopt.Option
	for _, opt := range beOptions {
		options = append(options, opt())
	}
	client := beRpc.client
	if result, err := client.IngestBinlog(context.Background(), req, options...); err != nil {
		return nil, xerror.Wrapf(err, xerror.Normal,
			"IngestBinlog error: %v, txnId: %d, be: %v", err, req.GetTxnId(), beRpc.backend)
	} else {
		return result, nil
	}
}

func WithBeRpcTimeout(timeout time.Duration) BeRpcOption {
	return func() callopt.Option {
		return callopt.WithRPCTimeout(timeout)
	}
}
