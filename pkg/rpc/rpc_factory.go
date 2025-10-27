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
	"fmt"
	"sync"

	"github.com/selectdb/ccr_syncer/pkg/ccr/base"
	beservice "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/backendservice/backendservice"
	"github.com/selectdb/ccr_syncer/pkg/xerror"
	log "github.com/sirupsen/logrus"

	"github.com/cloudwego/kitex/client"
)

type IRpcFactory interface {
	NewFeRpc(spec *base.Spec) (IFeRpc, error)
	NewBeRpc(be *base.Backend) (IBeRpc, error)
}

type RpcFactory struct {
	feRpcs     map[string]IFeRpc // key: connection string (cluster+host+port)
	feRpcsLock sync.Mutex

	beRpcs     map[base.Backend]IBeRpc
	beRpcsLock sync.Mutex
}

func NewRpcFactory() IRpcFactory {
	return &RpcFactory{
		feRpcs: make(map[string]IFeRpc),
		beRpcs: make(map[base.Backend]IBeRpc),
	}
}

// getFeKey generates a cache key for FE RPC based on connection info
// Key format: "cluster@host:port:thrift_port"
func getFeKey(spec *base.Spec) string {
	return fmt.Sprintf("%s@%s:%s:%s", spec.Cluster, spec.Host, spec.Port, spec.ThriftPort)
}

func (rf *RpcFactory) NewFeRpc(spec *base.Spec) (IFeRpc, error) {
	// valid spec
	if err := spec.Valid(); err != nil {
		return nil, err
	}

	// Generate cache key based on connection info (cluster + FE address)
	key := getFeKey(spec)

	rf.feRpcsLock.Lock()
	if feRpc, ok := rf.feRpcs[key]; ok {
		rf.feRpcsLock.Unlock()
		log.Debugf("RpcFactory: reused cached FeRpc for %s (cache hit)", key)
		return feRpc, nil
	}
	rf.feRpcsLock.Unlock()

	log.Debugf("RpcFactory: creating new FeRpc for %s (cache miss)", key)
	feRpc, err := NewFeRpc(spec)
	if err != nil {
		return nil, err
	}

	rf.feRpcsLock.Lock()
	defer rf.feRpcsLock.Unlock()
	rf.feRpcs[key] = feRpc
	return feRpc, nil
}

func (rf *RpcFactory) NewBeRpc(be *base.Backend) (IBeRpc, error) {
	rf.beRpcsLock.Lock()
	if beRpc, ok := rf.beRpcs[*be]; ok {
		rf.beRpcsLock.Unlock()
		return beRpc, nil
	}
	rf.beRpcsLock.Unlock()

	// create kitex BackendService client
	addr := fmt.Sprintf("%s:%d", be.Host, be.BePort)
	client, err := beservice.NewClient("BackendService",
		client.WithHostPorts(addr),
		client.WithConnectTimeout(connectTimeout),
		client.WithRPCTimeout(RpcTimeout))
	if err != nil {
		return nil, xerror.Wrapf(err, xerror.Normal, "NewBeClient error: %v", err)
	}

	beRpc := &BeRpc{
		backend: be,
		client:  client,
	}

	rf.beRpcsLock.Lock()
	defer rf.beRpcsLock.Unlock()
	rf.beRpcs[*be] = beRpc
	return beRpc, nil
}
