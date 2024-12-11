package rpc

import (
	"fmt"
	"sync"

	"github.com/selectdb/ccr_syncer/pkg/ccr/base"
	beservice "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/backendservice/backendservice"
	"github.com/selectdb/ccr_syncer/pkg/xerror"

	"github.com/cloudwego/kitex/client"
)

type IRpcFactory interface {
	NewFeRpc(spec *base.Spec) (IFeRpc, error)
	NewBeRpc(be *base.Backend) (IBeRpc, error)
}

type RpcFactory struct {
	feRpcs     map[*base.Spec]IFeRpc
	feRpcsLock sync.Mutex

	beRpcs     map[base.Backend]IBeRpc
	beRpcsLock sync.Mutex
}

func NewRpcFactory() IRpcFactory {
	return &RpcFactory{
		feRpcs: make(map[*base.Spec]IFeRpc),
		beRpcs: make(map[base.Backend]IBeRpc),
	}
}

func (rf *RpcFactory) NewFeRpc(spec *base.Spec) (IFeRpc, error) {
	// valid spec
	if err := spec.Valid(); err != nil {
		return nil, err
	}

	rf.feRpcsLock.Lock()
	if feRpc, ok := rf.feRpcs[spec]; ok {
		rf.feRpcsLock.Unlock()
		return feRpc, nil
	}
	rf.feRpcsLock.Unlock()

	feRpc, err := NewFeRpc(spec)
	if err != nil {
		return nil, err
	}

	rf.feRpcsLock.Lock()
	defer rf.feRpcsLock.Unlock()
	rf.feRpcs[spec] = feRpc
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
	client, err := beservice.NewClient("BackendService", client.WithHostPorts(addr), client.WithConnectTimeout(connectTimeout), client.WithRPCTimeout(rpcTimeout))
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
