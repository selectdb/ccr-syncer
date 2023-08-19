package rpc

import (
	"fmt"

	"github.com/selectdb/ccr_syncer/ccr/base"
	beservice "github.com/selectdb/ccr_syncer/rpc/kitex_gen/backendservice/backendservice"
	feservice "github.com/selectdb/ccr_syncer/rpc/kitex_gen/frontendservice/frontendservice"

	"github.com/cloudwego/kitex/client"
	"github.com/pkg/errors"
)

type IRpcFactory interface {
	NewFeRpc(spec *base.Spec) (IFeRpc, error)
	NewBeRpc(be *base.Backend) (IBeRpc, error)
}

type RpcFactory struct {
}

func NewRpcFactory() IRpcFactory {
	return &RpcFactory{}
}

func (rf *RpcFactory) NewFeRpc(spec *base.Spec) (IFeRpc, error) {
	// valid spec
	if err := spec.Valid(); err != nil {
		return nil, err
	}

	// create kitex FrontendService client
	if fe_client, err := feservice.NewClient("FrontendService", client.WithHostPorts(spec.Host+":"+spec.ThriftPort)); err != nil {
		return nil, errors.Wrapf(err, "NewFeClient error: %v, spec: %s", err, spec)
	} else {
		return &FeRpc{
			client: fe_client,
		}, nil
	}
}

func (rf *RpcFactory) NewBeRpc(be *base.Backend) (IBeRpc, error) {
	// create kitex FrontendService client
	if client, err := beservice.NewClient("FrontendService", client.WithHostPorts(fmt.Sprintf("%s:%d", be.Host, be.BePort))); err != nil {
		return nil, errors.Wrapf(err, "NewBeClient error: %v", err)
	} else {
		return &BeRpc{
			backend: be,
			client:  client,
		}, nil
	}
}