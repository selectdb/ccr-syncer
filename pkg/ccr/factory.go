package ccr

import (
	"github.com/selectdb/ccr_syncer/pkg/ccr/base"
	"github.com/selectdb/ccr_syncer/pkg/rpc"
)

type Factory struct {
	RpcFactory   rpc.IRpcFactory
	MetaFactory  MetaerFactory
	ISpecFactory base.SpecerFactory
}

func NewFactory(rpcFactory rpc.IRpcFactory, metaFactory MetaerFactory, ISpecFactory base.SpecerFactory) *Factory {
	return &Factory{
		RpcFactory:   rpcFactory,
		MetaFactory:  metaFactory,
		ISpecFactory: ISpecFactory,
	}
}
