package ccr

import (
	"github.com/selectdb/ccr_syncer/ccr/base"
	"github.com/selectdb/ccr_syncer/rpc"
)

type Factory struct {
	RpcFactory   rpc.IRpcFactory
	MetaFactory  IMetaFactory
	ISpecFactory base.ISpecFactory
}

func NewFactory(rpcFactory rpc.IRpcFactory, metaFactory IMetaFactory, ISpecFactory base.ISpecFactory) *Factory {
	return &Factory{
		RpcFactory:   rpcFactory,
		MetaFactory:  metaFactory,
		ISpecFactory: ISpecFactory,
	}
}
