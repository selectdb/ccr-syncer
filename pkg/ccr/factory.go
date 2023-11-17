package ccr

import (
	"github.com/selectdb/ccr_syncer/pkg/ccr/base"
	"github.com/selectdb/ccr_syncer/pkg/rpc"
)

type Factory struct {
	rpc.IRpcFactory
	MetaFactory  MetaerFactory
	ISpecFactory base.SpecerFactory
}

func NewFactory(rpcFactory rpc.IRpcFactory, metaFactory MetaerFactory, ISpecFactory base.SpecerFactory) *Factory {
	return &Factory{
		IRpcFactory:  rpcFactory,
		MetaFactory:  metaFactory,
		ISpecFactory: ISpecFactory,
	}
}

// func (f *Factory) NewFeRpc(spec *base.Spec) (rpc.IFeRpc, error) {
// 	return f.RpcFactory.NewFeRpc(spec)
// }

// func (f *Factory) NewBeRpc(be *base.Backend) (rpc.IBeRpc, error) {
// 	return f.RpcFactory.NewBeRpc(be)
// }
