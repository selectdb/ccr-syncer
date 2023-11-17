package ccr

import (
	"github.com/selectdb/ccr_syncer/pkg/ccr/base"
	"github.com/selectdb/ccr_syncer/pkg/rpc"
)

type Factory struct {
	rpc.IRpcFactory
	MetaerFactory
	base.SpecerFactory
}

func NewFactory(rpcFactory rpc.IRpcFactory, metaFactory MetaerFactory, ISpecFactory base.SpecerFactory) *Factory {
	return &Factory{
		IRpcFactory:   rpcFactory,
		MetaerFactory: metaFactory,
		SpecerFactory: ISpecFactory,
	}
}
