package ccr

import (
	"github.com/selectdb/ccr_syncer/pkg/ccr/base"
)

type MetaerFactory interface {
	NewMeta(tableSpec *base.Spec) Metaer
}

type MetaFactory struct {
}

func NewMetaFactory() MetaerFactory {
	return &MetaFactory{}
}

func (mf *MetaFactory) NewMeta(spec *base.Spec) Metaer {
	return NewMeta(spec)
}
