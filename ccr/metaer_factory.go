package ccr

import (
	"github.com/selectdb/ccr_syncer/ccr/base"
)

type MetaerFactory interface {
	NewMeta(tableSpec *base.Spec) Metaer
}

type MetaFactory struct {
}

func NewMetaFactory() MetaerFactory {
	return &MetaFactory{}
}

func (mf *MetaFactory) NewMeta(tableSpec *base.Spec) Metaer {
	return &Meta{
		Spec: tableSpec,
		DatabaseMeta: DatabaseMeta{
			Tables: make(map[int64]*TableMeta),
		},
		Backends:              make(map[int64]*base.Backend),
		DatabaseName2IdMap:    make(map[string]int64),
		TableName2IdMap:       make(map[string]int64),
		BackendHostPort2IdMap: make(map[string]int64),
	}
}
