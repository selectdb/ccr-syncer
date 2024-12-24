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
package ccr

import (
	"github.com/selectdb/ccr_syncer/pkg/ccr/base"
	"github.com/selectdb/ccr_syncer/pkg/rpc"
	"github.com/selectdb/ccr_syncer/pkg/xerror"

	tstatus "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/status"

	"github.com/tidwall/btree"
)

var DefaultThriftMetaFactory ThriftMetaFactory = &defaultThriftMetaFactory{}

type ThriftMetaFactory interface {
	NewThriftMeta(spec *base.Spec, rpcFactory rpc.IRpcFactory, tableIds []int64) (*ThriftMeta, error)
}

type defaultThriftMetaFactory struct{}

func (dtmf *defaultThriftMetaFactory) NewThriftMeta(spec *base.Spec, rpcFactory rpc.IRpcFactory, tableIds []int64) (*ThriftMeta, error) {
	return NewThriftMeta(spec, rpcFactory, tableIds)
}

func NewThriftMeta(spec *base.Spec, rpcFactory rpc.IRpcFactory, tableIds []int64) (*ThriftMeta, error) {
	meta := NewMeta(spec)
	feRpc, err := rpcFactory.NewFeRpc(spec)
	if err != nil {
		return nil, err
	}

	// Step 1: get backends
	backendMetaResp, err := feRpc.GetBackends(spec)
	if err != nil {
		return nil, err
	}

	if backendMetaResp.GetStatus().GetStatusCode() != tstatus.TStatusCode_OK {
		return nil, xerror.Errorf(xerror.Meta, "get backend meta failed, status: %s", backendMetaResp.GetStatus())
	}

	if !backendMetaResp.IsSetBackends() {
		return nil, xerror.New(xerror.Meta, "get backend meta failed, backend meta not set")
	}

	for _, backend := range backendMetaResp.GetBackends() {
		backendMeta := &base.Backend{
			Id:       backend.GetId(),
			Host:     backend.GetHost(),
			BePort:   uint16(backend.GetBePort()),
			HttpPort: uint16(backend.GetHttpPort()),
			BrpcPort: uint16(backend.GetBrpcPort()),
		}
		meta.Backends[backendMeta.Id] = backendMeta
	}

	// Step 2: get table metas
	tableMetaResp, err := feRpc.GetTableMeta(spec, tableIds)
	if err != nil {
		return nil, err
	}

	if tableMetaResp.GetStatus().GetStatusCode() != tstatus.TStatusCode_OK {
		return nil, xerror.Errorf(xerror.Meta, "get table meta failed, status: %s", tableMetaResp.GetStatus())
	}

	if !tableMetaResp.IsSetDbMeta() {
		return nil, xerror.New(xerror.Meta, "get table meta failed, db meta not set")
	}

	dbMeta := tableMetaResp.GetDbMeta()
	for _, table := range dbMeta.GetTables() {
		tableMeta := &TableMeta{
			DatabaseMeta:      &meta.DatabaseMeta,
			Id:                table.GetId(),
			Name:              table.GetName(),
			PartitionIdMap:    make(map[int64]*PartitionMeta),
			PartitionRangeMap: make(map[string]*PartitionMeta),
		}
		meta.Id = dbMeta.GetId()
		meta.Tables[tableMeta.Id] = tableMeta
		meta.TableName2IdMap[tableMeta.Name] = tableMeta.Id

		for _, partition := range table.GetPartitions() {
			partitionMeta := &PartitionMeta{
				TableMeta:      tableMeta,
				Id:             partition.GetId(),
				Name:           partition.GetName(),
				Range:          partition.GetRange(),
				VisibleVersion: partition.GetVisibleVersion(),
				IndexIdMap:     make(map[int64]*IndexMeta),
				IndexNameMap:   make(map[string]*IndexMeta),
			}
			tableMeta.PartitionIdMap[partitionMeta.Id] = partitionMeta
			tableMeta.PartitionRangeMap[partitionMeta.Range] = partitionMeta

			for _, index := range partition.GetIndexes() {
				indexName := index.GetName()
				isBaseIndex := indexName == tableMeta.Name // it is accurate, since lock is held
				indexMeta := &IndexMeta{
					PartitionMeta: partitionMeta,
					Id:            index.GetId(),
					Name:          indexName,
					IsBaseIndex:   isBaseIndex,
					TabletMetas:   btree.NewMap[int64, *TabletMeta](degree),
					ReplicaMetas:  btree.NewMap[int64, *ReplicaMeta](degree),
				}
				partitionMeta.IndexIdMap[indexMeta.Id] = indexMeta
				partitionMeta.IndexNameMap[indexMeta.Name] = indexMeta
				if tableMeta.Name == indexMeta.Name {
					tableMeta.BaseIndexId = indexMeta.Id
				}

				for _, tablet := range index.GetTablets() {
					tabletMeta := &TabletMeta{
						IndexMeta:    indexMeta,
						Id:           tablet.GetId(),
						ReplicaMetas: btree.NewMap[int64, *ReplicaMeta](degree),
					}
					indexMeta.TabletMetas.Set(tabletMeta.Id, tabletMeta)

					for _, replica := range tablet.GetReplicas() {
						replicaMeta := &ReplicaMeta{
							TabletMeta: tabletMeta,
							Id:         replica.GetId(),
							TabletId:   tabletMeta.Id,
							BackendId:  replica.GetBackendId(),
							Version:    replica.GetVersion(),
						}
						tabletMeta.ReplicaMetas.Set(replicaMeta.Id, replicaMeta)
						indexMeta.ReplicaMetas.Set(replicaMeta.Id, replicaMeta)
					}
				}
			}
		}
	}

	droppedPartitions := make(map[int64]struct{})
	for _, partition := range dbMeta.GetDroppedPartitions() {
		droppedPartitions[partition] = struct{}{}
	}
	droppedTables := make(map[int64]struct{})
	for _, table := range dbMeta.GetDroppedTables() {
		droppedTables[table] = struct{}{}
	}
	droppedIndexes := make(map[int64]struct{})
	for _, index := range dbMeta.GetDroppedIndexes() {
		droppedIndexes[index] = struct{}{}
	}

	return &ThriftMeta{
		meta:              meta,
		droppedPartitions: droppedPartitions,
		droppedTables:     droppedTables,
		droppedIndexes:    droppedIndexes,
	}, nil
}

type ThriftMeta struct {
	meta              *Meta
	droppedPartitions map[int64]struct{}
	droppedTables     map[int64]struct{}
	droppedIndexes    map[int64]struct{}
}

func (tm *ThriftMeta) GetTablets(tableId, partitionId, indexId int64) (*btree.Map[int64, *TabletMeta], error) {
	dbId := tm.meta.Id

	tableMeta, ok := tm.meta.Tables[tableId]
	if !ok {
		return nil, xerror.Errorf(xerror.Meta, "dbId: %d, tableId: %d not found", dbId, tableId)
	}

	partitionMeta, ok := tableMeta.PartitionIdMap[partitionId]
	if !ok {
		return nil, xerror.Errorf(xerror.Meta, "dbId: %d, tableId: %d, partitionId: %d not found", dbId, tableId, partitionId)
	}

	indexMeta, ok := partitionMeta.IndexIdMap[indexId]
	if !ok {
		return nil, xerror.Errorf(xerror.Meta, "dbId: %d, tableId: %d, partitionId: %d, indexId: %d not found", dbId, tableId, partitionId, indexId)
	}

	return indexMeta.TabletMetas, nil
}

func (tm *ThriftMeta) GetPartitionIdByRange(tableId int64, partitionRange string) (int64, error) {
	dbId := tm.meta.Id

	tableMeta, ok := tm.meta.Tables[tableId]
	if !ok {
		return 0, xerror.Errorf(xerror.Meta, "dbId: %d, tableId: %d not found", dbId, tableId)
	}

	partitionMeta, ok := tableMeta.PartitionRangeMap[partitionRange]
	if !ok {
		return 0, xerror.Errorf(xerror.Meta, "dbId: %d, tableId: %d, partitionRange: %s not found", dbId, tableId, partitionRange)
	}

	return partitionMeta.Id, nil
}

func (tm *ThriftMeta) GetPartitionRangeMap(tableId int64) (map[string]*PartitionMeta, error) {
	dbId := tm.meta.Id

	tableMeta, ok := tm.meta.Tables[tableId]
	if !ok {
		return nil, xerror.Errorf(xerror.Meta, "dbId: %d, tableId: %d not found", dbId, tableId)
	}

	return tableMeta.PartitionRangeMap, nil
}

func (tm *ThriftMeta) GetIndexIdMap(tableId, partitionId int64) (map[int64]*IndexMeta, error) {
	dbId := tm.meta.Id

	tableMeta, ok := tm.meta.Tables[tableId]
	if !ok {
		return nil, xerror.Errorf(xerror.Meta, "dbId: %d, tableId: %d not found", dbId, tableId)
	}

	partitionMeta, ok := tableMeta.PartitionIdMap[partitionId]
	if !ok {
		return nil, xerror.Errorf(xerror.Meta, "dbId: %d, tableId: %d, partitionId: %d not found", dbId, tableId, partitionId)
	}

	return partitionMeta.IndexIdMap, nil
}

func (tm *ThriftMeta) GetIndexNameMap(tableId, partitionId int64) (map[string]*IndexMeta, *IndexMeta, error) {
	dbId := tm.meta.Id

	tableMeta, ok := tm.meta.Tables[tableId]
	if !ok {
		return nil, nil, xerror.Errorf(xerror.Meta, "dbId: %d, tableId: %d not found", dbId, tableId)
	}

	partitionMeta, ok := tableMeta.PartitionIdMap[partitionId]
	if !ok {
		return nil, nil, xerror.Errorf(xerror.Meta, "dbId: %d, tableId: %d, partitionId: %d not found", dbId, tableId, partitionId)
	}

	baseIndex, ok := partitionMeta.IndexNameMap[tableMeta.Name]
	if !ok {
		return nil, nil, xerror.Errorf(xerror.Meta, "dbId: %d, tableId: %d, partitionId: %d, indexName: %s not found", dbId, tableId, partitionId, tableMeta.Name)
	}

	return partitionMeta.IndexNameMap, baseIndex, nil
}

func (tm *ThriftMeta) GetBackendMap() (map[int64]*base.Backend, error) {
	if tm.meta.HostMapping == nil {
		return tm.meta.Backends, nil
	}

	backends := make(map[int64]*base.Backend)
	for id, backend := range tm.meta.Backends {
		if host, ok := tm.meta.HostMapping[backend.Host]; ok {
			backend.Host = host
		} else {
			return nil, xerror.Errorf(xerror.Normal,
				"the public ip of host %s is not found, consider adding it via HTTP API /update_host_mapping", backend.Host)
		}
		backends[id] = backend
	}
	return backends, nil
}

// Whether the target partition are dropped
func (tm *ThriftMeta) IsPartitionDropped(partitionId int64) bool {
	_, ok := tm.droppedPartitions[partitionId]
	return ok
}

// Whether the target table are dropped
func (tm *ThriftMeta) IsTableDropped(tableId int64) bool {
	_, ok := tm.droppedTables[tableId]
	return ok
}

// Whether the target index are dropped
func (tm *ThriftMeta) IsIndexDropped(tableId int64) bool {
	_, ok := tm.droppedIndexes[tableId]
	return ok
}
