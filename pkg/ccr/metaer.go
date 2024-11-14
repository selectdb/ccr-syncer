package ccr

import (
	"fmt"

	base "github.com/selectdb/ccr_syncer/pkg/ccr/base"
	"github.com/selectdb/ccr_syncer/pkg/rpc"
	"github.com/tidwall/btree"
)

type DatabaseMeta struct {
	Id     int64
	Tables map[int64]*TableMeta // tableId -> tableMeta
}

type TableMeta struct {
	DatabaseMeta      *DatabaseMeta
	Id                int64
	Name              string                    // maybe dirty, such after rename
	PartitionIdMap    map[int64]*PartitionMeta  // partitionId -> partitionMeta
	PartitionRangeMap map[string]*PartitionMeta // partitionRange -> partitionMeta
}

// Stringer
func (t *TableMeta) String() string {
	return fmt.Sprintf("TableMeta{(id:%d), (name:%s)}", t.Id, t.Name)
}

type PartitionMeta struct {
	TableMeta      *TableMeta
	Id             int64
	Name           string
	Range          string
	VisibleVersion int64
	IndexIdMap     map[int64]*IndexMeta  // indexId -> indexMeta
	IndexNameMap   map[string]*IndexMeta // indexName -> indexMeta
}

// Stringer
func (p *PartitionMeta) String() string {
	return fmt.Sprintf("PartitionMeta{(id:%d), (name:%s), (range:%s)}", p.Id, p.Name, p.Range)
}

type IndexMeta struct {
	PartitionMeta *PartitionMeta
	Id            int64
	Name          string
	IsBaseIndex   bool
	TabletMetas   *btree.Map[int64, *TabletMeta]  // tabletId -> tablet
	ReplicaMetas  *btree.Map[int64, *ReplicaMeta] // replicaId -> replica
}

type TabletMeta struct {
	IndexMeta    *IndexMeta
	Id           int64
	ReplicaMetas *btree.Map[int64, *ReplicaMeta] // replicaId -> replica
}

type ReplicaMeta struct {
	TabletMeta *TabletMeta
	Id         int64
	TabletId   int64
	BackendId  int64
	Version    int64
}

type MetaCleaner interface {
	ClearDB(dbName string)
	ClearTable(dbName string, tableName string)
}

type IngestBinlogMetaer interface {
	GetTablets(tableId, partitionId, indexId int64) (*btree.Map[int64, *TabletMeta], error)
	GetPartitionIdByRange(tableId int64, partitionRange string) (int64, error)
	GetPartitionRangeMap(tableId int64) (map[string]*PartitionMeta, error)
	GetIndexIdMap(tableId, partitionId int64) (map[int64]*IndexMeta, error)
	GetIndexNameMap(tableId, partitionId int64) (map[string]*IndexMeta, *IndexMeta, error)
	GetBackendMap() (map[int64]*base.Backend, error)
	IsPartitionDropped(partitionId int64) bool
	IsTableDropped(tableId int64) bool
	IsIndexDropped(indexId int64) bool
}

type Metaer interface {
	GetDbId() (int64, error)
	GetFullTableName(tableName string) string

	UpdateTable(tableName string, tableId int64) (*TableMeta, error)
	GetTable(tableId int64) (*TableMeta, error)
	GetTableId(tableName string) (int64, error)
	GetTableNameById(tableId int64) (string, error)
	GetTables() (map[int64]*TableMeta, error)

	UpdatePartitions(tableId int64) error
	GetPartitionIdMap(tableId int64) (map[int64]*PartitionMeta, error)
	GetPartitionIds(tableName string) ([]int64, error)
	GetPartitionName(tableId int64, partitionId int64) (string, error)
	GetPartitionRange(tableId int64, partitionId int64) (string, error)
	GetPartitionIdByName(tableId int64, partitionName string) (int64, error)

	GetFrontends() ([]*base.Frontend, error)
	UpdateBackends() error
	GetBackends() ([]*base.Backend, error)
	GetBackendId(host, portStr string) (int64, error)

	UpdateIndexes(tableId, partitionId int64) error

	UpdateReplicas(tableId, partitionId int64) error
	GetReplicas(tableId, partitionId int64) (*btree.Map[int64, *ReplicaMeta], error)

	UpdateToken(rpcFactory rpc.IRpcFactory) error
	GetMasterToken(rpcFactory rpc.IRpcFactory) (string, error)

	CheckBinlogFeature() error
	DirtyGetTables() map[int64]*TableMeta
	ClearTablesCache()

	IngestBinlogMetaer

	MetaCleaner
}
