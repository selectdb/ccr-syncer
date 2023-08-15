package ccr

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"

	"github.com/selectdb/ccr_syncer/ccr/base"
	"github.com/selectdb/ccr_syncer/rpc"
	"github.com/selectdb/ccr_syncer/utils"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/btree"
)

const (
	degree = 128
)

// All Update* functions force to update meta from fe
// TODO: reduce code, need to each level contain up level reference to get id

func fmtHostPort(host string, port uint16) string {
	return fmt.Sprintf("%s:%d", host, port)
}

// All op is not concurrent safety
// Meta
type Meta struct {
	base.Spec
	DatabaseMeta
	token                 string
	Backends              map[int64]*base.Backend // backendId -> backend
	DatabaseName2IdMap    map[string]int64
	TableName2IdMap       map[string]int64
	BackendHostPort2IdMap map[string]int64
}

func NewMeta(tableSpec *base.Spec) *Meta {
	return &Meta{
		Spec: *tableSpec,
		DatabaseMeta: DatabaseMeta{
			Tables: make(map[int64]*TableMeta),
		},
		Backends:              make(map[int64]*base.Backend),
		DatabaseName2IdMap:    make(map[string]int64),
		TableName2IdMap:       make(map[string]int64),
		BackendHostPort2IdMap: make(map[string]int64),
	}
}

func (m *Meta) GetDbId() (int64, error) {
	dbName := m.Database

	if dbId, ok := m.DatabaseName2IdMap[dbName]; ok {
		return dbId, nil
	}

	dbname := "default_cluster:" + dbName
	// mysql> show proc '/dbs/';
	// +-------+------------------------------------+----------+----------+-------------+--------------------------+--------------+--------------+------------------+
	// | DbId  | DbName                             | TableNum | Size     | Quota       | LastConsistencyCheckTime | ReplicaCount | ReplicaQuota | TransactionQuota |
	// +-------+------------------------------------+----------+----------+-------------+--------------------------+--------------+--------------+------------------+
	// | 0     | default_cluster:information_schema | 24       | 0.000    | 1024.000 TB | NULL                     | 0            | 1073741824   | 100              |
	// | 10002 | default_cluster:__internal_schema  | 4        | 0.000    | 1024.000 TB | NULL                     | 28           | 1073741824   | 100              |
	// | 10116 | default_cluster:ccr                | 2        | 2.738 KB | 1024.000 TB | NULL                     | 27           | 1073741824   | 100              |
	// +-------+------------------------------------+----------+----------+-------------+--------------------------+--------------+--------------+------------------+
	db, err := m.Connect()
	if err != nil {
		return 0, err
	}

	var dbId int64
	var parsedDbname string
	discardCols := make([]sql.RawBytes, 8)
	scanArgs := []interface{}{&dbId, &parsedDbname}
	for i := range discardCols {
		scanArgs = append(scanArgs, &discardCols[i])
	}
	rows, err := db.Query("show proc '/dbs/'")
	if err != nil {
		return 0, errors.Wrapf(err, "show proc '/dbs/' failed")
	}

	defer rows.Close()
	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return 0, errors.Wrapf(err, "show proc '/dbs/' failed")
		}
		// match parsedDbname == dbname, return dbId
		if parsedDbname == dbname {
			m.DatabaseName2IdMap[dbname] = dbId
			m.DatabaseMeta.Id = dbId
			return dbId, nil
		}
	}

	if err := rows.Err(); err != nil {
		return 0, errors.Wrapf(err, "show proc '/dbs/' failed")
	}

	// not found
	return 0, errors.Errorf("%s not found dbId", dbname)
}

func (m *Meta) GetFullTableName(tableName string) string {
	dbName := m.Database
	fullTableName := dbName + "." + tableName
	return fullTableName
}

func (m *Meta) UpdateTable(tableName string, tableId int64) (*TableMeta, error) {
	dbId, err := m.GetDbId()
	if err != nil {
		return nil, err
	}

	// mysql> show proc '/dbs/{dbId}';
	// 	mysql> show proc '/dbs/10116/';
	// +---------+---------------+----------+---------------------+--------------+--------+------+--------------------------+--------------+
	// | TableId | TableName     | IndexNum | PartitionColumnName | PartitionNum | State  | Type | LastConsistencyCheckTime | ReplicaCount |
	// +---------+---------------+----------+---------------------+--------------+--------+------+--------------------------+--------------+
	// | 10118   | enable_binlog | 1        | NULL                | 1            | NORMAL | OLAP | NULL                     | 3            |
	// | 13004   | tbl_time      | 1        | k1                  | 3            | NORMAL | OLAP | NULL                     | 24           |
	// +---------+---------------+----------+---------------------+--------------+--------+------+--------------------------+--------------+
	db, err := m.Connect()
	if err != nil {
		return nil, err
	}

	var parsedTableId int64
	var parsedTableName string
	discardCols := make([]sql.RawBytes, 8)
	scanArgs := []interface{}{&parsedTableId, &parsedTableName}
	for i := range discardCols {
		scanArgs = append(scanArgs, &discardCols[i])
	}
	query := fmt.Sprintf("show proc '/dbs/%d/'", dbId)
	rows, err := db.Query(query)
	if err != nil {
		return nil, errors.Wrap(err, query)
	}

	defer rows.Close()
	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return nil, errors.Wrap(err, query)
		}
		// match parsedDbname == dbname, return dbId
		if parsedTableName == tableName || parsedTableId == tableId {
			fullTableName := m.GetFullTableName(parsedTableName)
			log.Debugf("found table:%s, tableId:%d", fullTableName, parsedTableId)
			m.TableName2IdMap[fullTableName] = parsedTableId
			tableMeta := &TableMeta{
				DatabaseMeta:   &m.DatabaseMeta,
				Id:             parsedTableId,
				Name:           parsedTableName,
				PartitionIdMap: make(map[int64]*PartitionMeta),
			}
			m.Tables[parsedTableId] = tableMeta
			return tableMeta, nil
		}
	}

	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, query)
	}

	// not found
	return nil, errors.Errorf("%v not found table", tableId)
}

func (m *Meta) GetTable(tableId int64) (*TableMeta, error) {
	tableMeta, ok := m.Tables[tableId]
	if !ok {
		var err error
		if tableMeta, err = m.UpdateTable("", tableId); err != nil {
			return nil, err
		}
	}
	return tableMeta, nil
}

func (m *Meta) GetTableId(tableName string) (int64, error) {
	fullTableName := m.GetFullTableName(tableName)
	if tableId, ok := m.TableName2IdMap[fullTableName]; ok {
		return tableId, nil
	}

	if tableMeta, err := m.UpdateTable(tableName, 0); err != nil {
		return 0, err
	} else {
		return tableMeta.Id, nil
	}
}

func (m *Meta) UpdatePartitions(tableId int64) error {
	// Step 1: get dbId
	dbId, err := m.GetDbId()
	if err != nil {
		return err
	}

	// Step 2: get tableId
	table, err := m.GetTable(tableId)
	if err != nil {
		return err
	}

	// Step 3: get partitionIds
	// mysql> show proc '/dbs/10116/10118/partitions';
	// +-------------+---------------+----------------+---------------------+--------+--------------+-------+-----------------+---------+----------------+---------------+---------------------+---------------------+--------------------------+----------+------------+-------------------------+-----------+
	// | PartitionId | PartitionName | VisibleVersion | VisibleVersionTime  | State  | PartitionKey | Range | DistributionKey | Buckets | ReplicationNum | StorageMedium | CooldownTime        | RemoteStoragePolicy | LastConsistencyCheckTime | DataSize | IsInMemory | ReplicaAllocation       | IsMutable |
	// +-------------+---------------+----------------+---------------------+--------+--------------+-------+-----------------+---------+----------------+---------------+---------------------+---------------------+--------------------------+----------+------------+-------------------------+-----------+
	// | 10117       | enable_binlog | 12             | 2023-05-17 18:58:10 | NORMAL |              |       | user_id         | 3       | 1              | HDD           | 9999-12-31 23:59:59 |                     | NULL                     | 2.738 KB | false      | tag.location.default: 1 | true      |
	// +-------------+---------------+----------------+---------------------+--------+--------------+-------+-----------------+---------+----------------+---------------+---------------------+---------------------+--------------------------+----------+------------+-------------------------+-----------+
	db, err := m.Connect()
	if err != nil {
		return err
	}

	partitions := make([]*PartitionMeta, 0)
	// total columns 18
	var partitionId int64     // column 0
	var partitionName string  // column 1
	var partitionKey string   // column 5
	var partitionRange string // column 6
	discardCols := make([]sql.RawBytes, 14)
	scanArgs := []interface{}{&partitionId, &partitionName, &discardCols[0], &discardCols[1], &discardCols[2], &partitionKey, &partitionRange}
	for i := 3; i < len(discardCols); i++ {
		scanArgs = append(scanArgs, &discardCols[i])
	}
	query := fmt.Sprintf("show proc '/dbs/%d/%d/partitions'", dbId, table.Id)
	log.Debug(query)
	rows, err := db.Query(query)
	if err != nil {
		return errors.Wrap(err, query)
	}

	defer rows.Close()
	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return errors.Wrap(err, query)
		}
		log.Debugf("partitionId: %d, partitionName: %s", partitionId, partitionName)
		partition := &PartitionMeta{
			TableMeta: table,
			Id:        partitionId,
			Name:      partitionName,
			Key:       partitionKey,
			Range:     partitionRange,
		}
		partitions = append(partitions, partition)
	}

	if err := rows.Err(); err != nil {
		return errors.Wrap(err, query)
	}

	table.PartitionIdMap = make(map[int64]*PartitionMeta)
	table.PartitionRangeMap = make(map[string]*PartitionMeta)
	for _, partition := range partitions {
		table.PartitionIdMap[partition.Id] = partition
		table.PartitionRangeMap[partition.Range] = partition
	}

	return nil
}

func (m *Meta) getPartitionsWithUpdate(tableId int64, depth int64) (map[int64]*PartitionMeta, error) {
	if depth >= 3 {
		return nil, errors.Errorf("getPartitions depth >= 3")
	}

	if err := m.UpdatePartitions(tableId); err != nil {
		return nil, err
	}

	depth++
	return m.getPartitions(tableId, depth)
}

func (m *Meta) getPartitions(tableId int64, depth int64) (map[int64]*PartitionMeta, error) {
	if depth >= 3 {
		return nil, fmt.Errorf("getPartitions depth >= 3")
	}

	tableMeta, err := m.GetTable(tableId)
	if err != nil {
		return nil, err
	}
	if len(tableMeta.PartitionIdMap) == 0 {
		return m.getPartitionsWithUpdate(tableId, depth)
	}

	return tableMeta.PartitionIdMap, nil
}

func (m *Meta) GetPartitionIdMap(tableId int64) (map[int64]*PartitionMeta, error) {
	return m.getPartitions(tableId, 0)
}

func (m *Meta) GetPartitionRangeMap(tableId int64) (map[string]*PartitionMeta, error) {
	if _, err := m.GetPartitionIdMap(tableId); err != nil {
		return nil, err
	}

	tabletMeta, err := m.GetTable(tableId)
	if err != nil {
		return nil, err
	}
	return tabletMeta.PartitionRangeMap, nil
}

func (m *Meta) GetPartitionIds(tableName string) ([]int64, error) {
	// TODO: optimize performance, cache it
	tableId, err := m.GetTableId(tableName)
	if err != nil {
		return nil, err
	}

	partitions, err := m.GetPartitionIdMap(tableId)
	if err != nil {
		return nil, err
	}

	partitionIds := make([]int64, 0, len(partitions))
	for partitionId := range partitions {
		partitionIds = append(partitionIds, partitionId)
	}
	// return sort partitionIds
	sort.Slice(partitionIds, func(i, j int) bool {
		return partitionIds[i] < partitionIds[j]
	})
	return partitionIds, nil
}

func (m *Meta) GetPartitionName(tableId int64, partitionId int64) (string, error) {
	partitions, err := m.GetPartitionIdMap(tableId)
	if err != nil {
		return "", err
	}
	partition, ok := partitions[partitionId]
	if !ok {
		partitions, err = m.getPartitionsWithUpdate(tableId, 0)
		if err != nil {
			return "", err
		}
		if partition, ok = partitions[partitionId]; !ok {
			return "", errors.Errorf("partitionId %d not found", partitionId)
		}
	}

	return partition.Name, nil
}

func (m *Meta) GetPartitionRange(tableId int64, partitionId int64) (string, error) {
	partitions, err := m.GetPartitionIdMap(tableId)
	if err != nil {
		return "", err
	}
	partition, ok := partitions[partitionId]
	if !ok {
		partitions, err = m.getPartitionsWithUpdate(tableId, 0)
		if err != nil {
			return "", err
		}
		if partition, ok = partitions[partitionId]; !ok {
			return "", errors.Errorf("partitionId %d not found", partitionId)
		}
	}

	return partition.Range, nil
}

func (m *Meta) GetPartitionIdByName(tableId int64, partitionName string) (int64, error) {
	// TODO: optimize performance
	partitions, err := m.GetPartitionIdMap(tableId)
	if err != nil {
		return 0, err
	}

	for partitionId, partition := range partitions {
		if partition.Name == partitionName {
			return partitionId, nil
		}
	}
	partitions, err = m.getPartitionsWithUpdate(tableId, 0)
	if err != nil {
		return 0, err
	}
	for partitionId, partition := range partitions {
		if partition.Name == partitionName {
			return partitionId, nil
		}
	}

	return 0, errors.Errorf("partition name %s not found", partitionName)
}

func (m *Meta) GetPartitionIdByRange(tableId int64, partitionRange string) (int64, error) {
	// TODO: optimize performance
	partitions, err := m.GetPartitionIdMap(tableId)
	if err != nil {
		return 0, err
	}

	for partitionId, partition := range partitions {
		if partition.Range == partitionRange {
			return partitionId, nil
		}
	}
	partitions, err = m.getPartitionsWithUpdate(tableId, 0)
	if err != nil {
		return 0, err
	}
	for partitionId, partition := range partitions {
		if partition.Range == partitionRange {
			return partitionId, nil
		}
	}

	return 0, errors.Errorf("partition range %s not found", partitionRange)
}

func (m *Meta) UpdateBackends() error {
	// mysql> show proc '/backends';
	// +-----------+-----------------+-----------+---------------+--------+----------+----------+---------------------+---------------------+-------+----------------------+-----------------------+-----------+------------------+---------------+---------------+---------+----------------+--------------------+--------------------------+--------+------------------------------+-------------------------------------------------------------------------------------------------------------------------------+-------------------------+----------+
	// | BackendId | Cluster         | Host      | HeartbeatPort | BePort | HttpPort | BrpcPort | LastStartTime       | LastHeartbeat       | Alive | SystemDecommissioned | ClusterDecommissioned | TabletNum | DataUsedCapacity | AvailCapacity | TotalCapacity | UsedPct | MaxDiskUsedPct | RemoteUsedCapacity | Tag                      | ErrMsg | Version                      | Status                                                                                                                        | HeartbeatFailureCounter | NodeRole |
	// +-----------+-----------------+-----------+---------------+--------+----------+----------+---------------------+---------------------+-------+----------------------+-----------------------+-----------+------------------+---------------+---------------+---------+----------------+--------------------+--------------------------+--------+------------------------------+-------------------------------------------------------------------------------------------------------------------------------+-------------------------+----------+
	// | 10028     | default_cluster | 127.0.0.1 | 9050          | 9060   | 8040     | 8060     | 2023-05-19 15:42:57 | 2023-05-20 17:50:11 | true  | false                | false                 | 55        | 2.737 KB         | 2.547 TB      | 10.829 TB     | 76.48 % | 76.48 %        | 0.000              | {"location" : "default"} |        | doris-0.0.0-trunk-22c2a0a65a | {"lastSuccessReportTabletsTime":"2023-05-20 17:49:30","lastStreamLoadTime":-1,"isQueryDisabled":false,"isLoadDisabled":false} | 0                       | mix      |
	// +-----------+-----------------+-----------+---------------+--------+----------+----------+---------------------+---------------------+-------+----------------------+-----------------------+-----------+------------------+---------------+---------------+---------+----------------+--------------------+--------------------------+--------+------------------------------+-------------------------------------------------------------------------------------------------------------------------------+-------------------------+----------+
	db, err := m.Connect()
	if err != nil {
		return err
	}

	backends := make([]*base.Backend, 0)
	query := "show proc '/backends'"
	log.Debug(query)
	rows, err := db.Query(query)
	if err != nil {
		return errors.Wrap(err, query)
	}

	defer rows.Close()
	for rows.Next() {
		// total 23, backend 6
		var backend base.Backend
		discardCols := make([]sql.RawBytes, 17)
		scanArgs := []interface{}{&backend.Id, &backend.Host, &backend.HeartbeatPort, &backend.BePort, &backend.HttpPort, &backend.BrpcPort}
		for i := range discardCols {
			scanArgs = append(scanArgs, &discardCols[i])
		}

		if err := rows.Scan(scanArgs...); err != nil {
			return errors.Wrap(err, query)
		}
		log.Debugf("backend: %v", &backend)
		backends = append(backends, &backend)
	}

	for _, backend := range backends {
		m.Backends[backend.Id] = backend

		hostPort := fmtHostPort(backend.Host, backend.BePort)
		m.BackendHostPort2IdMap[hostPort] = backend.Id
	}

	return nil
}

func (m *Meta) GetBackends() ([]*base.Backend, error) {
	if len(m.Backends) > 0 {
		backends := make([]*base.Backend, 0, len(m.Backends))
		for _, backend := range m.Backends {
			backends = append(backends, backend)
		}
		return backends, nil
	}

	if err := m.UpdateBackends(); err != nil {
		return nil, err
	}

	return m.GetBackends()
}

func (m *Meta) GetBackendMap() (map[int64]*base.Backend, error) {
	if len(m.Backends) > 0 {
		return m.Backends, nil
	}

	if err := m.UpdateBackends(); err != nil {
		return nil, err
	}

	return m.GetBackendMap()
}

func (m *Meta) GetBackendId(host string, portStr string) (int64, error) {
	// convert port from string to uint16
	port, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil {
		return 0, err
	}
	hostPort := fmtHostPort(host, uint16(port))
	if backendId, ok := m.BackendHostPort2IdMap[hostPort]; ok {
		return backendId, nil
	}
	if err := m.UpdateBackends(); err != nil {
		return 0, err
	}
	if backendId, ok := m.BackendHostPort2IdMap[hostPort]; ok {
		return backendId, nil
	}

	return 0, errors.Errorf("hostPort: %s not found", hostPort)
}

func (m *Meta) UpdateIndexes(tableId int64, partitionId int64) error {
	// TODO: Optimize performance
	// Step 1: get dbId
	dbId, err := m.GetDbId()
	if err != nil {
		return err
	}

	// Step 2: get tableId
	table, err := m.GetTable(tableId)
	if err != nil {
		return err
	}

	// Step 3: get partitions
	partitions, err := m.GetPartitionIdMap(tableId)
	if err != nil {
		return err
	}

	partition, ok := partitions[partitionId]
	if !ok {
		return errors.Errorf("partitionId: %d not found", partitionId)
	}

	// mysql> show proc '/dbs/10116/10118/partitions/10117';
	// +---------+---------------+--------+--------------------------+
	// | IndexId | IndexName     | State  | LastConsistencyCheckTime |
	// +---------+---------------+--------+--------------------------+
	// | 10119   | enable_binlog | NORMAL | NULL                     |
	// +---------+---------------+--------+--------------------------+
	// 1 row in set (0.01 sec)
	db, err := m.Connect()
	if err != nil {
		return err
	}

	indexes := make([]*IndexMeta, 0)
	// totoal rows 4
	var indexId int64
	var indexName string
	discardCols := make([]sql.RawBytes, 2)
	scanArgs := []interface{}{&indexId, &indexName}
	for i := range discardCols {
		scanArgs = append(scanArgs, &discardCols[i])
	}
	query := fmt.Sprintf("show proc '/dbs/%d/%d/partitions/%d'", dbId, table.Id, partition.Id)
	log.Debug(query)
	rows, err := db.Query(query)
	if err != nil {
		return errors.Wrap(err, query)
	}

	defer rows.Close()
	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return errors.Wrap(err, query)
		}
		log.Debugf("indexId: %d, indexName: %s", indexId, indexName)

		index := &IndexMeta{
			PartitionMeta: partition,
			Id:            indexId,
			Name:          indexName,
		}
		indexes = append(indexes, index)
	}

	if err := rows.Err(); err != nil {
		return errors.Wrap(err, query)
	}

	partition.IndexIdMap = make(map[int64]*IndexMeta)
	partition.IndexNameMap = make(map[string]*IndexMeta)
	for _, index := range indexes {
		partition.IndexIdMap[index.Id] = index
		partition.IndexNameMap[index.Name] = index
	}

	return nil
}

func (m *Meta) getIndexes(tableId int64, partitionId int64, hasUpdate bool) (map[int64]*IndexMeta, error) {
	partitions, err := m.GetPartitionIdMap(tableId)
	if err != nil {
		return nil, err
	}

	partition, ok := partitions[partitionId]
	if !ok || len(partition.IndexIdMap) == 0 {
		if hasUpdate {
			return nil, errors.Errorf("partitionId: %d not found", partitionId)
		}

		err = m.UpdateIndexes(tableId, partitionId)
		if err != nil {
			return nil, err
		}
		return m.getIndexes(tableId, partitionId, true)
	}

	return partition.IndexIdMap, nil
}

func (m *Meta) GetIndexIdMap(tableId int64, partitionId int64) (map[int64]*IndexMeta, error) {
	return m.getIndexes(tableId, partitionId, false)
}

func (m *Meta) GetIndexNameMap(tableId int64, partitionId int64) (map[string]*IndexMeta, error) {
	if _, err := m.getIndexes(tableId, partitionId, false); err != nil {
		return nil, err
	}

	partitions, err := m.GetPartitionIdMap(tableId)
	if err != nil {
		return nil, err
	}

	partition := partitions[partitionId]
	return partition.IndexNameMap, nil
}

func (m *Meta) updateReplica(index *IndexMeta) error {
	indexId := index.Id
	partitionId := index.PartitionMeta.Id
	tableId := index.PartitionMeta.TableMeta.Id
	dbId := index.PartitionMeta.TableMeta.DatabaseMeta.Id
	// mysql> show proc '/dbs/10116/10118/partitions/10117/10119';
	// +----------+-----------+-----------+------------+---------+-------------------+------------------+---------------+---------------+----------------+----------+--------+-------------------------+--------------+--------------+-----------+----------------------+---------------------------------------------+-----------------------------------------------------------+-------------------+----------------+
	// | TabletId | ReplicaId | BackendId | SchemaHash | Version | LstSuccessVersion | LstFailedVersion | LstFailedTime | LocalDataSize | RemoteDataSize | RowCount | State  | LstConsistencyCheckTime | CheckVersion | VersionCount | QueryHits | PathHash             | MetaUrl                                     | CompactionStatus                                          | CooldownReplicaId | CooldownMetaId |
	// +----------+-----------+-----------+------------+---------+-------------------+------------------+---------------+---------------+----------------+----------+--------+-------------------------+--------------+--------------+-----------+----------------------+---------------------------------------------+-----------------------------------------------------------+-------------------+----------------+
	// | 10120    | 10121     | 10028     | 2092007568 | 28      | 28                | -1               | NULL          | 1401          | 0              | 1        | NORMAL | NULL                    | -1           | 1            | 0         | -6333148228203168624 | http://127.0.0.1:8040/api/meta/header/10120 | http://127.0.0.1:8040/api/compaction/show?tablet_id=10120 | -1                |                |
	// | 10122    | 10123     | 10028     | 2092007568 | 28      | 28                | -1               | NULL          | 0             | 0              | 0        | NORMAL | NULL                    | -1           | 1            | 0         | -6333148228203168624 | http://127.0.0.1:8040/api/meta/header/10122 | http://127.0.0.1:8040/api/compaction/show?tablet_id=10122 | -1                |                |
	// | 10124    | 10125     | 10028     | 2092007568 | 28      | 28                | -1               | NULL          | 1402          | 0              | 1        | NORMAL | NULL                    | -1           | 1            | 0         | -6333148228203168624 | http://127.0.0.1:8040/api/meta/header/10124 | http://127.0.0.1:8040/api/compaction/show?tablet_id=10124 | -1                |                |
	// +----------+-----------+-----------+------------+---------+-------------------+------------------+---------------+---------------+----------------+----------+--------+-------------------------+--------------+--------------+-----------+----------------------+---------------------------------------------+-----------------------------------------------------------+-------------------+----------------+
	db, err := m.Connect()
	if err != nil {
		return err
	}

	replicas := make([]*ReplicaMeta, 0)
	// total columns 21
	var tabletId int64
	var replicaId int64
	var backendId int64
	discardCols := make([]sql.RawBytes, 18)
	scanArgs := []interface{}{&tabletId, &replicaId, &backendId}
	for i := range discardCols {
		scanArgs = append(scanArgs, &discardCols[i])
	}
	query := fmt.Sprintf("show proc '/dbs/%d/%d/partitions/%d/%d'", dbId, tableId, partitionId, indexId)
	log.Debug(query)
	rows, err := db.Query(query)
	if err != nil {
		return errors.Wrap(err, query)
	}

	defer rows.Close()
	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return errors.Wrap(err, query)
		}
		replica := &ReplicaMeta{
			Id:        replicaId,
			TabletId:  tabletId,
			BackendId: backendId,
		}
		replicas = append(replicas, replica)
	}

	if err := rows.Err(); err != nil {
		return errors.Wrap(err, query)
	}

	index.TabletMetas = btree.NewMap[int64, *TabletMeta](degree)
	index.ReplicaMetas = btree.NewMap[int64, *ReplicaMeta](degree)
	for _, replica := range replicas {
		tablet, ok := index.TabletMetas.Get(replica.TabletId)
		if !ok {
			tablet = &TabletMeta{
				IndexMeta:    index,
				Id:           replica.TabletId,
				ReplicaMetas: btree.NewMap[int64, *ReplicaMeta](degree),
			}
			index.TabletMetas.Set(tablet.Id, tablet)
		}
		replica.TabletMeta = tablet
		tablet.ReplicaMetas.Set(replica.Id, replica)
		index.ReplicaMetas.Set(replica.Id, replica)
	}

	return nil
}

func (m *Meta) UpdateReplicas(tableId int64, partitionId int64) error {
	indexes, err := m.GetIndexIdMap(tableId, partitionId)
	if err != nil {
		return err
	}

	if len(indexes) == 0 {
		return errors.Errorf("indexes is empty")
	}

	// TODO: Update index as much as possible, record error
	for _, index := range indexes {
		if err := m.updateReplica(index); err != nil {
			return err
		}
	}

	return nil
}

func (m *Meta) GetReplicas(tableId int64, partitionId int64) (*btree.Map[int64, *ReplicaMeta], error) {
	indexes, err := m.GetIndexIdMap(tableId, partitionId)
	if err != nil {
		return nil, err
	}

	if len(indexes) == 0 {
		return nil, errors.Errorf("indexes is empty")
	}

	// fast path, no rollup
	if len(indexes) == 1 {
		var indexId int64
		for id := range indexes {
			indexId = id
			break
		}

		index := indexes[indexId]
		if index.ReplicaMetas == nil {
			if err := m.updateReplica(index); err != nil {
				return nil, err
			}
		}
		return index.ReplicaMetas, nil
	}

	// slow path, rollup
	replicas := btree.NewMap[int64, *ReplicaMeta](degree)
	for _, index := range indexes {
		if index.ReplicaMetas == nil {
			if err := m.updateReplica(index); err != nil {
				return nil, err
			}
		}

		for _, replica := range index.ReplicaMetas.Values() {
			replicas.Set(replica.Id, replica)
		}
	}

	return replicas, nil
}

func (m *Meta) GetTablets(tableId, partitionId, indexId int64) (*btree.Map[int64, *TabletMeta], error) {
	_, err := m.GetReplicas(tableId, partitionId)
	if err != nil {
		return nil, err
	}

	indexes, err := m.GetIndexIdMap(tableId, partitionId)
	if err != nil {
		return nil, err
	}

	if tablets, ok := indexes[indexId]; ok {
		return tablets.TabletMetas, nil
	} else {
		return nil, errors.Errorf("index %d not found", indexId)
	}
}

func (m *Meta) UpdateToken() error {
	spec := &m.Spec

	rpc, err := rpc.NewFeRpc(spec)
	if err != nil {
		return err
	}

	if token, err := rpc.GetMasterToken(spec); err != nil {
		return err
	} else {
		m.token = token
		return nil
	}
}

func (m *Meta) GetMasterToken() (string, error) {
	if m.token != "" {
		return m.token, nil
	}

	if err := m.UpdateToken(); err != nil {
		return "", err
	}

	return m.token, nil
}

func (m *Meta) GetTableNameById(tableId int64) (string, error) {
	// mysql> show table 41034;
	// +---------------------+-------------------+-------+
	// | DbName              | TableName         | DbId  |
	// +---------------------+-------------------+-------+
	// | default_cluster:ccr | editlog_partition | 10116 |
	// +---------------------+-------------------+-------+

	db, err := m.Connect()
	if err != nil {
		return "", err
	}

	var _dbName string
	var tableName string
	var _dbId int64
	sql := fmt.Sprintf("show table %d", tableId)
	rows, err := db.Query(sql)
	if err != nil {
		return "", errors.Wrap(err, sql)
	}
	defer rows.Close()

	for rows.Next() {
		if err := rows.Scan(&_dbName, &tableName, &_dbId); err != nil {
			return "", errors.Wrap(err, sql)
		}
	}

	if err := rows.Err(); err != nil {
		return "", errors.Wrap(err, sql)
	}

	return tableName, nil
}

// this method not called frequently, so it behaves like update, get and cache it every time
func (m *Meta) GetTables() (map[int64]*TableMeta, error) {
	dbId, err := m.GetDbId()
	if err != nil {
		return nil, err
	}

	// mysql> show proc '/dbs/{dbId}';
	// 	mysql> show proc '/dbs/10116/';
	// +---------+---------------+----------+---------------------+--------------+--------+------+--------------------------+--------------+
	// | TableId | TableName     | IndexNum | PartitionColumnName | PartitionNum | State  | Type | LastConsistencyCheckTime | ReplicaCount |
	// +---------+---------------+----------+---------------------+--------------+--------+------+--------------------------+--------------+
	// | 10118   | enable_binlog | 1        | NULL                | 1            | NORMAL | OLAP | NULL                     | 3            |
	// | 13004   | tbl_time      | 1        | k1                  | 3            | NORMAL | OLAP | NULL                     | 24           |
	// +---------+---------------+----------+---------------------+--------------+--------+------+--------------------------+--------------+
	db, err := m.Connect()
	if err != nil {
		return nil, err
	}

	var tableId int64
	var tableName string
	discardCols := make([]sql.RawBytes, 8)
	scanArgs := []interface{}{&tableId, &tableName}
	for i := range discardCols {
		scanArgs = append(scanArgs, &discardCols[i])
	}
	query := fmt.Sprintf("show proc '/dbs/%d/'", dbId)
	rows, err := db.Query(query)
	if err != nil {
		return nil, errors.Wrapf(err, query)
	}

	tableName2IdMap := make(map[string]int64)
	tables := make(map[int64]*TableMeta) // tableId -> table
	defer rows.Close()
	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return nil, errors.Wrapf(err, query)
		}

		// match parsedDbname == dbname, return dbId
		fullTableName := m.GetFullTableName(tableName)
		log.Debugf("found table:%s, tableId:%d", fullTableName, tableId)
		tableName2IdMap[fullTableName] = tableId
		tables[tableId] = &TableMeta{
			DatabaseMeta:   &m.DatabaseMeta,
			Id:             tableId,
			Name:           tableName,
			PartitionIdMap: make(map[int64]*PartitionMeta),
		}
	}

	if err := rows.Err(); err != nil {
		return nil, errors.Wrapf(err, query)
	}

	m.TableName2IdMap = tableName2IdMap
	m.Tables = tables
	return tables, nil
}

func (m *Meta) CheckBinlogFeature() error {
	// Step 1: get fe binlog feature
	if binlogIsEnabled, err := m.isFEBinlogFeature(); err != nil {
		return err
	} else if !binlogIsEnabled {
		return errors.Errorf("Fe %v:%v enable_binlog_feature=false, please set it true in fe.conf",
			m.Spec.Host, m.Spec.Port)
	}

	// Step 2: get be binlog feature
	if err := m.checkBEsBinlogFeature(); err != nil {
		return err
	}

	return nil
}

func (m *Meta) isFEBinlogFeature() (bool, error) {
	db, err := m.Connect()
	if err != nil {
		return false, err
	}

	isEnabled := false
	scanArgs := utils.MakeSingleColScanArgs(1, &isEnabled, 4)
	query := fmt.Sprintf("ADMIN SHOW FRONTEND CONFIG LIKE \"%%enable_feature_binlog%%\"")
	rows, err := db.Query(query)
	if err != nil {
		return false, errors.Wrap(err, query)
	}

	defer rows.Close()
	if rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return false, errors.Wrap(err, query)
		}
	}

	return isEnabled, nil
}

func (m *Meta) checkBEsBinlogFeature() error {
	backends, err := m.GetBackends()
	if err != nil {
		return err
	}

	var disabledBinlogBEs []string
	for _, backend := range backends {
		url := fmt.Sprintf("http://%v:%v/api/show_config?conf_item=enable_feature_binlog",
			backend.Host, backend.HttpPort)
		resp, err := http.Get(url)
		if err != nil {
			return errors.Wrapf(err, "get url: %s failed", url)
		}

		var configs [][]string
		if err := json.NewDecoder(resp.Body).Decode(&configs); err != nil {
			return errors.Wrapf(err, "decode json failed, url: %s", url)
		}

		if len(configs) != 1 {
			return errors.Errorf("Be %v:%v len(configs) is invalid, configs: %v",
				backend.Host, backend.HttpPort, configs)
		}

		if configs[0][2] != "1" {
			disabledBinlogBEs = append(disabledBinlogBEs, fmt.Sprintf("%v:%v", backend.Host, backend.HttpPort))
		}
	}

	if len(disabledBinlogBEs) != 0 {
		return errors.Errorf("Be: %v enable_feature_binlog=false, please set it true in be.conf",
			disabledBinlogBEs)
	}

	return nil
}

func (m *Meta) DirtyGetTables() map[int64]*TableMeta {
	return m.Tables
}
