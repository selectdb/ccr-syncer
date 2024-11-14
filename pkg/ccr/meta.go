package ccr

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"

	"github.com/selectdb/ccr_syncer/pkg/ccr/base"
	"github.com/selectdb/ccr_syncer/pkg/rpc"
	tstatus "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/status"
	utils "github.com/selectdb/ccr_syncer/pkg/utils"
	"github.com/selectdb/ccr_syncer/pkg/xerror"

	log "github.com/sirupsen/logrus"
	"github.com/tidwall/btree"
)

const (
	degree = 128

	showErrMsg = "show proc '/dbs/' failed"

	TABLE_TYPE_OLAP = "OLAP"
	TABLE_TYPE_VIEW = "VIEW"
)

// All Update* functions force to update meta from fe
// TODO: reduce code, need to each level contain up level reference to get id

func fmtHostPort(host string, port uint16) string {
	return fmt.Sprintf("%s:%d", host, port)
}

// All op is not concurrent safety
// Meta
type Meta struct {
	*base.Spec
	DatabaseMeta
	token                 string
	Backends              map[int64]*base.Backend // backendId -> backend
	DatabaseName2IdMap    map[string]int64
	TableName2IdMap       map[string]int64
	BackendHostPort2IdMap map[string]int64
}

func NewMeta(spec *base.Spec) *Meta {
	return &Meta{
		Spec: spec,
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

	dbFullName := "default_cluster:" + dbName
	// mysql> show proc '/dbs/';
	// +-------+------------------------------------+----------+----------+-------------+--------------------------+--------------+--------------+------------------+
	// | DbId  | DbName                             | TableNum | Size     | Quota       | LastConsistencyCheckTime | ReplicaCount | ReplicaQuota | TransactionQuota |
	// +-------+------------------------------------+----------+----------+-------------+--------------------------+--------------+--------------+------------------+
	// | 0     | information_schema | 24       | 0.000    | 1024.000 TB | NULL                     | 0            | 1073741824   | 100              |
	// | 10002 | __internal_schema  | 4        | 0.000    | 1024.000 TB | NULL                     | 28           | 1073741824   | 100              |
	// | 10116 | ccr                | 2        | 2.738 KB | 1024.000 TB | NULL                     | 27           | 1073741824   | 100              |
	// +-------+------------------------------------+----------+----------+-------------+--------------------------+--------------+--------------+------------------+
	db, err := m.Connect()
	if err != nil {
		return 0, err
	}

	rows, err := db.Query("show proc '/dbs/'")
	if err != nil {
		return 1, xerror.Wrap(err, xerror.Normal, showErrMsg)
	}
	defer rows.Close()

	for rows.Next() {
		rowParser := utils.NewRowParser()
		if err := rowParser.Parse(rows); err != nil {
			return 0, xerror.Wrap(err, xerror.Normal, showErrMsg)
		}

		dbId, err := rowParser.GetInt64("DbId")
		if err != nil {
			return 0, xerror.Wrap(err, xerror.Normal, showErrMsg)
		}
		parsedDbName, err := rowParser.GetString("DbName")
		if err != nil {
			return 0, xerror.Wrap(err, xerror.Normal, showErrMsg)
		}

		// match parsedDbname == dbname, return dbId
		// the default_cluster prefix of db name will be removed in Doris v2.1.
		// here we compare both db name and db full name to make it compatible.
		if parsedDbName == dbName || parsedDbName == dbFullName {
			m.DatabaseName2IdMap[dbFullName] = dbId
			m.DatabaseMeta.Id = dbId
			return dbId, nil
		}
	}

	if err := rows.Err(); err != nil {
		return 0, xerror.Wrapf(err, xerror.Normal, showErrMsg)
	}

	// not found
	// ATTN: we don't treat db not found as xerror.Meta category.
	return 0, xerror.Errorf(xerror.Normal, "%s not found dbId", dbFullName)
}

func (m *Meta) GetFullTableName(tableName string) string {
	dbName := m.Database
	fullTableName := dbName + "." + tableName
	return fullTableName
}

// Update table meta, return xerror.Meta category if no such table exists.
func (m *Meta) UpdateTable(tableName string, tableId int64) (*TableMeta, error) {
	log.Infof("UpdateTable tableName: %s, tableId: %d", tableName, tableId)

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

	query := fmt.Sprintf("show proc '/dbs/%d/'", dbId)
	log.Infof("UpdateTable Sql: %s", query)
	rows, err := db.Query(query)
	if err != nil {
		return nil, xerror.Wrap(err, xerror.Normal, query)
	}
	defer rows.Close()

	for rows.Next() {
		rowParser := utils.NewRowParser()
		if err := rowParser.Parse(rows); err != nil {
			return nil, xerror.Wrapf(err, xerror.Normal, query)
		}

		parsedTableId, err := rowParser.GetInt64("TableId")
		if err != nil {
			return nil, xerror.Wrapf(err, xerror.Normal, query)
		}
		parsedTableName, err := rowParser.GetString("TableName")
		if err != nil {
			return nil, xerror.Wrapf(err, xerror.Normal, query)
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
		return nil, xerror.Wrap(err, xerror.Normal, query)
	}

	// not found
	return nil, xerror.Errorf(xerror.Meta, "tableName %s tableId %v not found table", tableName, tableId)
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
	query := fmt.Sprintf("show proc '/dbs/%d/%d/partitions'", dbId, table.Id)
	log.Debug(query)
	rows, err := db.Query(query)
	if err != nil {
		return xerror.Wrap(err, xerror.Normal, query)
	}

	defer rows.Close()
	for rows.Next() {
		rowParser := utils.NewRowParser()
		if err := rowParser.Parse(rows); err != nil {
			return xerror.Wrapf(err, xerror.Normal, query)
		}

		// get data
		partitionId, err := rowParser.GetInt64("PartitionId")
		if err != nil {
			return xerror.Wrapf(err, xerror.Normal, query)
		}
		partitionName, err := rowParser.GetString("PartitionName")
		if err != nil {
			return xerror.Wrapf(err, xerror.Normal, query)
		}
		partitionRange, err := rowParser.GetString("Range")
		if err != nil {
			return xerror.Wrapf(err, xerror.Normal, query)
		}
		log.Debugf("partitionId: %d, partitionName: %s", partitionId, partitionName)
		partition := &PartitionMeta{
			TableMeta: table,
			Id:        partitionId,
			Name:      partitionName,
			Range:     partitionRange,
		}
		partitions = append(partitions, partition)
	}

	if err := rows.Err(); err != nil {
		return xerror.Wrap(err, xerror.Normal, query)
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
		return nil, xerror.Errorf(xerror.Normal, "getPartitions depth >= 3")
	}

	if err := m.UpdatePartitions(tableId); err != nil {
		return nil, err
	}

	depth++
	return m.getPartitions(tableId, depth)
}

func (m *Meta) getPartitions(tableId int64, depth int64) (map[int64]*PartitionMeta, error) {
	if depth >= 3 {
		return nil, xerror.Errorf(xerror.Normal, "getPartitions depth >= 3")
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

// Get partition id map, return xerror.Meta category if no such table exists.
func (m *Meta) GetPartitionIdMap(tableId int64) (map[int64]*PartitionMeta, error) {
	return m.getPartitions(tableId, 0)
}

// Get partition range map, return xerror.Meta category if no such table exists.
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

// Get partition range by name, return xerror.Meta category if no such table or partition exists.
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
			return "", xerror.Errorf(xerror.Meta, "partitionId %d not found", partitionId)
		}
	}

	return partition.Name, nil
}

// Get partition range by id, return xerror.Meta category if no such table or partition exists.
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
			return "", xerror.Errorf(xerror.Meta, "partitionId %d not found", partitionId)
		}
	}

	return partition.Range, nil
}

// Get partition id by name, return xerror.Meta category if no such partition exists.
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

	return 0, xerror.Errorf(xerror.Meta, "partition name %s not found", partitionName)
}

// Get partition id by range, return xerror.Meta category if no such partition exists.
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

	return 0, xerror.Errorf(xerror.Meta, "partition range %s not found", partitionRange)
}

func (m *Meta) UpdateBackends() error {
	// mysql> show backends;
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
	query := "show backends"
	log.Debug(query)
	rows, err := db.Query(query)
	if err != nil {
		return xerror.Wrap(err, xerror.Normal, query)
	}

	defer rows.Close()
	for rows.Next() {
		rowParser := utils.NewRowParser()
		if err := rowParser.Parse(rows); err != nil {
			return xerror.Wrapf(err, xerror.Normal, query)
		}

		var backend base.Backend
		backend.Id, err = rowParser.GetInt64("BackendId")
		if err != nil {
			return xerror.Wrapf(err, xerror.Normal, query)
		}
		backend.Host, err = rowParser.GetString("Host")
		if err != nil {
			return xerror.Wrapf(err, xerror.Normal, query)
		}

		var port int64
		port, err = rowParser.GetInt64("BePort")
		if err != nil {
			return xerror.Wrapf(err, xerror.Normal, query)
		}
		backend.BePort = uint16(port)
		port, err = rowParser.GetInt64("HttpPort")
		if err != nil {
			return xerror.Wrapf(err, xerror.Normal, query)
		}
		backend.HttpPort = uint16(port)
		port, err = rowParser.GetInt64("BrpcPort")
		if err != nil {
			return xerror.Wrapf(err, xerror.Normal, query)
		}
		backend.BrpcPort = uint16(port)

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

func (m *Meta) GetFrontends() ([]*base.Frontend, error) {
	db, err := m.Connect()
	if err != nil {
		return nil, err
	}

	query := "select Host, QueryPort, RpcPort, IsMaster from frontends();"
	log.Debug(query)
	rows, err := db.Query(query)
	if err != nil {
		return nil, xerror.Wrap(err, xerror.Normal, query)
	}

	frontends := make([]*base.Frontend, 0)
	defer rows.Close()
	for rows.Next() {
		rowParser := utils.NewRowParser()
		if err := rowParser.Parse(rows); err != nil {
			return nil, xerror.Wrapf(err, xerror.Normal, query)
		}

		var fe base.Frontend
		fe.Host, err = rowParser.GetString("Host")
		if err != nil {
			return nil, xerror.Wrapf(err, xerror.Normal, query)
		}

		fe.Port, err = rowParser.GetString("QueryPort")
		if err != nil {
			return nil, xerror.Wrapf(err, xerror.Normal, query)
		}

		fe.ThriftPort, err = rowParser.GetString("RpcPort")
		if err != nil {
			return nil, xerror.Wrapf(err, xerror.Normal, query)
		}

		fe.IsMaster, err = rowParser.GetBool("IsMaster")
		if err != nil {
			return nil, xerror.Wrapf(err, xerror.Normal, query)
		}

		frontends = append(frontends, &fe)
	}
	return frontends, nil
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

	return 0, xerror.Errorf(xerror.Normal, "hostPort: %s not found", hostPort)
}

// Update indexes by table and partition, return xerror.Meta category if no such table or partition exists.
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
		return xerror.Errorf(xerror.Meta, "partitionId: %d not found", partitionId)
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
	query := fmt.Sprintf("show proc '/dbs/%d/%d/partitions/%d'", dbId, table.Id, partition.Id)
	log.Debug(query)
	rows, err := db.Query(query)
	if err != nil {
		return xerror.Wrap(err, xerror.Normal, query)
	}

	defer rows.Close()
	for rows.Next() {
		rowParser := utils.NewRowParser()
		if err := rowParser.Parse(rows); err != nil {
			return xerror.Wrapf(err, xerror.Normal, query)
		}

		indexId, err := rowParser.GetInt64("IndexId")
		if err != nil {
			return xerror.Wrapf(err, xerror.Normal, query)
		}
		indexName, err := rowParser.GetString("IndexName")
		if err != nil {
			return xerror.Wrapf(err, xerror.Normal, query)
		}
		log.Debugf("indexId: %d, indexName: %s", indexId, indexName)

		isBaseIndex := table.Name == indexName // it might be staled, caused by rename table
		index := &IndexMeta{
			PartitionMeta: partition,
			Id:            indexId,
			Name:          indexName,
			IsBaseIndex:   isBaseIndex,
		}
		indexes = append(indexes, index)
	}

	if err := rows.Err(); err != nil {
		return xerror.Wrap(err, xerror.Normal, query)
	}

	partition.IndexIdMap = make(map[int64]*IndexMeta)
	partition.IndexNameMap = make(map[string]*IndexMeta)
	for _, index := range indexes {
		partition.IndexIdMap[index.Id] = index
		partition.IndexNameMap[index.Name] = index
	}

	return nil
}

// Get indexes by table and partition, return xerror.Meta if no such table or partition exists.
func (m *Meta) getIndexes(tableId int64, partitionId int64, hasUpdate bool) (map[int64]*IndexMeta, error) {
	partitions, err := m.GetPartitionIdMap(tableId)
	if err != nil {
		return nil, err
	}

	partition, ok := partitions[partitionId]
	if !ok || len(partition.IndexIdMap) == 0 {
		if hasUpdate {
			return nil, xerror.Errorf(xerror.Meta, "partitionId: %d not found", partitionId)
		}

		err = m.UpdateIndexes(tableId, partitionId)
		if err != nil {
			return nil, err
		}
		return m.getIndexes(tableId, partitionId, true)
	}

	return partition.IndexIdMap, nil
}

// Get indexes id map by table and partition, return xerror.Meta if no such table or partition exists.
func (m *Meta) GetIndexIdMap(tableId int64, partitionId int64) (map[int64]*IndexMeta, error) {
	return m.getIndexes(tableId, partitionId, false)
}

// Get indexes name map by table and partition, return xerror.Meta if no such table or partition exists.
func (m *Meta) GetIndexNameMap(tableId int64, partitionId int64) (map[string]*IndexMeta, *IndexMeta, error) {
	if _, err := m.getIndexes(tableId, partitionId, false); err != nil {
		return nil, nil, err
	}

	partitions, err := m.GetPartitionIdMap(tableId)
	if err != nil {
		return nil, nil, err
	}

	if partition, ok := partitions[partitionId]; !ok {
		return nil, nil, xerror.Errorf(xerror.Meta, "partition %d is not found", partitionId)
	} else {
		return partition.IndexNameMap, nil, nil
	}
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
	query := fmt.Sprintf("show proc '/dbs/%d/%d/partitions/%d/%d'", dbId, tableId, partitionId, indexId)
	log.Debug(query)
	rows, err := db.Query(query)
	if err != nil {
		return xerror.Wrap(err, xerror.Normal, query)
	}

	defer rows.Close()
	for rows.Next() {
		rowParser := utils.NewRowParser()
		if err := rowParser.Parse(rows); err != nil {
			return xerror.Wrapf(err, xerror.Normal, query)
		}

		tabletId, err := rowParser.GetInt64("TabletId")
		if err != nil {
			return xerror.Wrapf(err, xerror.Normal, query)
		}
		replicaId, err := rowParser.GetInt64("ReplicaId")
		if err != nil {
			return xerror.Wrapf(err, xerror.Normal, query)
		}
		backendId, err := rowParser.GetInt64("BackendId")
		if err != nil {
			return xerror.Wrapf(err, xerror.Normal, query)
		}
		replica := &ReplicaMeta{
			Id:        replicaId,
			TabletId:  tabletId,
			BackendId: backendId,
		}
		replicas = append(replicas, replica)
	}

	if err := rows.Err(); err != nil {
		return xerror.Wrap(err, xerror.Normal, query)
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

// Update replicas by table and partition, return xerror.Meta category if no such table or partition exists.
func (m *Meta) UpdateReplicas(tableId int64, partitionId int64) error {
	indexes, err := m.GetIndexIdMap(tableId, partitionId)
	if err != nil {
		return err
	}

	if len(indexes) == 0 {
		return xerror.Errorf(xerror.Meta, "indexes is empty")
	}

	// TODO: Update index as much as possible, record error
	for _, index := range indexes {
		if err := m.updateReplica(index); err != nil {
			return err
		}
	}

	return nil
}

// Get replicas by table and partition, return xerror.Meta category if no such table or partition exists.
func (m *Meta) GetReplicas(tableId int64, partitionId int64) (*btree.Map[int64, *ReplicaMeta], error) {
	indexes, err := m.GetIndexIdMap(tableId, partitionId)
	if err != nil {
		return nil, err
	}

	if len(indexes) == 0 {
		return nil, xerror.Errorf(xerror.Meta, "indexes is empty")
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

// Get tablets by table, partition and index, return xerror.Meta category if no such table, partition or index exists.
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
		return nil, xerror.Errorf(xerror.Meta, "index %d not found", indexId)
	}
}

func (m *Meta) UpdateToken(rpcFactory rpc.IRpcFactory) error {
	spec := m.Spec

	rpc, err := rpcFactory.NewFeRpc(spec)
	if err != nil {
		return err
	}

	if resp, err := rpc.GetMasterToken(spec); err != nil {
		return err
	} else if resp.GetStatus().GetStatusCode() != tstatus.TStatusCode_OK {
		return xerror.Errorf(xerror.Meta, "get master token failed, status: %s", resp.GetStatus().String())
	} else {
		m.token = resp.GetToken()
		return nil
	}
}

func (m *Meta) GetMasterToken(rpcFactory rpc.IRpcFactory) (string, error) {
	if m.token != "" {
		return m.token, nil
	}

	if err := m.UpdateToken(rpcFactory); err != nil {
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

	sql := fmt.Sprintf("show table %d", tableId)
	rows, err := db.Query(sql)
	if err != nil {
		return "", xerror.Wrap(err, xerror.Normal, sql)
	}
	defer rows.Close()

	var tableName string
	for rows.Next() {
		rowParser := utils.NewRowParser()
		if err := rowParser.Parse(rows); err != nil {
			return "", xerror.Wrapf(err, xerror.Normal, sql)
		}

		tableName, err = rowParser.GetString("TableName")
		if err != nil {
			return "", xerror.Wrap(err, xerror.Normal, sql)
		}
	}

	if err := rows.Err(); err != nil {
		return "", xerror.Wrap(err, xerror.Normal, sql)
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

	query := fmt.Sprintf("show proc '/dbs/%d/'", dbId)
	rows, err := db.Query(query)
	if err != nil {
		return nil, xerror.Wrapf(err, xerror.Normal, query)
	}

	tableName2IdMap := make(map[string]int64)
	tables := make(map[int64]*TableMeta) // tableId -> table
	defer rows.Close()
	for rows.Next() {
		rowParser := utils.NewRowParser()
		if err := rowParser.Parse(rows); err != nil {
			return nil, xerror.Wrapf(err, xerror.Normal, query)
		}

		tableId, err := rowParser.GetInt64("TableId")
		if err != nil {
			return nil, xerror.Wrapf(err, xerror.Normal, query)
		}
		tableName, err := rowParser.GetString("TableName")
		if err != nil {
			return nil, xerror.Wrapf(err, xerror.Normal, query)
		}
		tableType, err := rowParser.GetString("Type")
		if err != nil {
			return nil, xerror.Wrapf(err, xerror.Normal, "get tables Type failed, query: %s", query)
		}

		fullTableName := m.GetFullTableName(tableName)
		log.Debugf("found table: %s, id: %d, type: %s", fullTableName, tableId, tableType)

		if tableType != TABLE_TYPE_OLAP && tableType != TABLE_TYPE_VIEW {
			// See fe/fe-core/src/main/java/org/apache/doris/backup/BackupHandler.java:backup() for details
			continue
		}

		// match parsedDbname == dbname, return dbId
		tableName2IdMap[fullTableName] = tableId
		tables[tableId] = &TableMeta{
			DatabaseMeta:   &m.DatabaseMeta,
			Id:             tableId,
			Name:           tableName,
			PartitionIdMap: make(map[int64]*PartitionMeta),
		}
	}

	if err := rows.Err(); err != nil {
		return nil, xerror.Wrapf(err, xerror.Normal, query)
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
		return xerror.Errorf(xerror.Normal, "Fe %v:%v enable_feature_binlog=false, please set it true in fe.conf",
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

	query := fmt.Sprintf("ADMIN SHOW FRONTEND CONFIG LIKE \"%%enable_feature_binlog%%\"")
	rows, err := db.Query(query)
	if err != nil {
		return false, xerror.Wrap(err, xerror.Normal, query)
	}
	defer rows.Close()

	var isEnabled bool = false
	if rows.Next() {
		rowParser := utils.NewRowParser()
		if err := rowParser.Parse(rows); err != nil {
			return false, xerror.Wrapf(err, xerror.Normal, query)
		}

		isEnabled, err = rowParser.GetBool("Value")
		if err != nil {
			return false, xerror.Wrap(err, xerror.Normal, query)
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
			return xerror.Wrapf(err, xerror.Normal, "get url: %s failed", url)
		}

		var configs [][]string
		if err := json.NewDecoder(resp.Body).Decode(&configs); err != nil {
			return xerror.Wrapf(err, xerror.Normal, "decode json failed, url: %s", url)
		}

		if len(configs) != 1 {
			return xerror.Errorf(xerror.Normal, "Be %v:%v len(configs) is invalid, configs: %v",
				backend.Host, backend.HttpPort, configs)
		}

		if configs[0][2] != "true" && configs[0][2] != "1" {
			disabledBinlogBEs = append(disabledBinlogBEs, fmt.Sprintf("%v:%v", backend.Host, backend.HttpPort))
		}
	}

	if len(disabledBinlogBEs) != 0 {
		return xerror.Errorf(xerror.Normal, "Be: %v enable_feature_binlog=false, please set it true in be.conf",
			disabledBinlogBEs)
	}

	return nil
}

func (m *Meta) DirtyGetTables() map[int64]*TableMeta {
	return m.Tables
}

func (m *Meta) ClearTablesCache() {
	m.Tables = make(map[int64]*TableMeta)
	m.TableName2IdMap = make(map[string]int64)
}

func (m *Meta) ClearDB(dbName string) {
	if m.Database != dbName {
		log.Info("dbName not match, skip clear")
		return
	}

	// clear DatabaseMeta
	m.DbId = 0
	m.Tables = make(map[int64]*TableMeta)

	m.DatabaseName2IdMap = make(map[string]int64)
	m.TableName2IdMap = make(map[string]int64)
}

func (m *Meta) ClearTable(dbName string, tableName string) {
	if m.Database != dbName {
		log.Info("dbName not match, skip clear")
		return
	}

	tableId, ok := m.TableName2IdMap[tableName]
	if !ok {
		log.Infof("table %s not found, skip clear", tableName)
		return
	}

	// remove TableMeta in DatabaseMeta
	delete(m.Tables, tableId)

	delete(m.TableName2IdMap, tableName)
}

func (m *Meta) IsPartitionDropped(partitionId int64) bool {
	panic("IsPartitionDropped is not supported, please use ThriftMeta instead")
}

func (m *Meta) IsTableDropped(partitionId int64) bool {
	panic("IsTableDropped is not supported, please use ThriftMeta instead")
}

func (m *Meta) IsIndexDropped(indexId int64) bool {
	panic("IsIndexDropped is not supported, please use ThriftMeta instead")
}
