package ccr

import (
	"database/sql"
	"fmt"
	"sort"
	"strconv"

	"github.com/selectdb/ccr_syncer/ccr/base"
	"github.com/selectdb/ccr_syncer/rpc"

	log "github.com/sirupsen/logrus"
	"github.com/tidwall/btree"
)

const (
	degree = 128
)

// All Update* functions force to update meta from fe
// TODO: reduce code, need to each level contain up level reference to get id

func fmtHostPort(host string, port int16) string {
	return fmt.Sprintf("%s:%d", host, port)
}

type DatabaseMeta struct {
	Id     int64
	Tables map[int64]*TableMeta // tableId -> table
}

type TableMeta struct {
	DatabaseMeta *DatabaseMeta
	Id           int64
	Partitions   map[int64]*PartitionMeta // partitionId -> partition
}

type PartitionMeta struct {
	TableMeta *TableMeta
	Id        int64
	Name      string
	Indexes   map[int64]*IndexMeta // indexId -> index
}

// Stringer
func (p *PartitionMeta) String() string {
	return fmt.Sprintf("PartitionMeta{(id:%d), (name:%s)}", p.Id, p.Name)
}

type IndexMeta struct {
	PartitionMeta *PartitionMeta
	Id            int64
	Name          string
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
		log.Fatal(err)
	}
	defer db.Close()

	var dbId int64
	var parsedDbname string
	discardCols := make([]sql.RawBytes, 7)
	scanArgs := []interface{}{&dbId, &parsedDbname}
	for i := range discardCols {
		scanArgs = append(scanArgs, &discardCols[i])
	}
	rows, err := db.Query("show proc '/dbs/'")
	if err != nil {
		log.Fatal(err)
		return 0, err
	}

	defer rows.Close()
	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			log.Fatal(err)
		}
		// match parsedDbname == dbname, return dbId
		if parsedDbname == dbname {
			m.DatabaseName2IdMap[dbname] = dbId
			m.DatabaseMeta.Id = dbId
			return dbId, nil
		}
	}

	if err := rows.Err(); err != nil {
		log.Fatal(err)
		return 0, err
	}

	// not found
	return 0, fmt.Errorf("%s not found dbId", dbname)
}

func (m *Meta) GetFullTableName(tableName string) string {
	dbName := m.Database
	fullTableName := dbName + "." + tableName
	return fullTableName
}

func (m *Meta) UpdateTable(tableName string) error {
	fullTableName := m.GetFullTableName(tableName)

	dbId, err := m.GetDbId()
	if err != nil {
		return err
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
		log.Fatal(err)
		return err
	}

	var tableId int64
	var parsedTableName string
	discardCols := make([]sql.RawBytes, 7)
	scanArgs := []interface{}{&tableId, &parsedTableName}
	for i := range discardCols {
		scanArgs = append(scanArgs, &discardCols[i])
	}
	query := fmt.Sprintf("show proc '/dbs/%d/'", dbId)
	rows, err := db.Query(query)
	if err != nil {
		log.Fatal(err)
		return err
	}

	defer rows.Close()
	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			log.Fatal(err)
		}
		// match parsedDbname == dbname, return dbId
		if parsedTableName == tableName {
			log.Debugf("found table:%s, tableId:%d", fullTableName, tableId)
			m.TableName2IdMap[fullTableName] = tableId
			m.Tables[tableId] = &TableMeta{
				DatabaseMeta: &m.DatabaseMeta,
				Id:           tableId,
			}
			return nil
		}
	}

	if err := rows.Err(); err != nil {
		log.Fatal(err)
		return err
	}

	// not found
	return fmt.Errorf("%s not found table", fullTableName)
}

func (m *Meta) GetTable(tableName string) (*TableMeta, error) {
	fullTableName := m.GetFullTableName(tableName)
	_, ok := m.TableName2IdMap[fullTableName]
	if !ok {
		if err := m.UpdateTable(tableName); err != nil {
			return nil, err
		}
		_, ok = m.TableName2IdMap[fullTableName]
		if !ok {
			return nil, fmt.Errorf("table %s not found", fullTableName)
		}
	}

	tableId := m.TableName2IdMap[fullTableName]
	return m.Tables[tableId], nil
}

func (m *Meta) GetTableId(tableName string) (int64, error) {
	fullTableName := m.GetFullTableName(tableName)
	if tableId, ok := m.TableName2IdMap[fullTableName]; ok {
		return tableId, nil
	}

	if err := m.UpdateTable(tableName); err != nil {
		return 0, err
	}

	if tableId, ok := m.TableName2IdMap[fullTableName]; ok {
		return tableId, nil
	} else {
		return 0, fmt.Errorf("table %s not found tableId", fullTableName)
	}
}

func (m *Meta) UpdatePartitions(tableName string) error {
	// Step 1: get dbId
	dbId, err := m.GetDbId()
	if err != nil {
		log.Error(err)
		return err
	}

	// Step 2: get tableId
	table, err := m.GetTable(tableName)
	if err != nil {
		log.Error(err)
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
		log.Error(err)
		return err
	}
	partitions := make([]*PartitionMeta, 0)
	// total columns 18
	var partitionId int64
	var partitionName string
	discardCols := make([]sql.RawBytes, 16)
	scanArgs := []interface{}{&partitionId, &partitionName}
	for i := range discardCols {
		scanArgs = append(scanArgs, &discardCols[i])
	}
	query := fmt.Sprintf("show proc '/dbs/%d/%d/partitions'", dbId, table.Id)
	log.Info(query)
	rows, err := db.Query(query)
	if err != nil {
		log.Error(err)
		return err
	}

	defer rows.Close()
	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			log.Fatal(err)
		}
		log.Printf("partitionId: %d, partitionName: %s", partitionId, partitionName)
		partition := &PartitionMeta{
			TableMeta: table,
			Id:        partitionId,
			Name:      partitionName,
		}
		partitions = append(partitions, partition)
	}

	if err := rows.Err(); err != nil {
		log.Fatal(err)
		return err
	}

	table.Partitions = make(map[int64]*PartitionMeta)
	for _, partition := range partitions {
		table.Partitions[partition.Id] = partition
	}

	return nil
}

func (m *Meta) getPartitionsWithUpdate(tableName string, depth int64) (map[int64]*PartitionMeta, error) {
	if depth >= 3 {
		return nil, fmt.Errorf("getPartitions depth >= 3")
	}

	if err := m.UpdatePartitions(tableName); err != nil {
		log.Error(err)
		return nil, err
	}

	depth++
	return m.getPartitions(tableName, depth)
}

func (m *Meta) getPartitions(tableName string, depth int64) (map[int64]*PartitionMeta, error) {
	if depth >= 3 {
		return nil, fmt.Errorf("getPartitions depth >= 3")
	}

	fullTableName := m.GetFullTableName(tableName)
	tableId, ok := m.TableName2IdMap[fullTableName]
	if !ok {
		return m.getPartitionsWithUpdate(tableName, depth)
	}

	var tableMeta *TableMeta
	tableMeta, ok = m.Tables[tableId]
	if !ok {
		return m.getPartitionsWithUpdate(tableName, depth)
	}
	if len(tableMeta.Partitions) == 0 {
		return m.getPartitionsWithUpdate(tableName, depth)
	}

	return tableMeta.Partitions, nil
}

func (m *Meta) GetPartitions(tableName string) (map[int64]*PartitionMeta, error) {
	return m.getPartitions(tableName, 0)
}

func (m *Meta) GetPartitionIds(tableName string) ([]int64, error) {
	// TODO: optimize performance, cache it
	partitions, err := m.GetPartitions(tableName)
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

func (m *Meta) GetPartitionName(tableName string, partitionId int64) (string, error) {
	partitions, err := m.GetPartitions(tableName)
	if err != nil {
		return "", err
	}

	partition, ok := partitions[partitionId]
	if !ok {
		return "", fmt.Errorf("partitionId %d not found", partitionId)
	}

	return partition.Name, nil
}

func (m *Meta) GetPartitionIdByName(tableName string, partitionName string) (int64, error) {
	// TODO: optimize performance
	partitions, err := m.GetPartitions(tableName)
	if err != nil {
		return 0, err
	}

	for partitionId, partition := range partitions {
		if partition.Name == partitionName {
			return partitionId, nil
		}
	}

	return 0, fmt.Errorf("partitionName %s not found", partitionName)
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
		log.Fatal(err)
		return err
	}

	backends := make([]*base.Backend, 0)
	query := "show proc '/backends'"
	log.Info(query)
	rows, err := db.Query(query)
	if err != nil {
		log.Fatal(err)
		return err
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
			log.Fatal(err)
			return err
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
		log.Error(err)
		return nil, err
	}

	return m.GetBackends()
}

func (m *Meta) GetBackendMap() (map[int64]*base.Backend, error) {
	if len(m.Backends) > 0 {
		return m.Backends, nil
	}

	if err := m.UpdateBackends(); err != nil {
		log.Error(err)
		return nil, err
	}

	return m.GetBackendMap()
}

func (m *Meta) GetBackendId(host string, portStr string) (int64, error) {
	// convert port from string to int16
	port, err := strconv.ParseInt(portStr, 10, 16)
	if err != nil {
		log.Fatal(err)
		return 0, err
	}
	hostPort := fmtHostPort(host, int16(port))
	if backendId, ok := m.BackendHostPort2IdMap[hostPort]; ok {
		return backendId, nil
	}
	if err := m.UpdateBackends(); err != nil {
		log.Fatal(err)
		return 0, err
	}
	if backendId, ok := m.BackendHostPort2IdMap[hostPort]; ok {
		return backendId, nil
	}

	return 0, fmt.Errorf("hostPort: %s not found", hostPort)
}

func (m *Meta) UpdateIndexes(tableName string, partitionId int64) error {
	// TODO: Optimize performance
	// Step 1: get dbId
	dbId, err := m.GetDbId()
	if err != nil {
		log.Error(err)
		return err
	}

	// Step 2: get tableId
	table, err := m.GetTable(tableName)
	if err != nil {
		log.Error(err)
		return err
	}

	// Step 3: get partitions
	parititions, err := m.GetPartitions(tableName)
	if err != nil {
		log.Fatal(err)
		return err
	}

	partition, ok := parititions[partitionId]
	if !ok {
		return fmt.Errorf("partitionId: %d not found", partitionId)
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
		log.Error(err)
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
	log.Info(query)
	rows, err := db.Query(query)
	if err != nil {
		log.Error(err)
		return err
	}

	defer rows.Close()
	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			log.Fatal(err)
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
		log.Fatal(err)
		return err
	}

	partition.Indexes = make(map[int64]*IndexMeta)
	for _, index := range indexes {
		partition.Indexes[index.Id] = index
	}

	return nil
}

func (m *Meta) getIndexes(tableName string, partitionId int64, hasUpdate bool) (map[int64]*IndexMeta, error) {
	parititions, err := m.GetPartitions(tableName)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}

	partition, ok := parititions[partitionId]
	if !ok || len(partition.Indexes) == 0 {
		if hasUpdate {
			return nil, fmt.Errorf("partitionId: %d not found", partitionId)
		}

		err = m.UpdateIndexes(tableName, partitionId)
		if err != nil {
			log.Fatal(err)
			return nil, err
		}
		return m.getIndexes(tableName, partitionId, true)
	}

	return partition.Indexes, nil
}

func (m *Meta) GetIndexes(tableName string, partitionId int64) (map[int64]*IndexMeta, error) {
	return m.getIndexes(tableName, partitionId, false)
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
		log.Error(err)
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
	log.Info(query)
	rows, err := db.Query(query)
	if err != nil {
		log.Fatal(err)
		return err
	}

	defer rows.Close()
	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			log.Fatal(err)
		}
		replica := &ReplicaMeta{
			Id:        replicaId,
			TabletId:  tabletId,
			BackendId: backendId,
		}
		replicas = append(replicas, replica)
	}

	if err := rows.Err(); err != nil {
		log.Fatal(err)
		return err
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

func (m *Meta) UpdateReplicas(tableName string, partitionId int64) error {
	indexes, err := m.GetIndexes(tableName, partitionId)
	if err != nil {
		log.Fatal(err)
		return err
	}

	if len(indexes) == 0 {
		return fmt.Errorf("indexes is empty")
	}

	// TODO: Update index as much as possible, record error
	for _, index := range indexes {
		if err := m.updateReplica(index); err != nil {
			return err
		}
	}
	return nil
}

func (m *Meta) GetReplicas(tableName string, partitionId int64) (*btree.Map[int64, *ReplicaMeta], error) {
	indexes, err := m.GetIndexes(tableName, partitionId)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}

	if len(indexes) == 0 {
		return nil, fmt.Errorf("indexes is empty")
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

func (m *Meta) GetTablets(tableName string, partitionId int64) (*btree.Map[int64, *TabletMeta], error) {
	_, err := m.GetReplicas(tableName, partitionId)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}

	indexes, err := m.GetIndexes(tableName, partitionId)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}

	if len(indexes) == 0 {
		return nil, fmt.Errorf("indexes is empty")
	}

	// fast path, no rollup
	if len(indexes) == 1 {
		var indexId int64
		for id := range indexes {
			indexId = id
			break
		}

		index := indexes[indexId]
		return index.TabletMetas, nil
	}

	// slow path, rollup
	tablets := btree.NewMap[int64, *TabletMeta](degree)
	for _, index := range indexes {
		for _, tablet := range index.TabletMetas.Values() {
			log.Infof("tablet: %d, replica len: %d", tablet.Id, tablet.ReplicaMetas.Len()) // TODO: remove it
			tablets.Set(tablet.Id, tablet)
		}
	}

	return tablets, nil
}

func (m *Meta) GetTabletList(tableName string, partitionId int64) ([]*TabletMeta, error) {
	tablets, err := m.GetTablets(tableName, partitionId)
	if err != nil {
		return nil, err
	}

	if tablets.Len() == 0 {
		return nil, fmt.Errorf("tablets is empty")
	}

	list := make([]*TabletMeta, 0, tablets.Len())
	list = append(list, tablets.Values()...)

	return list, nil
}

func (m *Meta) UpdateToken() error {
	spec := &m.Spec

	rpc, err := rpc.NewThriftRpc(spec)
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
		log.Error(err)
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
		log.Fatal(err)
		return "", err
	}

	var _dbName string
	var tableName string
	var _dbId int64
	sql := fmt.Sprintf("show table %d", tableId)
	rows, err := db.Query(sql)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	for rows.Next() {
		if err := rows.Scan(&_dbName, &tableName, &_dbId); err != nil {
			return "", err
		}
	}

	if err := rows.Err(); err != nil {
		return "", err
	}

	return tableName, nil
}

// Exec sql
func (m *Meta) Exec(sql string) error {
	db, err := m.Connect()
	if err != nil {
		return err
	}

	_, err = db.Exec(sql)
	return err
}
