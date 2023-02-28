package ccr

import (
	"database/sql"
	"fmt"
	"strconv"

	"github.com/selectdb/ccr_syncer/ccr/base"
	"github.com/selectdb/ccr_syncer/rpc"

	log "github.com/sirupsen/logrus"
)

// All Update* functions force to update meta from fe

func fmtHostPort(host string, port int16) string {
	return fmt.Sprintf("%s:%d", host, port)
}

type DatabaseMeta struct {
	Id     int64
	Tables map[int64]*TableMeta // tableId -> table
}

type TableMeta struct {
	Id         int64
	Partitions map[int64]*PartitionMeta // partitionId -> partition
}

type PartitionMeta struct {
	Id      int64
	Name    string
	Indexes map[int64]*IndexMeta // indexId -> index
}

// Stringer
func (p *PartitionMeta) String() string {
	return fmt.Sprintf("PartitionMeta{(id:%d), (name:%s)}", p.Id, p.Name)
}

type IndexMeta struct {
	Id          int64
	Name        string
	TabletMetas map[int64]TabletMeta // tabletId -> tablet
}

type TabletMeta struct {
	Id           int64
	ReplicaMetas map[int64]ReplicaMeta // replicaId -> replica
}

type ReplicaMeta struct {
	Id        int64
	BackendId int64
}

// All op is not thread safety
// Meta
type Meta struct {
	base.Spec
	DatabaseMeta
	token                 string
	Backends              map[int64]base.Backend // backendId -> backend
	DatabaseName2IdMap    map[string]int64
	TableName2IdMap       map[string]int64
	BackendHostPort2IdMap map[string]int64
}

func NewMeta(tableSpec *base.Spec) *Meta {
	return &Meta{
		Spec:                  *tableSpec,
		DatabaseMeta:          DatabaseMeta{},
		Backends:              make(map[int64]base.Backend),
		DatabaseName2IdMap:    make(map[string]int64),
		TableName2IdMap:       make(map[string]int64),
		BackendHostPort2IdMap: make(map[string]int64),
	}
}

func (m *Meta) GetDbId(dbName string) (int64, error) {
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
	dbName := m.Database
	fullTableName := m.GetFullTableName(tableName)

	dbId, err := m.GetDbId(dbName)
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
			m.TableName2IdMap[fullTableName] = tableId
			if m.Tables == nil {
				m.Tables = make(map[int64]*TableMeta)
			}
			m.Tables[tableId] = &TableMeta{
				Id: tableId,
			}
			return nil
		}
	}

	if err := rows.Err(); err != nil {
		log.Fatal(err)
		return err
	}

	// not found
	return fmt.Errorf("%s:%s not found table", dbName, tableName)
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
	dbName := m.Database

	// Step 1: get dbId
	dbId, err := m.GetDbId(dbName)
	if err != nil {
		log.Error(err)
		return err
	}

	// Step 2: get tableId
	tableId, err := m.GetTableId(tableName)
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
	partitions := make([]PartitionMeta, 0)
	// total rows 18
	var partitionId int64
	var partitionName string
	discardCols := make([]sql.RawBytes, 16)
	scanArgs := []interface{}{&partitionId, &partitionName}
	for i := range discardCols {
		scanArgs = append(scanArgs, &discardCols[i])
	}
	query := fmt.Sprintf("show proc '/dbs/%d/%d/partitions'", dbId, tableId)
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
		log.Printf("partitionId: %d, partitionName: %s", partitionId, partitionName)
		partition := PartitionMeta{
			Id:   partitionId,
			Name: partitionName,
		}
		partitions = append(partitions, partition)
	}

	if err := rows.Err(); err != nil {
		log.Fatal(err)
		return err
	}

	return nil
}

func (m *Meta) getPartitionsWithUpdate(tableName string, depth int64) ([]*PartitionMeta, error) {
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

func (m *Meta) getPartitions(tableName string, depth int64) ([]*PartitionMeta, error) {
	if depth >= 3 {
		return nil, fmt.Errorf("getPartitions depth >= 3")
	}

	tableId, ok := m.TableName2IdMap[tableName]
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

	partitions := make([]*PartitionMeta, 0)
	for _, partition := range tableMeta.Partitions {
		partitions = append(partitions, partition)
	}
	return partitions, nil
}

func (m *Meta) GetPartitions(tableName string) ([]*PartitionMeta, error) {
	return m.getPartitions(tableName, 0)
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

	backends := make([]base.Backend, 0)
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
		backends = append(backends, backend)
	}

	for _, backend := range backends {
		m.Backends[backend.Id] = backend

		hostPort := fmtHostPort(backend.Host, backend.BePort)
		m.BackendHostPort2IdMap[hostPort] = backend.Id
	}

	return nil
}

func (m *Meta) GetBackends() ([]base.Backend, error) {
	if len(m.Backends) > 0 {
		backends := make([]base.Backend, len(m.Backends))
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

// func (m *Meta) UpdateReplicas() error {
// 	// Step 1: get dbId
// 	dbId, err := m.GetDbId(dbName)
// 	if err != nil {
// 		log.Fatal(err)
// 		return nil, err
// 	}
// }

// func (m *Meta) GetReplicas(tableName string) ([]ReplicaMeta, error) {
// }

func (m *Meta) GetMasterToken() (string, error) {
	spec := &m.Spec

	rpc, err := rpc.NewThriftRpc(spec)
	if err != nil {
		return "", err
	}

	if token, err := rpc.GetMasterToken(spec); err != nil {
		return "", err
	} else {
		return token, nil
	}
}
