package base

import (
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/selectdb/ccr_syncer/pkg/ccr/record"
	"github.com/selectdb/ccr_syncer/pkg/utils"
	"github.com/selectdb/ccr_syncer/pkg/xerror"

	log "github.com/sirupsen/logrus"
)

var ErrRestoreSignatureNotMatched = xerror.NewWithoutStack(xerror.Normal, "The signature is not matched, the table already exist but with different schema")

const (
	BACKUP_CHECK_DURATION  = time.Second * 3
	RESTORE_CHECK_DURATION = time.Second * 3
	MAX_CHECK_RETRY_TIMES  = 86400 // 3 day
	SIGNATURE_NOT_MATCHED  = "already exist but with different schema"
)

type BackupState int

const (
	BackupStateUnknown   BackupState = iota
	BackupStatePending   BackupState = iota
	BackupStateFinished  BackupState = iota
	BackupStateCancelled BackupState = iota
)

func (s BackupState) String() string {
	switch s {
	case BackupStateUnknown:
		return "unknown"
	case BackupStatePending:
		return "pending"
	case BackupStateFinished:
		return "finished"
	case BackupStateCancelled:
		return "cancelled"
	default:
		return "unknown"
	}
}

func ParseBackupState(state string) BackupState {
	switch state {
	case "PENDING":
		return BackupStatePending
	case "FINISHED":
		return BackupStateFinished
	case "CANCELLED":
		return BackupStateCancelled
	default:
		return BackupStateUnknown
	}
}

type RestoreState int

const (
	RestoreStateUnknown   RestoreState = iota
	RestoreStatePending   RestoreState = iota
	RestoreStateFinished  RestoreState = iota
	RestoreStateCancelled RestoreState = iota
)

func (s RestoreState) String() string {
	switch s {
	case RestoreStateUnknown:
		return "unknown"
	case RestoreStatePending:
		return "pending"
	case RestoreStateFinished:
		return "finished"
	case RestoreStateCancelled:
		return "cancelled"
	default:
		return "unknown"
	}
}

func _parseRestoreState(state string) RestoreState {
	switch state {
	case "PENDING":
		return RestoreStatePending
	case "FINISHED":
		return RestoreStateFinished
	case "CANCELLED":
		return RestoreStateCancelled
	default:
		return RestoreStateUnknown
	}
}

type RestoreInfo struct {
	State          RestoreState
	StateStr       string
	Label          string
	Status         string
	Timestamp      string
	ReplicationNum int64
	CreateTime     string // 2024-10-22 06:29:27
}

func parseRestoreInfo(parser *utils.RowParser) (*RestoreInfo, error) {
	restoreStateStr, err := parser.GetString("State")
	if err != nil {
		return nil, xerror.Wrap(err, xerror.Normal, "parse restore State failed")
	}

	label, err := parser.GetString("Label")
	if err != nil {
		return nil, xerror.Wrap(err, xerror.Normal, "parse restore Label failed")
	}

	restoreStatus, err := parser.GetString("Status")
	if err != nil {
		return nil, xerror.Wrap(err, xerror.Normal, "parse restore Status failed")
	}

	timestamp, err := parser.GetString("Timestamp")
	if err != nil {
		return nil, xerror.Wrap(err, xerror.Normal, "parse restore Timestamp failed")
	}

	replicationNum, err := parser.GetInt64("ReplicationNum")
	if err != nil {
		return nil, xerror.Wrap(err, xerror.Normal, "parse restore ReplicationNum failed")
	}

	createTime, err := parser.GetString("CreateTime")
	if err != nil {
		return nil, xerror.Wrap(err, xerror.Normal, "parse restore CreateTime failed")
	}

	info := &RestoreInfo{
		State:          _parseRestoreState(restoreStateStr),
		StateStr:       restoreStateStr,
		Label:          label,
		Status:         restoreStatus,
		Timestamp:      timestamp,
		ReplicationNum: replicationNum,
		CreateTime:     createTime,
	}
	return info, nil
}

type BackupInfo struct {
	State        BackupState
	StateStr     string
	SnapshotName string
	CreateTime   string // 2024-10-22 06:27:06
}

func parseBackupInfo(parser *utils.RowParser) (*BackupInfo, error) {
	stateStr, err := parser.GetString("State")
	if err != nil {
		return nil, xerror.Wrap(err, xerror.Normal, "parse backup State failed")
	}

	snapshotName, err := parser.GetString("SnapshotName")
	if err != nil {
		return nil, xerror.Wrap(err, xerror.Normal, "parse backup SnapshotName failed")
	}

	createTime, err := parser.GetString("CreateTime")
	if err != nil {
		return nil, xerror.Wrap(err, xerror.Normal, "parse backup CreateTime failed")
	}

	info := &BackupInfo{
		State:        ParseBackupState(stateStr),
		StateStr:     stateStr,
		SnapshotName: snapshotName,
		CreateTime:   createTime,
	}
	return info, nil
}

type Frontend struct {
	Host       string `json:"host"`
	Port       string `json:"port"`
	ThriftPort string `json:"thrift_port"`
	IsMaster   bool   `json:"is_master"`
}

func (f *Frontend) String() string {
	return fmt.Sprintf("host: %s, port: %s, thrift_port: %s, is_master: %v", f.Host, f.Port, f.ThriftPort, f.IsMaster)
}

type Spec struct {
	// embed Frontend as current master frontend
	Frontend
	Frontends []Frontend `json:"frontends"`

	User     string `json:"user"`
	Password string `json:"password"`
	Cluster  string `json:"cluster"`

	Database string `json:"database"`
	DbId     int64  `json:"db_id"`
	Table    string `json:"table"`
	TableId  int64  `json:"table_id"`

	observers []utils.Observer[SpecEvent]
}

func (s *Spec) String() string {
	return fmt.Sprintf("host: %s, port: %s, thrift_port: %s, user: %s, cluster: %s, database: %s, database id: %d, table: %s, table id: %d",
		s.Host, s.Port, s.ThriftPort, s.User, s.Cluster, s.Database, s.DbId, s.Table, s.TableId)
}

// valid table spec
func (s *Spec) Valid() error {
	if s.Host == "" {
		return xerror.Errorf(xerror.Normal, "host is empty")
	}

	// convert port to int16 and check port in range [0, 65535]
	port, err := strconv.ParseUint(s.Port, 10, 16)
	if err != nil {
		return xerror.Errorf(xerror.Normal, "port is invalid: %s", s.Port)
	}
	if port > 65535 {
		return xerror.Errorf(xerror.Normal, "port is invalid: %s", s.Port)
	}

	// convert thrift port to int16 and check port in range [0, 65535]
	thriftPort, err := strconv.ParseUint(s.ThriftPort, 10, 16)
	if err != nil {
		return xerror.Errorf(xerror.Normal, "thrift_port is invalid: %s", s.ThriftPort)
	}
	if thriftPort > 65535 {
		return xerror.Errorf(xerror.Normal, "thrift_port is invalid: %s", s.ThriftPort)
	}

	if s.User == "" {
		return xerror.Errorf(xerror.Normal, "user is empty")
	}

	if s.Database == "" {
		return xerror.Errorf(xerror.Normal, "database is empty")
	}

	return nil
}

func (s *Spec) connect(dsn string) (*sql.DB, error) {
	return GetMysqlDB(dsn)
}

// create mysql connection from spec
func (s *Spec) Connect() (*sql.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/", s.User, s.Password, s.Host, s.Port)
	return s.connect(dsn)
}

func (s *Spec) ConnectDB() (*sql.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", s.User, s.Password, s.Host, s.Port, s.Database)
	return s.connect(dsn)
}

// mysql> show create database ccr;
// +----------+----------------------------------------------------------------------------------------------+
// | Database | Create Database                                                                              |
// +----------+----------------------------------------------------------------------------------------------+
// | ccr      | CREATE DATABASE `ccr`
// PROPERTIES (
// "binlog.enable" = "true",
// "binlog.ttl_seconds" = "3600"
// ) |
// +----------+----------------------------------------------------------------------------------------------+
func (s *Spec) IsDatabaseEnableBinlog() (bool, error) {
	log.Infof("check database %s enable binlog", s.Database)

	db, err := s.Connect()
	if err != nil {
		return false, err
	}

	var createDBString string
	query := fmt.Sprintf("SHOW CREATE DATABASE %s", utils.FormatKeywordName(s.Database))
	rows, err := db.Query(query)
	if err != nil {
		return false, xerror.Wrap(err, xerror.Normal, query)
	}
	defer rows.Close()

	for rows.Next() {
		rowParser := utils.NewRowParser()
		if err := rowParser.Parse(rows); err != nil {
			return false, xerror.Wrap(err, xerror.Normal, query)
		}
		createDBString, err = rowParser.GetString("Create Database")
		if err != nil {
			return false, xerror.Wrap(err, xerror.Normal, query)
		}
	}

	if err := rows.Err(); err != nil {
		return false, xerror.Wrap(err, xerror.Normal, query)
	}

	log.Infof("database %s create string: %s", s.Database, createDBString)

	// check "binlog.enable" = "true" in create database string
	binlogEnableString := `"binlog.enable" = "true"`
	return strings.Contains(createDBString, binlogEnableString), nil
}

func (s *Spec) IsTableEnableBinlog() (bool, error) {
	log.Infof("check table %s.%s enable binlog", s.Database, s.Table)

	db, err := s.Connect()
	if err != nil {
		return false, err
	}

	var createTableString string
	query := fmt.Sprintf("SHOW CREATE TABLE %s.%s", utils.FormatKeywordName(s.Database), utils.FormatKeywordName(s.Table))
	rows, err := db.Query(query)
	if err != nil {
		return false, xerror.Wrap(err, xerror.Normal, query)
	}
	defer rows.Close()

	for rows.Next() {
		rowParser := utils.NewRowParser()
		if err := rowParser.Parse(rows); err != nil {
			return false, xerror.Wrap(err, xerror.Normal, query)
		}
		createTableString, err = rowParser.GetString("Create Table")
		if err != nil {
			return false, xerror.Wrap(err, xerror.Normal, query)
		}
	}

	if err := rows.Err(); err != nil {
		return false, xerror.Wrap(err, xerror.Normal, query)
	}

	log.Tracef("table %s.%s create string: %s", s.Database, s.Table, createTableString)

	// check "binlog.enable" = "true" in create table string
	binlogEnableString := `"binlog.enable" = "true"`
	return strings.Contains(createTableString, binlogEnableString), nil
}

func (s *Spec) GetAllTables() ([]string, error) {
	log.Debugf("get all tables in database %s", s.Database)

	db, err := s.ConnectDB()
	if err != nil {
		return nil, err
	}

	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		return nil, xerror.Wrap(err, xerror.Normal, "show tables failed")
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		rowParser := utils.NewRowParser()
		if err := rowParser.Parse(rows); err != nil {
			return nil, xerror.Wrap(err, xerror.Normal, "SHOW TABLES")
		}
		table, err := rowParser.GetString(fmt.Sprintf("Tables_in_%s", s.Database))
		if err != nil {
			return nil, xerror.Wrap(err, xerror.Normal, "SHOW TABLES")
		}
		tables = append(tables, table)
	}
	return tables, nil
}

func (s *Spec) queryResult(querySQL string, queryColumn string, errMsg string) ([]string, error) {
	db, err := s.ConnectDB()
	if err != nil {
		return nil, err
	}

	rows, err := db.Query(querySQL)
	if err != nil {
		return nil, xerror.Wrap(err, xerror.Normal, querySQL+" failed")
	}
	defer rows.Close()

	var results []string
	for rows.Next() {
		rowParser := utils.NewRowParser()
		if err := rowParser.Parse(rows); err != nil {
			return nil, xerror.Wrap(err, xerror.Normal, errMsg)
		}
		result, err := rowParser.GetString(queryColumn)
		if err != nil {
			return nil, xerror.Wrap(err, xerror.Normal, errMsg)
		}
		results = append(results, result)
	}

	return results, nil
}

func (s *Spec) GetAllViewsFromTable(tableName string) ([]string, error) {
	log.Debugf("get all view from table %s", tableName)

	var results []string
	// first, query information_schema.tables with table_schema and table_type, get all views' name
	querySql := fmt.Sprintf("SELECT table_name FROM information_schema.tables WHERE table_schema = '%s' AND  table_type = 'VIEW'", s.Database)
	viewsFromQuery, err := s.queryResult(querySql, "table_name", "QUERY VIEWS")
	if err != nil {
		return nil, xerror.Wrap(err, xerror.Normal, "query views from information schema failed")
	}

	// then query view's create sql, if create sql contains tableName, this view is wanted
	viewRegex := regexp.MustCompile("(`internal`\\.`\\w+`|`default_cluster:\\w+`)\\.`" + strings.TrimSpace(tableName) + "`")
	for _, eachViewName := range viewsFromQuery {
		showCreateViewSql := fmt.Sprintf("SHOW CREATE VIEW %s", eachViewName)
		createViewSqlList, err := s.queryResult(showCreateViewSql, "Create View", "SHOW CREATE VIEW")
		if err != nil {
			return nil, xerror.Wrap(err, xerror.Normal, "show create view failed")
		}

		// a view has only one create sql, so use createViewSqlList[0] as the only sql
		if len(createViewSqlList) > 0 {
			found := viewRegex.MatchString(createViewSqlList[0])
			if found {
				results = append(results, eachViewName)
			}
		}
	}

	log.Debugf("get view result is %s", results)
	return results, nil
}

func (s *Spec) RenameTable(destTableName string, renameTable *record.RenameTable) error {
	// rename table may be 'rename table', 'rename rollup', 'rename partition'
	var sql string
	// ALTER TABLE table1 RENAME table2;
	if renameTable.NewTableName != "" && renameTable.OldTableName != "" {
		sql = fmt.Sprintf("ALTER TABLE %s RENAME %s", renameTable.OldTableName, renameTable.NewTableName)
	}

	// ALTER TABLE example_table RENAME ROLLUP rollup1 rollup2;
	// if rename rollup, table name is unchanged
	if renameTable.NewRollupName != "" && renameTable.OldRollupName != "" {
		sql = fmt.Sprintf("ALTER TABLE %s RENAME ROLLUP %s %s", destTableName, renameTable.OldRollupName, renameTable.NewRollupName)
	}

	// ALTER TABLE example_table RENAME PARTITION p1 p2;
	// if rename partition, table name is unchanged
	if renameTable.NewParitionName != "" && renameTable.OldParitionName != "" {
		sql = fmt.Sprintf("ALTER TABLE %s RENAME PARTITION %s %s;", destTableName, renameTable.OldParitionName, renameTable.NewParitionName)
	}
	if sql == "" {
		return xerror.Errorf(xerror.Normal, "rename sql is empty")
	}

	log.Infof("renam table sql: %s", sql)
	return s.DbExec(sql)
}

func (s *Spec) dropTable(table string, force bool) error {
	log.Infof("drop table %s.%s", s.Database, table)

	db, err := s.Connect()
	if err != nil {
		return err
	}

	suffix := ""
	if force {
		suffix = "FORCE"
	}
	sql := fmt.Sprintf("DROP TABLE %s.%s %s", utils.FormatKeywordName(s.Database), utils.FormatKeywordName(table), suffix)
	_, err = db.Exec(sql)
	if err != nil {
		return xerror.Wrapf(err, xerror.Normal, "drop table %s.%s failed, sql: %s", s.Database, table, sql)
	}
	return nil
}

func (s *Spec) ClearDB() error {
	log.Infof("clear database %s", s.Database)

	db, err := s.Connect()
	if err != nil {
		return err
	}

	sql := fmt.Sprintf("DROP DATABASE %s", utils.FormatKeywordName(s.Database))
	_, err = db.Exec(sql)
	if err != nil {
		return xerror.Wrapf(err, xerror.Normal, "drop database %s failed", s.Database)
	}

	if _, err = db.Exec("CREATE DATABASE " + utils.FormatKeywordName(s.Database)); err != nil {
		return xerror.Wrapf(err, xerror.Normal, "create database %s failed", s.Database)
	}
	return nil
}

func (s *Spec) CreateDatabase() error {
	log.Debug("create database")

	db, err := s.Connect()
	if err != nil {
		return nil
	}

	if _, err = db.Exec("CREATE DATABASE IF NOT EXISTS " + utils.FormatKeywordName(s.Database)); err != nil {
		return xerror.Wrapf(err, xerror.Normal, "create database %s failed", s.Database)
	}
	return nil
}

func (s *Spec) CreateTableOrView(createTable *record.CreateTable, srcDatabase string) error {
	//	Creating table will only occur when sync db.
	//	When create view, the db name of sql is source db name, we should use dest db name to create view
	createSql := createTable.Sql
	viewRegex := regexp.MustCompile(`(?i)^CREATE(\s+)VIEW`)
	isCreateView := viewRegex.MatchString(createSql)
	if isCreateView {
		log.Debugf("create view, use dest db name to replace source db name")

		// replace `internal`.`source_db_name`. or `default_cluster:source_db_name`. to `internal`.`dest_db_name`.
		originalNameNewStyle := "`internal`.`" + strings.TrimSpace(srcDatabase) + "`."
		originalNameOldStyle := "`default_cluster:" + strings.TrimSpace(srcDatabase) + "`." // for Doris 2.0.x
		replaceName := "`internal`.`" + strings.TrimSpace(s.Database) + "`."
		createTable.Sql = strings.ReplaceAll(
			strings.ReplaceAll(createTable.Sql, originalNameNewStyle, replaceName), originalNameOldStyle, replaceName)
		log.Debugf("original create view sql is %s, after replace, now sql is %s", createSql, createTable.Sql)
	}

	sql := createTable.Sql
	log.Infof("create table or view sql: %s", sql)
	// HACK: for drop table
	return s.DbExec(sql)
}

func (s *Spec) CheckDatabaseExists() (bool, error) {
	log.Debugf("check database exist by spec: %s", s.String())
	db, err := s.Connect()
	if err != nil {
		return false, err
	}

	sql := fmt.Sprintf("SHOW DATABASES LIKE '%s'", s.Database)
	rows, err := db.Query(sql)
	if err != nil {
		return false, xerror.Wrapf(err, xerror.Normal, "show databases failed, sql: %s", sql)
	}
	defer rows.Close()

	var database string
	for rows.Next() {
		rowParser := utils.NewRowParser()
		if err := rowParser.Parse(rows); err != nil {
			return false, xerror.Wrap(err, xerror.Normal, sql)
		}
		database, err = rowParser.GetString("Database")
		if err != nil {
			return false, xerror.Wrap(err, xerror.Normal, sql)
		}
	}

	if err := rows.Err(); err != nil {
		return false, xerror.Wrapf(err, xerror.Normal, "scan database name failed, sql: %s", sql)
	}

	return database != "", nil
}

// check table exits in database dir by spec
func (s *Spec) CheckTableExists() (bool, error) {
	log.Debugf("check table exist by spec: %s", s.String())

	return s.CheckTableExistsByName(s.Table)
}

// check table exists in database dir by the specified table name.
func (s *Spec) CheckTableExistsByName(tableName string) (bool, error) {
	db, err := s.Connect()
	if err != nil {
		return false, err
	}

	sql := fmt.Sprintf("SHOW TABLES FROM %s LIKE '%s'", utils.FormatKeywordName(s.Database), tableName)
	rows, err := db.Query(sql)
	if err != nil {
		return false, xerror.Wrapf(err, xerror.Normal, "show tables failed, sql: %s", sql)
	}
	defer rows.Close()

	var table string
	for rows.Next() {
		rowParser := utils.NewRowParser()
		if err := rowParser.Parse(rows); err != nil {
			return false, xerror.Wrap(err, xerror.Normal, sql)
		}
		table, err = rowParser.GetString(fmt.Sprintf("Tables_in_%s", s.Database))
		if err != nil {
			return false, xerror.Wrap(err, xerror.Normal, sql)
		}
	}
	if err := rows.Err(); err != nil {
		return false, xerror.Wrapf(err, xerror.Normal, "scan table name failed, sql: %s", sql)
	}

	return table != "", nil
}

// mysql> BACKUP SNAPSHOT ccr.snapshot_20230605 TO `__keep_on_local__` ON (      src_1 ) PROPERTIES ("type" = "full");
func (s *Spec) CreateSnapshotAndWaitForDone(tables []string) (string, error) {
	if tables == nil {
		tables = make([]string, 0)
	}
	if len(tables) == 0 {
		tables = append(tables, s.Table)
	}

	var snapshotName string
	var tableRefs string
	if len(tables) == 1 {
		// snapshot name format "ccrs_${table}_${timestamp}"
		// table refs = table
		snapshotName = fmt.Sprintf("ccrs_%s_%s_%d", s.Database, s.Table, time.Now().Unix())
		tableRefs = utils.FormatKeywordName(tables[0])
	} else {
		// snapshot name format "ccrs_${db}_${timestamp}"
		// table refs = tables.join(", ")
		snapshotName = fmt.Sprintf("ccrs_%s_%d", s.Database, time.Now().Unix())
		tableRefs = "`" + strings.Join(tables, "`,`") + "`"
	}

	// means source is a empty db, table number is 0
	if tableRefs == "``" {
		return "", xerror.Errorf(xerror.Normal, "source db is empty! you should have at least one table")
	}

	log.Infof("create snapshot %s.%s", s.Database, snapshotName)

	db, err := s.Connect()
	if err != nil {
		return "", err
	}

	backupSnapshotSql := fmt.Sprintf("BACKUP SNAPSHOT %s.%s TO `__keep_on_local__` ON ( %s ) PROPERTIES (\"type\" = \"full\")", utils.FormatKeywordName(s.Database), utils.FormatKeywordName(snapshotName), tableRefs)
	log.Debugf("backup snapshot sql: %s", backupSnapshotSql)
	_, err = db.Exec(backupSnapshotSql)
	if err != nil {
		return "", xerror.Wrapf(err, xerror.Normal, "backup snapshot %s failed, sql: %s", snapshotName, backupSnapshotSql)
	}

	backupFinished, err := s.CheckBackupFinished(snapshotName)
	if err != nil {
		return "", err
	}
	if !backupFinished {
		err = xerror.Errorf(xerror.Normal, "check backup state timeout, max try times: %d, sql: %s", MAX_CHECK_RETRY_TIMES, backupSnapshotSql)
		return "", err
	}

	return snapshotName, nil
}

// mysql> BACKUP SNAPSHOT ccr.snapshot_20230605 TO `__keep_on_local__` ON (src_1 PARTITION (`p1`)) PROPERTIES ("type" = "full");
func (s *Spec) CreatePartialSnapshotAndWaitForDone(table string, partitions []string) (string, error) {
	if len(table) == 0 {
		return "", xerror.Errorf(xerror.Normal, "source db is empty! you should have at least one table")
	}

	// snapshot name format "ccrp_${table}_${timestamp}"
	// table refs = table
	snapshotName := fmt.Sprintf("ccrp_%s_%s_%d", s.Database, s.Table, time.Now().Unix())
	tableRef := utils.FormatKeywordName(table)

	log.Infof("create partial snapshot %s.%s", s.Database, snapshotName)

	db, err := s.Connect()
	if err != nil {
		return "", err
	}

	partitionRefs := ""
	if len(partitions) > 0 {
		partitionRefs = " PARTITION (`" + strings.Join(partitions, "`,`") + "`)"
	}
	backupSnapshotSql := fmt.Sprintf(
		"BACKUP SNAPSHOT %s.%s TO `__keep_on_local__` ON (%s%s) PROPERTIES (\"type\" = \"full\")",
		utils.FormatKeywordName(s.Database), snapshotName, tableRef, partitionRefs)
	log.Debugf("backup partial snapshot sql: %s", backupSnapshotSql)
	_, err = db.Exec(backupSnapshotSql)
	if err != nil {
		return "", xerror.Wrapf(err, xerror.Normal, "backup partial snapshot %s failed, sql: %s", snapshotName, backupSnapshotSql)
	}

	backupFinished, err := s.CheckBackupFinished(snapshotName)
	if err != nil {
		return "", err
	}
	if !backupFinished {
		err = xerror.Errorf(xerror.Normal, "check backup state timeout, max try times: %d, sql: %s", MAX_CHECK_RETRY_TIMES, backupSnapshotSql)
		return "", err
	}

	return snapshotName, nil
}

// TODO: Add TaskErrMsg
func (s *Spec) checkBackupFinished(snapshotName string) (BackupState, error) {
	log.Debugf("check backup state of snapshot %s", snapshotName)

	db, err := s.Connect()
	if err != nil {
		return BackupStateUnknown, err
	}

	sql := fmt.Sprintf("SHOW BACKUP FROM %s WHERE SnapshotName = \"%s\"", utils.FormatKeywordName(s.Database), snapshotName)
	log.Debugf("check backup state sql: %s", sql)
	rows, err := db.Query(sql)
	if err != nil {
		return BackupStateUnknown, xerror.Wrapf(err, xerror.Normal, "show backup failed, sql: %s", sql)
	}
	defer rows.Close()

	if rows.Next() {
		rowParser := utils.NewRowParser()
		if err := rowParser.Parse(rows); err != nil {
			return BackupStateUnknown, xerror.Wrap(err, xerror.Normal, sql)
		}

		info, err := parseBackupInfo(rowParser)
		if err != nil {
			return BackupStateUnknown, xerror.Wrap(err, xerror.Normal, sql)
		}

		log.Infof("check snapshot %s backup state: [%v]", snapshotName, info.StateStr)
		return info.State, nil
	}
	return BackupStateUnknown, xerror.Errorf(xerror.Normal, "no backup state found, sql: %s", sql)
}

func (s *Spec) CheckBackupFinished(snapshotName string) (bool, error) {
	log.Debugf("check backup state, spec: %s, snapshot: %s", s.String(), snapshotName)

	for i := 0; i < MAX_CHECK_RETRY_TIMES; i++ {
		// Retry network related error to avoid full sync when the target network is interrupted, process is restarted.
		if backupState, err := s.checkBackupFinished(snapshotName); err != nil && !isNetworkRelated(err) {
			return false, err
		} else if err == nil && backupState == BackupStateFinished {
			return true, nil
		} else if err == nil && backupState == BackupStateCancelled {
			return false, xerror.Errorf(xerror.Normal, "backup failed or canceled")
		} else {
			// BackupStatePending, BackupStateUnknown or network related errors.
			if err != nil {
				log.Warnf("check backup state is failed, spec: %s, snapshot: %s, err: %v", s.String(), snapshotName, err)
			}
			time.Sleep(BACKUP_CHECK_DURATION)
		}
	}

	return false, xerror.Errorf(xerror.Normal, "check backup state timeout, max try times: %d", MAX_CHECK_RETRY_TIMES)
}

func (s *Spec) CancelBackupIfExists() error {
	log.Debugf("cancel backup job if exists, database: %s", s.Database)

	db, err := s.Connect()
	if err != nil {
		return err
	}

	query := fmt.Sprintf("SHOW BACKUP FROM %s", utils.FormatKeywordName(s.Database))
	log.Infof("show backup state sql: %s", query)
	rows, err := db.Query(query)
	if err != nil {
		return xerror.Wrap(err, xerror.Normal, "query backup state failed")
	}
	defer rows.Close()

	for rows.Next() {
		rowParser := utils.NewRowParser()
		if err := rowParser.Parse(rows); err != nil {
			return xerror.Wrap(err, xerror.Normal, "scan backup state failed")
		}

		info, err := parseBackupInfo(rowParser)
		if err != nil {
			return xerror.Wrap(err, xerror.Normal, "scan backup state failed")
		}

		log.Infof("check snapshot %s backup state [%v], create time: %s",
			info.SnapshotName, info.StateStr, info.CreateTime)

		// Only cancel the running backup job issued by syncer
		if !isSyncerIssuedJob(info.SnapshotName, s.Database) {
			continue
		}

		if info.State == BackupStateFinished || info.State == BackupStateCancelled {
			continue
		}

		cancelSql := fmt.Sprintf("CANCEL BACKUP FROM %s", s.Database)
		log.Infof("cancel backup sql: %s, snapshot: %s", cancelSql, info.SnapshotName)
		if _, err = db.Exec(cancelSql); err != nil {
			return xerror.Wrapf(err, xerror.Normal,
				"cancel backup job %s failed, database: %s", info.SnapshotName, s.Database)
		}
	}
	return nil
}

func (s *Spec) CancelRestoreIfExists(srcDbName string) error {
	log.Debugf("cancel restore job if exists, src db: %s", srcDbName)

	db, err := s.Connect()
	if err != nil {
		return err
	}

	query := fmt.Sprintf("SHOW RESTORE FROM %s", utils.FormatKeywordName(s.Database))
	log.Debugf("show restore state sql: %s", query)
	rows, err := db.Query(query)
	if err != nil {
		return xerror.Wrap(err, xerror.Normal, "query restore state failed")
	}
	defer rows.Close()

	for rows.Next() {
		rowParser := utils.NewRowParser()
		if err := rowParser.Parse(rows); err != nil {
			return xerror.Wrap(err, xerror.Normal, "scan restore state failed")
		}

		info, err := parseRestoreInfo(rowParser)
		if err != nil {
			return xerror.Wrap(err, xerror.Normal, "scan restore state failed")
		}

		log.Infof("check snapshot %s restore state: [%v], create time: %s",
			info.Label, info.StateStr, info.CreateTime)

		// Only cancel the running restore job issued by syncer
		if !isSyncerIssuedJob(info.Label, srcDbName) {
			continue
		}

		if info.State == RestoreStateCancelled || info.State == RestoreStateFinished {
			continue
		}

		cancelSql := fmt.Sprintf("CANCEL RESTORE FROM %s", utils.FormatKeywordName(s.Database))
		log.Infof("cancel restore sql: %s, running snapshot %s", cancelSql, info.Label)

		_, err = db.Exec(cancelSql)
		if err != nil {
			return xerror.Wrapf(err, xerror.Normal, "cancel running restore failed, snapshot %s", info.Label)
		}
	}
	return nil
}

// TODO: Add TaskErrMsg
func (s *Spec) checkRestoreFinished(snapshotName string) (RestoreState, string, error) {
	log.Debugf("check restore state %s", snapshotName)

	db, err := s.Connect()
	if err != nil {
		return RestoreStateUnknown, "", err
	}

	query := fmt.Sprintf("SHOW RESTORE FROM %s WHERE Label = \"%s\"", utils.FormatKeywordName(s.Database), snapshotName)

	log.Debugf("check restore state sql: %s", query)
	rows, err := db.Query(query)
	if err != nil {
		return RestoreStateUnknown, "", xerror.Wrap(err, xerror.Normal, "query restore state failed")
	}
	defer rows.Close()

	if rows.Next() {
		rowParser := utils.NewRowParser()
		if err := rowParser.Parse(rows); err != nil {
			return RestoreStateUnknown, "", xerror.Wrap(err, xerror.Normal, "scan restore state failed")
		}

		info, err := parseRestoreInfo(rowParser)
		if err != nil {
			return RestoreStateUnknown, "", xerror.Wrap(err, xerror.Normal, "scan restore state failed")
		}

		log.Infof("check snapshot %s restore state: [%v], restore status: %s",
			snapshotName, info.StateStr, info.Status)

		return info.State, info.Status, nil
	}
	return RestoreStateUnknown, "", xerror.Errorf(xerror.Normal, "no restore state found")
}

func (s *Spec) CheckRestoreFinished(snapshotName string) (bool, error) {
	log.Debugf("check restore state is finished, spec: %s, snapshot: %s", s.String(), snapshotName)

	for i := 0; i < MAX_CHECK_RETRY_TIMES; i++ {
		// Retry network related error to avoid full sync when the target network is interrupted, process is restarted.
		if restoreState, status, err := s.checkRestoreFinished(snapshotName); err != nil && !isNetworkRelated(err) {
			return false, err
		} else if err == nil && restoreState == RestoreStateFinished {
			return true, nil
		} else if err == nil && restoreState == RestoreStateCancelled && strings.Contains(status, SIGNATURE_NOT_MATCHED) {
			return false, xerror.XWrapf(ErrRestoreSignatureNotMatched, "restore failed, spec: %s, snapshot: %s, status: %s", s.String(), snapshotName, status)
		} else if err == nil && restoreState == RestoreStateCancelled {
			return false, xerror.Errorf(xerror.Normal, "restore failed or canceled, spec: %s, snapshot: %s, status: %s", s.String(), snapshotName, status)
		} else {
			// RestoreStatePending, RestoreStateUnknown or network error.
			if err != nil {
				log.Warnf("check restore state is failed, spec: %s, snapshot: %s, err: %v", s.String(), snapshotName, err)
			}
			time.Sleep(RESTORE_CHECK_DURATION)
		}
	}

	log.Warnf("check restore state timeout, max try times: %d, spec: %s, snapshot: %s", MAX_CHECK_RETRY_TIMES, s, snapshotName)
	return false, nil
}

func (s *Spec) GetRestoreSignatureNotMatchedTableOrView(snapshotName string) (string, bool, error) {
	log.Debugf("get restore signature not matched table, spec: %s, snapshot: %s", s.String(), snapshotName)

	for i := 0; i < MAX_CHECK_RETRY_TIMES; i++ {
		if restoreState, status, err := s.checkRestoreFinished(snapshotName); err != nil {
			return "", false, err
		} else if restoreState == RestoreStateFinished {
			return "", false, nil
		} else if restoreState == RestoreStateCancelled && strings.Contains(status, SIGNATURE_NOT_MATCHED) {
			pattern := regexp.MustCompile("(?P<tableOrView>Table|View) (?P<tableName>.*) already exist but with different schema")
			matches := pattern.FindStringSubmatch(status)
			index := pattern.SubexpIndex("tableName")
			if len(matches) == 0 || index == -1 || len(matches[index]) == 0 {
				return "", false, xerror.Errorf(xerror.Normal, "match table name from restore status failed, spec: %s, snapshot: %s, status: %s", s.String(), snapshotName, status)
			}

			resource := matches[pattern.SubexpIndex("tableOrView")]
			tableOrView := resource == "Table"
			return matches[index], tableOrView, nil
		} else if restoreState == RestoreStateCancelled {
			return "", false, xerror.Errorf(xerror.Normal, "restore failed or canceled, spec: %s, snapshot: %s, status: %s", s.String(), snapshotName, status)
		} else {
			// RestoreStatePending, RestoreStateUnknown
			time.Sleep(RESTORE_CHECK_DURATION)
		}
	}

	log.Warnf("get restore signature not matched timeout, max try times: %d, spec: %s, snapshot: %s", MAX_CHECK_RETRY_TIMES, s, snapshotName)
	return "", false, nil
}

func (s *Spec) waitTransactionDone(txnId int64) error {
	db, err := s.Connect()
	if err != nil {
		return err
	}

	// SHOW TRANSACTION
	// [FROM db_name]
	// WHERE
	// [id=transaction_id]
	// [label = label_name];
	query := fmt.Sprintf("SHOW TRANSACTION FROM %s WHERE id = %d", utils.FormatKeywordName(s.Database), txnId)

	log.Debugf("wait transaction done sql: %s", query)
	rows, err := db.Query(query)
	if err != nil {
		return xerror.Wrap(err, xerror.Normal, "query restore state failed")
	}
	defer rows.Close()

	var transactionStatus string
	if rows.Next() {
		rowParser := utils.NewRowParser()
		if err := rowParser.Parse(rows); err != nil {
			return xerror.Wrap(err, xerror.Normal, "scan transaction status failed")
		}

		transactionStatus, err = rowParser.GetString("TransactionStatus")
		if err != nil {
			return xerror.Wrap(err, xerror.Normal, "scan transaction status failed")
		}

		log.Debugf("check transaction %d status: [%v]", txnId, transactionStatus)
		if transactionStatus == "VISIBLE" {
			return nil
		} else {
			return xerror.Errorf(xerror.Normal, "transaction %d status: %s", txnId, transactionStatus)
		}
	}
	return xerror.Errorf(xerror.Normal, "no transaction status found")
}

func (s *Spec) WaitTransactionDone(txnId int64) {
	for {
		if err := s.waitTransactionDone(txnId); err != nil {
			log.Errorf("wait transaction done failed, err +%v", err)
			time.Sleep(time.Second)
		} else {
			break
		}
	}
}

// Exec sql
func (s *Spec) Exec(sql string) error {
	db, err := s.Connect()
	if err != nil {
		return err
	}

	_, err = db.Exec(sql)
	if err != nil {
		return xerror.Wrapf(err, xerror.Normal, "exec sql %s failed", sql)
	}
	return nil
}

// Db Exec sql
func (s *Spec) DbExec(sql string) error {
	db, err := s.ConnectDB()
	if err != nil {
		return err
	}

	_, err = db.Exec(sql)
	if err != nil {
		return xerror.Wrapf(err, xerror.Normal, "exec sql %s failed", sql)
	}
	return nil
}

// impl SpecCleaner
func (s *Spec) CleanDB() error {
	log.Infof("clean spec %s", s.String())

	return nil
}

// impl utils.Subject[specEvent]
func (s *Spec) Register(observer utils.Observer[SpecEvent]) {
	log.Debugf("register observer %v", observer)

	s.observers = append(s.observers, observer)
}

func (s *Spec) Unregister(observer utils.Observer[SpecEvent]) {
	log.Debugf("unregister observer %v", observer)

	for i, o := range s.observers {
		if o == observer {
			s.observers = append(s.observers[:i], s.observers[i+1:]...)
			break
		}
	}
}

func (s *Spec) Notify(event SpecEvent) {
	log.Debugf("notify observers, event: %v", feNotMasterEvent)

	for _, o := range s.observers {
		o.Update(event)
	}
}

func (s *Spec) Update(event SpecEvent) {
	switch event {
	case feNotMasterEvent:
		log.Infof("frontend %s:%s is not master, try next", s.Host, s.Port)
		// TODO(Drogon): impl switch fe
	default:
		break
	}
}

func (s *Spec) LightningSchemaChange(srcDatabase, tableAlias string, lightningSchemaChange *record.ModifyTableAddOrDropColumns) error {
	log.Debugf("lightningSchemaChange %v", lightningSchemaChange)

	rawSql := lightningSchemaChange.RawSql

	// 1. remove database prefix
	//   "rawSql": "ALTER TABLE `default_cluster:ccr`.`test_ddl` ADD COLUMN `nid1` int(11) NULL COMMENT \"\""
	// replace `default_cluster:${Src.Database}`.`test_ddl` to `test_ddl`
	var sql string
	if strings.Contains(rawSql, fmt.Sprintf("`default_cluster:%s`.", srcDatabase)) {
		sql = strings.Replace(rawSql, fmt.Sprintf("`default_cluster:%s`.", srcDatabase), "", 1)
	} else {
		sql = strings.Replace(rawSql, fmt.Sprintf("`%s`.", srcDatabase), "", 1)
	}

	// 2. handle alias
	if tableAlias != "" {
		re := regexp.MustCompile("ALTER TABLE `[^`]*`")
		sql = re.ReplaceAllString(sql, fmt.Sprintf("ALTER TABLE `%s`", tableAlias))
	}

	// 3. compatible REPLACE_IF_NOT_NULL NULL DEFAULT "null"
	// 	See https://github.com/apache/doris/pull/41205 for details
	sql = strings.Replace(sql, "REPLACE_IF_NOT_NULL NULL DEFAULT \"null\"",
		"REPLACE_IF_NOT_NULL NULL DEFAULT NULL", 1)

	log.Infof("lighting schema change sql, rawSql: %s, sql: %s", rawSql, sql)
	return s.DbExec(sql)
}

func (s *Spec) RenameColumn(destTableName string, renameColumn *record.RenameColumn) error {
	renameSql := fmt.Sprintf("ALTER TABLE `%s` RENAME COLUMN `%s` `%s`", destTableName, renameColumn.ColName, renameColumn.NewColName)
	log.Infof("rename column sql: %s", renameSql)
	return s.DbExec(renameSql)
}

func (s *Spec) TruncateTable(destTableName string, truncateTable *record.TruncateTable) error {
	var sql string
	if truncateTable.RawSql == "" {
		sql = fmt.Sprintf("TRUNCATE TABLE %s", utils.FormatKeywordName(destTableName))
	} else {
		sql = fmt.Sprintf("TRUNCATE TABLE %s %s", utils.FormatKeywordName(destTableName), truncateTable.RawSql)
	}

	log.Infof("truncate table sql: %s", sql)

	return s.DbExec(sql)
}

func (s *Spec) ReplaceTable(fromName, toName string, swap bool) error {
	sql := fmt.Sprintf("ALTER TABLE %s REPLACE WITH TABLE %s PROPERTIES(\"swap\"=\"%t\")",
		utils.FormatKeywordName(toName), utils.FormatKeywordName(fromName), swap)

	log.Infof("replace table sql: %s", sql)

	return s.DbExec(sql)
}

func (s *Spec) DropTable(tableName string, force bool) error {
	sqlSuffix := ""
	if force {
		sqlSuffix = "FORCE"
	}
	dropSql := fmt.Sprintf("DROP TABLE %s %s", utils.FormatKeywordName(tableName), sqlSuffix)
	log.Infof("drop table sql: %s", dropSql)
	return s.DbExec(dropSql)
}

func (s *Spec) DropView(viewName string) error {
	dropView := fmt.Sprintf("DROP VIEW IF EXISTS %s ", utils.FormatKeywordName(viewName))
	log.Infof("drop view sql: %s", dropView)
	return s.DbExec(dropView)
}

func (s *Spec) AddPartition(destTableName string, addPartition *record.AddPartition) error {
	addPartitionSql := addPartition.GetSql(destTableName)
	addPartitionSql = correctAddPartitionSql(addPartitionSql, addPartition)
	log.Infof("addPartitionSql: %s", addPartitionSql)
	return s.DbExec(addPartitionSql)
}

func (s *Spec) DropPartition(destTableName string, dropPartition *record.DropPartition) error {
	destDbName := utils.FormatKeywordName(s.Database)
	destTableName = utils.FormatKeywordName(destTableName)
	dropPartitionSql := fmt.Sprintf("ALTER TABLE %s.%s %s", destDbName, destTableName, dropPartition.Sql)
	log.Infof("dropPartitionSql: %s", dropPartitionSql)
	return s.Exec(dropPartitionSql)
}

func (s *Spec) DesyncTables(tables ...string) error {
	var err error

	failedTables := []string{}
	for _, table := range tables {
		desyncSql := fmt.Sprintf("ALTER TABLE %s SET (\"is_being_synced\"=\"false\")", utils.FormatKeywordName(table))
		log.Debugf("db exec sql: %s", desyncSql)
		if err = s.DbExec(desyncSql); err != nil {
			failedTables = append(failedTables, table)
		}
	}

	if len(failedTables) > 0 {
		return xerror.Wrapf(err, xerror.FE, "failed tables: %s", strings.Join(failedTables, ","))
	}

	return nil
}

// Determine whether the error are network related, eg connection refused, connection reset, exposed from net packages.
func isNetworkRelated(err error) bool {
	msg := err.Error()

	// The below errors are exposed from net packages.
	// See https://github.com/golang/go/issues/23827 for details.
	return strings.Contains(msg, "timeout awaiting response headers") ||
		strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "connection reset by peer") ||
		strings.Contains(msg, "connection timeouted") ||
		strings.Contains(msg, "i/o timeout")
}

func correctAddPartitionSql(addPartitionSql string, addPartition *record.AddPartition) string {
	// HACK:
	//
	// The doris version before 2.1.3 and 2.0.10 did not handle unpartitioned and temporary
	// partitions correctly, see https://github.com/apache/doris/pull/35461 for details.
	//
	// 1. fix unpartitioned add partition sql
	// 2. support add temporary partition
	if strings.Contains(addPartitionSql, "VALUES [(), ())") {
		re := regexp.MustCompile(`VALUES \[\(\), \(\)\) \([^\)]+\)`)
		addPartitionSql = re.ReplaceAllString(addPartitionSql, "")
	}
	if strings.Contains(addPartitionSql, "VALUES IN (((") {
		re := regexp.MustCompile(`VALUES IN \(\(\((.*)\)\)\)`)
		matches := re.FindStringSubmatch(addPartitionSql)
		if len(matches) > 1 {
			replace := fmt.Sprintf("VALUES IN ((%s))", matches[1])
			addPartitionSql = re.ReplaceAllString(addPartitionSql, replace)
		}
	}
	if addPartition.IsTemp && !strings.Contains(addPartitionSql, "ADD TEMPORARY PARTITION") {
		addPartitionSql = strings.ReplaceAll(addPartitionSql, "ADD PARTITION", "ADD TEMPORARY PARTITION")
	}
	return addPartitionSql
}

func isSyncerIssuedJob(label, dbName string) bool {
	fullSyncPrefix := fmt.Sprintf("ccrs_%s", dbName)
	partialSyncPrefix := fmt.Sprintf("ccrp_%s", dbName)
	return strings.HasPrefix(label, fullSyncPrefix) || strings.HasPrefix(label, partialSyncPrefix)
}
