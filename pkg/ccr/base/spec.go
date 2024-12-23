// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License
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
var ErrBackupTableNotFound = xerror.NewWithoutStack(xerror.Normal, "backup table not found")
var ErrBackupPartitionNotFound = xerror.NewWithoutStack(xerror.Normal, "backup partition not found")

const (
	BACKUP_CHECK_DURATION  = time.Second * 3
	RESTORE_CHECK_DURATION = time.Second * 3
	MAX_CHECK_RETRY_TIMES  = 86400 // 3 day
	SIGNATURE_NOT_MATCHED  = "already exist but with different schema"

	FE_CONFIG_ENABLE_RESTORE_SNAPSHOT_COMPRESSION = "enable_restore_snapshot_rpc_compression"
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
	Status       string
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

	status, err := parser.GetString("Status")
	if err != nil {
		return nil, xerror.Wrap(err, xerror.Normal, "parse backup Status failed")
	}

	info := &BackupInfo{
		State:        ParseBackupState(stateStr),
		StateStr:     stateStr,
		SnapshotName: snapshotName,
		CreateTime:   createTime,
		Status:       status,
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

	// The mapping of host private and public ip
	HostMapping map[string]string `json:"host_mapping,omitempty"`

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

// create mysql connection from spec
//
// Since the underlying connections are cached by the DSN, we do not set the DB name in the DSN.
// The user should not set any session variables in the connections directly.
func (s *Spec) Connect() (*sql.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/", s.User, s.Password, s.Host, s.Port)
	return GetMysqlDB(dsn)
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

func (s *Spec) CheckTablePropertyValid() ([]string, error) {
	log.Infof("check table %s.%s properties valid", s.Database, s.Table)

	db, err := s.Connect()

	checkResult := []string{}

	validProperty := []string{
		`"binlog.enable" = "true"`,
		`"light_schema_change" = "true"`,
	}

	if err != nil {
		return checkResult, err
	}

	var createTableString string
	query := fmt.Sprintf("SHOW CREATE TABLE %s.%s", utils.FormatKeywordName(s.Database), utils.FormatKeywordName(s.Table))
	rows, err := db.Query(query)
	if err != nil {
		return checkResult, xerror.Wrap(err, xerror.Normal, query)
	}
	defer rows.Close()

	for rows.Next() {
		rowParser := utils.NewRowParser()
		if err := rowParser.Parse(rows); err != nil {
			return checkResult, xerror.Wrap(err, xerror.Normal, query)
		}
		createTableString, err = rowParser.GetString("Create Table")
		if err != nil {
			return checkResult, xerror.Wrap(err, xerror.Normal, query)
		}
	}

	if err := rows.Err(); err != nil {
		return checkResult, xerror.Wrap(err, xerror.Normal, query)
	}

	log.Tracef("table %s.%s create string: %s", s.Database, s.Table, createTableString)

	// check valid property in create table string

	for _, property := range validProperty {
		if isValid := strings.Contains(createTableString, property); !isValid {
			checkResult = append(checkResult, property)
		}
	}

	return checkResult, nil
}

func (s *Spec) IsEnableRestoreSnapshotCompression() (bool, error) {
	log.Debugf("check frontend enable restore snapshot compression")

	db, err := s.Connect()
	if err != nil {
		return false, err
	}

	sql := fmt.Sprintf("SHOW FRONTEND CONFIG LIKE '%s'", FE_CONFIG_ENABLE_RESTORE_SNAPSHOT_COMPRESSION)
	rows, err := db.Query(sql)
	if err != nil {
		return false, xerror.Wrap(err, xerror.Normal, "show frontend config failed")
	}
	defer rows.Close()

	enableCompress := false
	if rows.Next() {
		rowParser := utils.NewRowParser()
		if err := rowParser.Parse(rows); err != nil {
			return false, xerror.Wrap(err, xerror.Normal, "parse show frontend config result failed")
		}
		value, err := rowParser.GetString("Value")
		if err != nil {
			return false, xerror.Wrap(err, xerror.Normal, "parse show frontend config Value failed")
		}
		enableCompress = strings.ToLower(value) == "true"
	}

	if err := rows.Err(); err != nil {
		return false, xerror.Wrapf(err, xerror.Normal,
			"check frontend enable restore snapshot compress, sql: %s", sql)
	}

	log.Debugf("frontend enable restore snapshot compression: %t", enableCompress)
	return enableCompress, nil
}

func (s *Spec) GetAllTables() ([]string, error) {
	log.Debugf("get all tables in database %s", s.Database)

	db, err := s.Connect()
	if err != nil {
		return nil, err
	}

	sql := fmt.Sprintf("SHOW TABLES %s", utils.FormatKeywordName(s.Database))
	rows, err := db.Query(sql)
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

	if err := rows.Err(); err != nil {
		return nil, xerror.Wrapf(err, xerror.Normal, "SHOW TABLES")
	}

	return tables, nil
}

func (s *Spec) queryResult(querySQL string, queryColumn string, errMsg string) ([]string, error) {
	db, err := s.Connect()
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

	if err := rows.Err(); err != nil {
		return nil, xerror.Wrapf(err, xerror.Normal, "query result failed, sql: %s", querySQL)
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
	dbName := utils.FormatKeywordName(s.Database)
	for _, eachViewName := range viewsFromQuery {
		eachViewName = utils.FormatKeywordName(eachViewName)
		showCreateViewSql := fmt.Sprintf("SHOW CREATE VIEW %s.%s", dbName, eachViewName)
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
	destTableName = utils.FormatKeywordName(destTableName)
	// rename table may be 'rename table', 'rename rollup', 'rename partition'
	var sql string
	// ALTER TABLE table1 RENAME table2;
	if renameTable.NewTableName != "" && renameTable.OldTableName != "" {
		oldName := utils.FormatKeywordName(renameTable.OldTableName)
		newName := utils.FormatKeywordName(renameTable.NewTableName)
		dbName := utils.FormatKeywordName(s.Database)
		sql = fmt.Sprintf("ALTER TABLE %s.%s RENAME %s", dbName, oldName, newName)
	}

	// ALTER TABLE example_table RENAME ROLLUP rollup1 rollup2;
	// if rename rollup, table name is unchanged
	if renameTable.NewRollupName != "" && renameTable.OldRollupName != "" {
		oldName := utils.FormatKeywordName(renameTable.OldRollupName)
		newName := utils.FormatKeywordName(renameTable.NewRollupName)
		dbName := utils.FormatKeywordName(s.Database)
		sql = fmt.Sprintf("ALTER TABLE %s.%s RENAME ROLLUP %s %s", dbName, destTableName, oldName, newName)
	}

	// ALTER TABLE example_table RENAME PARTITION p1 p2;
	// if rename partition, table name is unchanged
	if renameTable.NewPartitionName != "" && renameTable.OldPartitionName != "" {
		oldName := utils.FormatKeywordName(renameTable.OldPartitionName)
		newName := utils.FormatKeywordName(renameTable.NewPartitionName)
		dbName := utils.FormatKeywordName(s.Database)
		sql = fmt.Sprintf("ALTER TABLE %s.%s RENAME PARTITION %s %s", dbName, destTableName, oldName, newName)
	}
	if sql == "" {
		return xerror.Errorf(xerror.Normal, "rename sql is empty")
	}

	log.Infof("rename table sql: %s", sql)
	return s.Exec(sql)
}

func (s *Spec) RenameTableWithName(oldName, newName string) error {
	oldName = utils.FormatKeywordName(oldName)
	newName = utils.FormatKeywordName(newName)
	dbName := utils.FormatKeywordName(s.Database)
	sql := fmt.Sprintf("ALTER TABLE %s.%s RENAME %s", dbName, oldName, newName)
	log.Infof("rename table sql: %s", sql)
	return s.Exec(sql)
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
	if createTable.IsCreateView() {
		log.Debugf("create view, use dest db name to replace source db name")

		// replace `internal`.`source_db_name`. or `default_cluster:source_db_name`. to `internal`.`dest_db_name`.
		originalNameNewStyle := "`internal`.`" + strings.TrimSpace(srcDatabase) + "`."
		originalNameOldStyle := "`default_cluster:" + strings.TrimSpace(srcDatabase) + "`." // for Doris 2.0.x
		replaceName := "`internal`.`" + strings.TrimSpace(s.Database) + "`."
		createSql = strings.ReplaceAll(
			strings.ReplaceAll(createSql, originalNameNewStyle, replaceName), originalNameOldStyle, replaceName)
		log.Debugf("original create view sql is %s, after replace, now sql is %s", createTable.Sql, createSql)
	}

	createSql = AddDBPrefixToCreateTableOrViewSql(s.Database, createSql)

	// COMMENT 'xxx' should be COMMENT "xxx" and excape content
	createSql = ReplaceAndEscapeComment(createSql)

	// Compatible with doris 2.1.x, see apache/doris#44834 for details.
	for strings.Contains(createSql, "MAXVALUEMAXVALUE") {
		createSql = strings.Replace(createSql, "MAXVALUEMAXVALUE", "MAXVALUE, MAXVALUE", -1)
	}

	log.Infof("create table or view sql: %s", createSql)
	return s.Exec(createSql)
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

func (s *Spec) CancelRestoreIfExists(snapshotName string) error {
	log.Debugf("cancel restore %s, db name: %s", snapshotName, s.Database)

	db, err := s.Connect()
	if err != nil {
		return err
	}

	info, err := s.queryRestoreInfo(db, snapshotName)
	if err != nil {
		return err
	}

	if info == nil || info.State == RestoreStateCancelled || info.State == RestoreStateFinished {
		return nil
	}

	sql := fmt.Sprintf("CANCEL RESTORE FROM %s", utils.FormatKeywordName(s.Database))
	log.Infof("cancel restore %s, sql: %s", snapshotName, sql)
	_, err = db.Exec(sql)
	if err != nil {
		return xerror.Wrapf(err, xerror.Normal, "cancel restore failed, sql: %s", sql)
	}
	return nil
}

// Create a full snapshot of the specified tables, if tables is empty, backup the entire database.
// mysql> BACKUP SNAPSHOT ccr.snapshot_20230605 TO `__keep_on_local__` ON (      src_1 ) PROPERTIES ("type" = "full");
func (s *Spec) CreateSnapshot(snapshotName string, tables []string) error {
	if tables == nil {
		tables = make([]string, 0)
	}
	if len(tables) == 0 {
		tables = append(tables, s.Table)
	}

	var tableRefs string
	if len(tables) == 1 {
		// table refs = table
		tableRefs = utils.FormatKeywordName(tables[0])
	} else {
		// table refs = tables.join(", ")
		tableRefs = "`" + strings.Join(tables, "`,`") + "`"
	}

	// means source is a empty db, table number is 0, so backup the entire database
	if tableRefs == "``" {
		tableRefs = ""
	} else {
		tableRefs = fmt.Sprintf("ON ( %s )", tableRefs)
	}

	db, err := s.Connect()
	if err != nil {
		return err
	}

	backupSnapshotSql := fmt.Sprintf("BACKUP SNAPSHOT %s.%s TO `__keep_on_local__` %s PROPERTIES (\"type\" = \"full\")",
		utils.FormatKeywordName(s.Database), utils.FormatKeywordName(snapshotName), tableRefs)
	log.Infof("create snapshot %s.%s, backup snapshot sql: %s", s.Database, snapshotName, backupSnapshotSql)
	_, err = db.Exec(backupSnapshotSql)
	if err != nil {
		return xerror.Wrapf(err, xerror.Normal, "backup snapshot %s failed, sql: %s", snapshotName, backupSnapshotSql)
	}

	return nil
}

// mysql> BACKUP SNAPSHOT ccr.snapshot_20230605 TO `__keep_on_local__` ON (src_1 PARTITION (`p1`)) PROPERTIES ("type" = "full");
func (s *Spec) CreatePartialSnapshot(snapshotName, table string, partitions []string) error {
	if len(table) == 0 {
		return xerror.Errorf(xerror.Normal, "source db is empty! you should have at least one table")
	}

	// table refs = table
	tableRef := utils.FormatKeywordName(table)

	log.Infof("create partial snapshot %s.%s", s.Database, snapshotName)

	db, err := s.Connect()
	if err != nil {
		return err
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
		if strings.Contains(err.Error(), "Unknown table") {
			return ErrBackupTableNotFound
		} else if strings.Contains(err.Error(), "Unknown partition") {
			return ErrBackupPartitionNotFound
		} else {
			return xerror.Wrapf(err, xerror.Normal, "backup partial snapshot %s failed, sql: %s", snapshotName, backupSnapshotSql)
		}
	}

	return nil
}

// TODO: Add TaskErrMsg
func (s *Spec) checkBackupFinished(snapshotName string) (BackupState, string, error) {
	log.Debugf("check backup state of snapshot %s", snapshotName)

	db, err := s.Connect()
	if err != nil {
		return BackupStateUnknown, "", err
	}

	sql := fmt.Sprintf("SHOW BACKUP FROM %s WHERE SnapshotName = \"%s\"", utils.FormatKeywordName(s.Database), snapshotName)
	log.Debugf("check backup state sql: %s", sql)
	rows, err := db.Query(sql)
	if err != nil {
		return BackupStateUnknown, "", xerror.Wrapf(err, xerror.Normal, "show backup failed, sql: %s", sql)
	}
	defer rows.Close()

	if rows.Next() {
		rowParser := utils.NewRowParser()
		if err := rowParser.Parse(rows); err != nil {
			return BackupStateUnknown, "", xerror.Wrap(err, xerror.Normal, sql)
		}

		info, err := parseBackupInfo(rowParser)
		if err != nil {
			return BackupStateUnknown, "", xerror.Wrap(err, xerror.Normal, sql)
		}

		log.Infof("check snapshot %s backup state: [%v]", snapshotName, info.StateStr)
		return info.State, info.Status, nil
	}

	if err := rows.Err(); err != nil {
		return BackupStateUnknown, "", xerror.Wrapf(err, xerror.Normal, "check snapshot backup state, sql: %s", sql)
	}

	return BackupStateUnknown, "", xerror.Errorf(xerror.Normal, "no backup state found, sql: %s", sql)
}

func (s *Spec) CheckBackupFinished(snapshotName string) (bool, error) {
	log.Debugf("check backup state, spec: %s, snapshot: %s", s.String(), snapshotName)

	// Retry network related error to avoid full sync when the target network is interrupted, process is restarted.
	if backupState, status, err := s.checkBackupFinished(snapshotName); err != nil && !isNetworkRelated(err) {
		return false, err
	} else if err == nil && backupState == BackupStateFinished {
		return true, nil
	} else if err == nil && backupState == BackupStateCancelled {
		return false, xerror.Errorf(xerror.Normal, "backup failed or canceled, backup status: %s", status)
	} else {
		// BackupStatePending, BackupStateUnknown or network related errors.
		if err != nil {
			log.Warnf("check backup state is failed, spec: %s, snapshot: %s, err: %v", s.String(), snapshotName, err)
		}
		return false, nil
	}
}

// Get the valid (running or finished) backup job with a unique prefix to indicate
// if a backup job needs to be issued again.
func (s *Spec) GetValidBackupJob(snapshotNamePrefix string) (string, error) {
	log.Debugf("get valid backup job if exists, database: %s, label prefix: %s", s.Database, snapshotNamePrefix)

	db, err := s.Connect()
	if err != nil {
		return "", err
	}

	query := fmt.Sprintf("SHOW BACKUP FROM %s WHERE SnapshotName LIKE \"%s%%\"",
		utils.FormatKeywordName(s.Database), snapshotNamePrefix)
	log.Infof("show backup state sql: %s", query)
	rows, err := db.Query(query)
	if err != nil {
		return "", xerror.Wrap(err, xerror.Normal, "query backup state failed")
	}
	defer rows.Close()

	labels := make([]string, 0)
	for rows.Next() {
		rowParser := utils.NewRowParser()
		if err := rowParser.Parse(rows); err != nil {
			return "", xerror.Wrap(err, xerror.Normal, "scan backup state failed")
		}

		info, err := parseBackupInfo(rowParser)
		if err != nil {
			return "", xerror.Wrap(err, xerror.Normal, "scan backup state failed")
		}

		log.Infof("check snapshot %s backup state [%v], create time: %s",
			info.SnapshotName, info.StateStr, info.CreateTime)

		if info.State == BackupStateCancelled {
			continue
		}

		labels = append(labels, info.SnapshotName)
	}

	if err := rows.Err(); err != nil {
		return "", xerror.Wrapf(err, xerror.Normal, "get valid backup job, sql: %s", query)
	}

	// Return the last one. Assume that the result of `SHOW BACKUP` is ordered by CreateTime in ascending order.
	if len(labels) != 0 {
		return labels[len(labels)-1], nil
	}

	return "", nil
}

// Get the valid (running or finished) restore job with a unique prefix to indicate
// if a restore job needs to be issued again.
func (s *Spec) GetValidRestoreJob(snapshotNamePrefix string) (string, error) {
	log.Debugf("get valid restore job if exists, label prefix: %s", snapshotNamePrefix)

	db, err := s.Connect()
	if err != nil {
		return "", err
	}

	query := fmt.Sprintf("SHOW RESTORE FROM %s WHERE Label LIKE \"%s%%\"",
		utils.FormatKeywordName(s.Database), snapshotNamePrefix)
	log.Infof("show restore state sql: %s", query)
	rows, err := db.Query(query)
	if err != nil {
		return "", xerror.Wrap(err, xerror.Normal, "query restore state failed")
	}
	defer rows.Close()

	labels := make([]string, 0)
	for rows.Next() {
		rowParser := utils.NewRowParser()
		if err := rowParser.Parse(rows); err != nil {
			return "", xerror.Wrap(err, xerror.Normal, "scan restore state failed")
		}

		info, err := parseRestoreInfo(rowParser)
		if err != nil {
			return "", xerror.Wrap(err, xerror.Normal, "scan restore state failed")
		}

		log.Infof("check snapshot %s restore state: [%v], create time: %s",
			info.Label, info.StateStr, info.CreateTime)

		if info.State == RestoreStateCancelled {
			continue
		}

		labels = append(labels, info.Label)
	}

	if err := rows.Err(); err != nil {
		return "", xerror.Wrapf(err, xerror.Normal, "get valid restore job, sql: %s", query)
	}

	// Return the last one. Assume that the result of `SHOW BACKUP` is ordered by CreateTime in ascending order.
	if len(labels) != 0 {
		return labels[len(labels)-1], nil
	}

	return "", nil
}

// query restore info, return nil if not found
func (s *Spec) queryRestoreInfo(db *sql.DB, snapshotName string) (*RestoreInfo, error) {
	query := fmt.Sprintf("SHOW RESTORE FROM %s WHERE Label = \"%s\"",
		utils.FormatKeywordName(s.Database), snapshotName)

	log.Debugf("query restore info sql: %s", query)
	rows, err := db.Query(query)
	if err != nil {
		return nil, xerror.Wrap(err, xerror.Normal, "query restore state failed")
	}
	defer rows.Close()

	if rows.Next() {
		rowParser := utils.NewRowParser()
		if err := rowParser.Parse(rows); err != nil {
			return nil, xerror.Wrap(err, xerror.Normal, "scan restore state failed")
		}

		info, err := parseRestoreInfo(rowParser)
		if err != nil {
			return nil, xerror.Wrap(err, xerror.Normal, "scan restore state failed")
		}

		log.Infof("query snapshot %s restore state: [%v], restore status: %s",
			snapshotName, info.StateStr, info.Status)

		return info, nil
	}

	if err := rows.Err(); err != nil {
		return nil, xerror.Wrapf(err, xerror.Normal, "query restore info, sql: %s", query)
	}

	return nil, nil
}

func (s *Spec) checkRestoreFinished(snapshotName string) (RestoreState, string, error) {
	log.Debugf("check restore state %s", snapshotName)

	db, err := s.Connect()
	if err != nil {
		return RestoreStateUnknown, "", err
	}

	info, err := s.queryRestoreInfo(db, snapshotName)
	if err != nil {
		return RestoreStateUnknown, "", err
	}

	if info == nil {
		return RestoreStateUnknown, "", xerror.Errorf(xerror.Normal, "no restore state found")
	}

	return info.State, info.Status, nil
}

func (s *Spec) CheckRestoreFinished(snapshotName string) (bool, error) {
	log.Debugf("check restore state is finished, spec: %s, snapshot: %s", s.String(), snapshotName)

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
		return false, nil
	}
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

	if err := rows.Err(); err != nil {
		return xerror.Wrapf(err, xerror.Normal, "get transaction status failed, sql: %s", query)
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
	var match string
	if strings.Contains(rawSql, fmt.Sprintf("`default_cluster:%s`.", srcDatabase)) {
		match = fmt.Sprintf("`default_cluster:%s`.", srcDatabase)
	} else {
		match = fmt.Sprintf("`%s`.", srcDatabase)
	}
	dbName := utils.FormatKeywordName(s.Database)
	sql := strings.Replace(rawSql, match, fmt.Sprintf("%s.", dbName), 1)

	// 2. handle alias
	if tableAlias != "" {
		tableAlias := utils.FormatKeywordName(tableAlias)
		re := regexp.MustCompile(fmt.Sprintf("ALTER TABLE %s.`[^`]*`", dbName))
		sql = re.ReplaceAllString(sql, fmt.Sprintf("ALTER TABLE %s.%s", dbName, tableAlias))
	}

	// 3. compatible REPLACE_IF_NOT_NULL NULL DEFAULT "null"
	// 	See https://github.com/apache/doris/pull/41205 for details
	sql = strings.Replace(sql, "REPLACE_IF_NOT_NULL NULL DEFAULT \"null\"",
		"REPLACE_IF_NOT_NULL NULL DEFAULT NULL", 1)

	sql = strings.ReplaceAll(sql, "DEFAULT \"CURRENT_TIMESTAMP\"", "DEFAULT CURRENT_TIMESTAMP")
	sql = strings.ReplaceAll(sql, "DEFAULT \"BITMAP_EMPTY_DEFAULT_VALUE\"", "DEFAULT BITMAP_EMPTY_DEFAULT_VALUE")

	log.Infof("lighting schema change sql, rawSql: %s, sql: %s", rawSql, sql)
	return s.Exec(sql)
}

func (s *Spec) RenameColumn(destTableName string, renameColumn *record.RenameColumn) error {
	dbName := utils.FormatKeywordName(s.Database)
	destTableName = utils.FormatKeywordName(destTableName)
	renameSql := fmt.Sprintf("ALTER TABLE %s.%s RENAME COLUMN `%s` `%s`",
		dbName, destTableName, renameColumn.ColName, renameColumn.NewColName)
	log.Infof("rename column sql: %s", renameSql)
	return s.Exec(renameSql)
}

func (s *Spec) ModifyComment(destTableName string, modifyComment *record.ModifyComment) error {
	dbName := utils.FormatKeywordName(s.Database)
	destTableName = utils.FormatKeywordName(destTableName)

	var modifySql string
	if modifyComment.Type == "COLUMN" {
		var sb strings.Builder
		sb.WriteString(fmt.Sprintf("ALTER TABLE %s.%s ", dbName, destTableName))
		first := true
		for col, comment := range modifyComment.ColToComment {
			if !first {
				sb.WriteString(", ")
			}
			sb.WriteString(fmt.Sprintf("MODIFY COLUMN `%s` COMMENT '%s'", col, utils.EscapeStringValue(comment)))
			first = false
		}
		modifySql = sb.String()
	} else if modifyComment.Type == "TABLE" {
		modifySql = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COMMENT '%s'",
			dbName, destTableName, utils.EscapeStringValue(modifyComment.TblComment))
	} else {
		return xerror.Errorf(xerror.Normal, "unsupported modify comment type: %s", modifyComment.Type)
	}

	log.Infof("modify comment sql: %s", modifySql)
	return s.Exec(modifySql)
}

func (s *Spec) TruncateTable(destTableName string, truncateTable *record.TruncateTable) error {
	dbName := utils.FormatKeywordName(s.Database)
	destTableName = utils.FormatKeywordName(destTableName)

	var sql string
	if truncateTable.RawSql == "" {
		sql = fmt.Sprintf("TRUNCATE TABLE %s.%s", dbName, destTableName)
	} else {
		sql = fmt.Sprintf("TRUNCATE TABLE %s.%s %s", dbName, destTableName, truncateTable.RawSql)
	}

	log.Infof("truncate table sql: %s", sql)

	return s.Exec(sql)
}

func (s *Spec) ReplaceTable(fromName, toName string, swap bool) error {
	dbName := utils.FormatKeywordName(s.Database)
	toName = utils.FormatKeywordName(toName)
	fromName = utils.FormatKeywordName(fromName)
	sql := fmt.Sprintf("ALTER TABLE %s.%s REPLACE WITH TABLE %s PROPERTIES(\"swap\"=\"%t\")",
		dbName, toName, fromName, swap)

	log.Infof("replace table sql: %s", sql)

	return s.Exec(sql)
}

func (s *Spec) DropTable(tableName string, force bool) error {
	sqlSuffix := ""
	if force {
		sqlSuffix = "FORCE"
	}
	dbName := utils.FormatKeywordName(s.Database)
	tableName = utils.FormatKeywordName(tableName)
	dropSql := fmt.Sprintf("DROP TABLE %s.%s %s", dbName, tableName, sqlSuffix)
	log.Infof("drop table sql: %s", dropSql)
	return s.Exec(dropSql)
}

func (s *Spec) DropView(viewName string) error {
	dbName := utils.FormatKeywordName(s.Database)
	viewName = utils.FormatKeywordName(viewName)
	dropView := fmt.Sprintf("DROP VIEW IF EXISTS %s.%s ", dbName, viewName)
	log.Infof("drop view sql: %s", dropView)
	return s.Exec(dropView)
}

func (s *Spec) AlterViewDef(srcDatabase, viewName string, alterView *record.AlterView) error {
	// 1. remove database prefix
	// CREATE VIEW `view_test_1159493057` AS
	//	SELECT
	//		`internal`.`regression_test_db_sync_view_alter`.`tbl_duplicate_0_1159493057`.`user_id` AS `k1`,
	// 		`internal`.`regression_test_db_sync_view_alter`.`tbl_duplicate_0_1159493057`.`name` AS `name`,
	// 		MAX(`internal`.`regression_test_db_sync_view_alter`.`tbl_duplicate_0_1159493057`.`age`) AS `v1`
	//	FROM `internal`.`regression_test_db_sync_view_alter`.`tbl_duplicate_0_1159493057`
	var def string
	prefix := fmt.Sprintf("`internal`.`%s`.", srcDatabase)
	if !strings.Contains(alterView.InlineViewDef, prefix) {
		prefix = fmt.Sprintf(" `%s`.", srcDatabase)
	}
	dbName := utils.FormatKeywordName(s.Database)
	def = strings.ReplaceAll(alterView.InlineViewDef, prefix, fmt.Sprintf(" %s.", dbName))

	viewName = utils.FormatKeywordName(viewName)
	alterViewSql := fmt.Sprintf("ALTER VIEW %s.%s AS %s", dbName, viewName, def)
	log.Infof("alter view sql: %s", alterViewSql)
	return s.Exec(alterViewSql)
}

func (s *Spec) AddPartition(destTableName string, addPartition *record.AddPartition) error {
	addPartitionSql := addPartition.GetSql(s.Database, destTableName)
	addPartitionSql = correctAddPartitionSql(addPartitionSql, addPartition)
	log.Infof("add partition sql: %s, original sql: %s", addPartitionSql, addPartition.Sql)
	return s.Exec(addPartitionSql)
}

func (s *Spec) DropPartition(destTableName string, dropPartition *record.DropPartition) error {
	dbName := utils.FormatKeywordName(s.Database)
	destTableName = utils.FormatKeywordName(destTableName)
	dropPartitionSql := fmt.Sprintf("ALTER TABLE %s.%s %s", dbName, destTableName, dropPartition.Sql)
	log.Infof("drop partition sql: %s", dropPartitionSql)
	return s.Exec(dropPartitionSql)
}

func (s *Spec) RenamePartition(destTableName, oldPartition, newPartition string) error {
	dbName := utils.FormatKeywordName(s.Database)
	destTableName = utils.FormatKeywordName(destTableName)
	oldPartition = utils.FormatKeywordName(oldPartition)
	newPartition = utils.FormatKeywordName(newPartition)
	renamePartitionSql := fmt.Sprintf("ALTER TABLE %s.%s RENAME PARTITION %s %s",
		dbName, destTableName, oldPartition, newPartition)
	log.Infof("rename partition sql: %s", renamePartitionSql)
	return s.Exec(renamePartitionSql)
}

func (s *Spec) LightningIndexChange(alias string, record *record.ModifyTableAddOrDropInvertedIndices) error {
	rawSql := record.GetRawSql()
	if len(record.AlternativeIndexes) == 0 {
		return xerror.Errorf(xerror.Normal, "lightning index change job is empty, should not be here")
	}

	dbName := utils.FormatKeywordName(s.Database)
	alias = utils.FormatKeywordName(alias)
	sql := fmt.Sprintf("ALTER TABLE %s.%s", dbName, alias)
	if record.IsDropInvertedIndex {
		dropIndexes := []string{}
		for _, index := range record.AlternativeIndexes {
			if !index.IsInvertedIndex() {
				return xerror.Errorf(xerror.Normal, "lightning index change job is not inverted index, should not be here")
			}
			indexName := utils.FormatKeywordName(index.GetIndexName())
			dropIndexes = append(dropIndexes, fmt.Sprintf("DROP INDEX %s", indexName))
		}
		sql = fmt.Sprintf("%s %s", sql, strings.Join(dropIndexes, ", "))
	} else {
		addIndexes := []string{}
		for _, index := range record.AlternativeIndexes {
			if !index.IsInvertedIndex() {
				return xerror.Errorf(xerror.Normal, "lightning index change job is not inverted index, should not be here")
			}
			columns := index.GetColumns()
			columnsRef := fmt.Sprintf("(`%s`)", strings.Join(columns, "`,`"))
			indexName := utils.FormatKeywordName(index.GetIndexName())
			addIndex := fmt.Sprintf("ADD INDEX %s %s USING INVERTED COMMENT '%s'",
				indexName, columnsRef, index.GetComment())
			addIndexes = append(addIndexes, addIndex)
		}
		sql = fmt.Sprintf("%s %s", sql, strings.Join(addIndexes, ", "))
	}

	log.Infof("lighting index change sql, rawSql: %s, sql: %s", rawSql, sql)
	return s.Exec(sql)
}

func (s *Spec) BuildIndex(tableAlias string, buildIndex *record.IndexChangeJob) error {
	if buildIndex.IsDropOp {
		return xerror.Errorf(xerror.Normal, "build index job is drop op, should not be here")
	}

	if len(buildIndex.Indexes) != 1 {
		return xerror.Errorf(xerror.Normal, "build index job has more than one index, should not be here")
	}

	index := buildIndex.Indexes[0]
	indexName := index.GetIndexName()
	indexName = utils.FormatKeywordName(indexName)
	tableAlias = utils.FormatKeywordName(tableAlias)
	dbName := utils.FormatKeywordName(s.Database)
	sql := fmt.Sprintf("BUILD INDEX %s ON %s.%s", indexName, dbName, tableAlias)
	if buildIndex.PartitionName != "" {
		sqlWithPart := fmt.Sprintf("%s PARTITION (%s)", sql, utils.FormatKeywordName(buildIndex.PartitionName))

		log.Infof("build index sql: %s", sqlWithPart)
		err := s.Exec(sqlWithPart)
		if err == nil {
			return nil
		} else if !strings.Contains(err.Error(), "is not partitioned, cannot build index with partitions") {
			return err
		}

		log.Infof("table %s is not partitioned, try to build index without partition", tableAlias)
	}

	log.Infof("build index sql: %s", sql)
	return s.Exec(sql)
}

func (s *Spec) RenameRollup(destTableName, oldRollup, newRollup string) error {
	dbName := utils.FormatKeywordName(s.Database)
	destTableName = utils.FormatKeywordName(destTableName)
	oldRollup = utils.FormatKeywordName(oldRollup)
	newRollup = utils.FormatKeywordName(newRollup)
	renameRollupSql := fmt.Sprintf("ALTER TABLE %s.%s RENAME ROLLUP %s %s",
		dbName, destTableName, oldRollup, newRollup)
	log.Infof("rename rollup sql: %s", renameRollupSql)
	return s.Exec(renameRollupSql)
}

func (s *Spec) DropRollup(destTableName, rollup string) error {
	dbName := utils.FormatKeywordName(s.Database)
	destTableName = utils.FormatKeywordName(destTableName)
	rollup = utils.FormatKeywordName(rollup)
	dropRollupSql := fmt.Sprintf("ALTER TABLE %s.%s DROP ROLLUP %s", dbName, destTableName, rollup)
	log.Infof("drop rollup sql: %s", dropRollupSql)
	return s.Exec(dropRollupSql)
}

func (s *Spec) DesyncTables(tables ...string) error {
	var err error

	failedTables := []string{}
	dbName := utils.FormatKeywordName(s.Database)
	for _, table := range tables {
		table = utils.FormatKeywordName(table)
		desyncSql := fmt.Sprintf("ALTER TABLE %s.%s SET (\"is_being_synced\"=\"false\")", dbName, table)
		log.Debugf("exec sql: %s", desyncSql)
		if err = s.Exec(desyncSql); err != nil {
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

func AddDBPrefixToCreateTableOrViewSql(dbName, createSql string) string {
	// extract table/view name from create sql, and add db prefix
	re := regexp.MustCompile("^\\s*CREATE\\s+(VIEW|TABLE)\\s+`([^`]+)`\\s+")
	matches := re.FindStringSubmatch(createSql)
	if len(matches) == 3 {
		resource := matches[1]
		viewName := utils.FormatKeywordName(matches[2])
		dbName := utils.FormatKeywordName(dbName)
		createSql = re.ReplaceAllString(createSql,
			fmt.Sprintf("CREATE %s %s.%s ", resource, dbName, viewName))
	}
	return createSql
}

func ReplaceAndEscapeComment(input string) string {
	re := regexp.MustCompile(`COMMENT '(.*?)'`)

	return re.ReplaceAllStringFunc(input, func(match string) string {
		groups := re.FindStringSubmatch(match)
		if len(groups) < 2 {
			return match
		}
		content := groups[1]
		escapedContent := strconv.Quote(content)
		replaced := fmt.Sprintf(`COMMENT "%s"`, escapedContent[1:len(escapedContent)-1])
		return replaced
	})
}
