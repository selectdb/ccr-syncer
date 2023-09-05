package base

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/selectdb/ccr_syncer/utils"

	_ "github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"
	"go.uber.org/zap"
)

const (
	BACKUP_CHECK_DURATION  = time.Second * 3
	RESTORE_CHECK_DURATION = time.Second * 3
	MAX_CHECK_RETRY_TIMES  = 86400 // 3 day
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

func ParseRestoreState(state string) RestoreState {
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

type Frontend struct {
	Host       string `json:"host"`
	Port       string `json:"port"`
	ThriftPort string `json:"thrift_port"`
}

// TODO(Drogon): timeout config
type Spec struct {
	// embed Frontend as current master frontend
	Frontend
	frontends []Frontend `json:"frontends"`

	User     string `json:"user"`
	Password string `json:"password"`
	Cluster  string `json:"cluster"`

	Database string `json:"database"`
	DbId     int64  `json:"db_id"`
	Table    string `json:"table"`
	TableId  int64  `json:"table_id"`

	observers []utils.Observer[specEvent]
}

// valid table spec
func (s *Spec) Valid() error {
	if s.Host == "" {
		return errors.Errorf("host is empty")
	}

	// convert port to int16 and check port in range [0, 65535]
	port, err := strconv.ParseUint(s.Port, 10, 16)
	if err != nil {
		return errors.Errorf("port is invalid: %s", s.Port)
	}
	if port > 65535 {
		return errors.Errorf("port is invalid: %s", s.Port)
	}

	// convert thrift port to int16 and check port in range [0, 65535]
	thriftPort, err := strconv.ParseUint(s.ThriftPort, 10, 16)
	if err != nil {
		return errors.Errorf("thrift_port is invalid: %s", s.ThriftPort)
	}
	if thriftPort > 65535 {
		return errors.Errorf("thrift_port is invalid: %s", s.ThriftPort)
	}

	if s.User == "" {
		return errors.Errorf("user is empty")
	}

	if s.Database == "" {
		return errors.Errorf("database is empty")
	}

	return nil
}

func (s *Spec) String() string {
	return fmt.Sprintf("host: %s, port: %s, thrift_port: %s, user: %s, cluster: %s, database: %s, database id: %d, table: %s, table id: %d",
		s.Host, s.Port, s.ThriftPort, s.User, s.Cluster, s.Database, s.DbId, s.Table, s.TableId)
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

	var _dbNmae string
	var createDBString string
	err = db.QueryRow("SHOW CREATE DATABASE "+s.Database).Scan(&_dbNmae, &createDBString)
	if err != nil {
		return false, errors.Wrapf(err, "show create database %s failed", s.Database)
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

	var tableName string
	var createTableString string
	sql := fmt.Sprintf("SHOW CREATE TABLE %s.%s", s.Database, s.Table)
	err = db.QueryRow(sql).Scan(&tableName, &createTableString)
	if err != nil {
		return false, errors.Wrapf(err, "show create table %s.%s failed", s.Database, s.Table)
	}
	log.Infof("table %s.%s create string: %s", s.Database, s.Table, createTableString)

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
		return nil, errors.Wrapf(err, "show tables failed")
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var table string
		err = rows.Scan(&table)
		if err != nil {
			return nil, errors.Wrapf(err, "scan table name failed")
		}
		tables = append(tables, table)
	}
	return tables, nil
}

func (s *Spec) dropTable(table string) error {
	log.Infof("drop table %s.%s", s.Database, table)

	db, err := s.Connect()
	if err != nil {
		return err
	}

	sql := fmt.Sprintf("DROP TABLE %s.%s", s.Database, table)
	_, err = db.Exec(sql)
	if err != nil {
		return errors.Wrapf(err, "drop table %s.%s failed, sql: %s", s.Database, table, sql)
	}
	return nil
}

func (s *Spec) DropTable() error {
	return s.dropTable(s.Table)
}

func (s *Spec) DropTables(tables []string) ([]string, error) {
	log.Infof("drop tables %s", tables)

	var err error
	var successTables []string
	for _, table := range tables {
		err = s.dropTable(table)
		if err != nil {
			break
		}
		successTables = append(successTables, table)
	}

	if err != nil {
		err = errors.Errorf("drop tables %s failed", tables)
	}
	return successTables, err
}

func (s *Spec) ClearDB() error {
	log.Infof("clear database %s", s.Database)

	db, err := s.Connect()
	if err != nil {
		return err
	}

	sql := fmt.Sprintf("DROP DATABASE %s", s.Database)
	_, err = db.Exec(sql)
	if err != nil {
		return errors.Wrapf(err, "drop database %s failed", s.Database)
	}

	if _, err = db.Exec("CREATE DATABASE " + s.Database); err != nil {
		return errors.Wrapf(err, "create database %s failed", s.Database)
	}
	return nil
}

func (s *Spec) CreateDatabase() error {
	log.Debug("create database")

	db, err := s.Connect()
	if err != nil {
		return nil
	}

	if _, err = db.Exec("CREATE DATABASE IF NOT EXISTS " + s.Database); err != nil {
		return errors.Wrapf(err, "create database %s failed", s.Database)
	}
	return nil
}

func (s *Spec) CreateTable(stmt string) error {
	db, err := s.Connect()
	if err != nil {
		return nil
	}

	if _, err = db.Exec(stmt); err != nil {
		return errors.Wrapf(err, "create table %s.%s failed", s.Database, s.Table)
	}
	return nil
}

func (s *Spec) CheckDatabaseExists() (bool, error) {
	log.Debug("check database exist by spec", zap.String("spec", s.String()))
	db, err := s.Connect()
	if err != nil {
		return false, err
	}

	sql := fmt.Sprintf("SHOW DATABASES LIKE '%s'", s.Database)
	rows, err := db.Query(sql)
	if err != nil {
		return false, errors.Wrapf(err, "show databases failed, sql: %s", sql)
	}
	defer rows.Close()

	var database string
	for rows.Next() {
		if err := rows.Scan(&database); err != nil {
			return false, errors.Wrapf(err, "scan database name failed, sql: %s", sql)
		}
	}
	if err := rows.Err(); err != nil {
		return false, errors.Wrapf(err, "scan database name failed, sql: %s", sql)
	}

	return database != "", nil
}

// check table exits in database dir by spec
func (s *Spec) CheckTableExists() (bool, error) {
	log.Debug("check table exists by spec", zap.String("spec", s.String()))

	db, err := s.Connect()
	if err != nil {
		return false, err
	}

	sql := fmt.Sprintf("SHOW TABLES FROM %s LIKE '%s'", s.Database, s.Table)
	rows, err := db.Query(sql)
	if err != nil {
		return false, errors.Wrapf(err, "show tables failed, sql: %s", sql)
	}
	defer rows.Close()

	var table string
	for rows.Next() {
		if err := rows.Scan(&table); err != nil {
			return false, errors.Wrapf(err, "scan table name failed, sql: %s", sql)
		}
	}
	if err := rows.Err(); err != nil {
		return false, errors.Wrapf(err, "scan table name failed, sql: %s", sql)
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
		tableRefs = tables[0]
	} else {
		// snapshot name format "ccrs_${db}_${timestamp}"
		// table refs = tables.join(", ")
		snapshotName = fmt.Sprintf("ccrs_%s_%d", s.Database, time.Now().Unix())
		tableRefs = strings.Join(tables, ", ")
	}

	log.Infof("create snapshot %s.%s", s.Database, snapshotName)

	db, err := s.Connect()
	if err != nil {
		return "", err
	}

	backupSnapshotSql := fmt.Sprintf("BACKUP SNAPSHOT %s.%s TO `__keep_on_local__` ON ( %s ) PROPERTIES (\"type\" = \"full\")", s.Database, snapshotName, tableRefs)
	log.Debugf("backup snapshot sql: %s", backupSnapshotSql)
	_, err = db.Exec(backupSnapshotSql)
	if err != nil {
		return "", errors.Wrapf(err, "backup snapshot %s failed, sql: %s", snapshotName, backupSnapshotSql)
	}

	backupFinished, err := s.CheckBackupFinished(snapshotName)
	if err != nil {
		return "", err
	}
	if !backupFinished {
		err = errors.Errorf("check backup state timeout, max try times: %d, sql: %s", MAX_CHECK_RETRY_TIMES, backupSnapshotSql)
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

	var backupStateStr string
	scanArgs := utils.MakeSingleColScanArgs(3, &backupStateStr, 10)

	sql := fmt.Sprintf("SHOW BACKUP FROM %s WHERE SnapshotName = \"%s\"", s.Database, snapshotName)
	log.Debugf("check backup state sql: %s", sql)
	rows, err := db.Query(sql)
	if err != nil {
		return BackupStateUnknown, errors.Wrapf(err, "show backup failed, sql: %s", sql)
	}
	defer rows.Close()

	if rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return BackupStateUnknown, errors.Wrapf(err, "scan backup state failed, sql: %s", sql)
		}

		log.Infof("check snapshot %s backup state: [%v]", snapshotName, backupStateStr)
		return ParseBackupState(backupStateStr), nil
	}
	return BackupStateUnknown, errors.Errorf("no backup state found, sql: %s", sql)
}

func (s *Spec) CheckBackupFinished(snapshotName string) (bool, error) {
	log.Debug("check backup state", zap.String("database", s.Database))

	for i := 0; i < MAX_CHECK_RETRY_TIMES; i++ {
		if backupState, err := s.checkBackupFinished(snapshotName); err != nil {
			return false, err
		} else if backupState == BackupStateFinished {
			return true, nil
		} else if backupState == BackupStateCancelled {
			return false, errors.Errorf("backup failed or canceled")
		} else {
			// BackupStatePending, BackupStateUnknown
			time.Sleep(BACKUP_CHECK_DURATION)
		}
	}

	return false, errors.Errorf("check backup state timeout, max try times: %d", MAX_CHECK_RETRY_TIMES)
}

// TODO: Add TaskErrMsg
func (s *Spec) checkRestoreFinished(snapshotName string) (RestoreState, error) {
	log.Debugf("check restore state %s", snapshotName)

	db, err := s.Connect()
	if err != nil {
		return RestoreStateUnknown, err
	}

	var restoreStateStr string
	scanArgs := utils.MakeSingleColScanArgs(4, &restoreStateStr, 16)

	query := fmt.Sprintf("SHOW RESTORE FROM %s WHERE Label = \"%s\"", s.Database, snapshotName)

	log.Debugf("check restore state sql: %s", query)
	rows, err := db.Query(query)
	if err != nil {
		return RestoreStateUnknown, errors.Wrapf(err, "query restore state failed")
	}
	defer rows.Close()

	if rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return RestoreStateUnknown, errors.Wrapf(err, "scan restore state failed")
		}

		log.Infof("check snapshot %s restore state: [%v]", snapshotName, restoreStateStr)

		return ParseRestoreState(restoreStateStr), nil
	}
	return RestoreStateUnknown, errors.Errorf("no restore state found")
}

func (s *Spec) CheckRestoreFinished(snapshotName string) (bool, error) {
	log.Debug("check restore is finished", zap.String("spec", s.String()), zap.String("snapshot", snapshotName))

	for i := 0; i < MAX_CHECK_RETRY_TIMES; i++ {
		if backupState, err := s.checkRestoreFinished(snapshotName); err != nil {
			return false, err
		} else if backupState == RestoreStateFinished {
			return true, nil
		} else if backupState == RestoreStateCancelled {
			return false, errors.Errorf("backup failed or canceled, spec: %s, snapshot: %s", s.String(), snapshotName)
		} else {
			// RestoreStatePending, RestoreStateUnknown
			time.Sleep(RESTORE_CHECK_DURATION)
		}
	}

	return false, errors.Errorf("check restore state timeout, max try times: %d, spec: %s, snapshot: %s", MAX_CHECK_RETRY_TIMES, s.String(), snapshotName)
}

// Exec sql
func (s *Spec) Exec(sql string) error {
	db, err := s.Connect()
	if err != nil {
		return err
	}

	_, err = db.Exec(sql)
	if err != nil {
		return errors.Wrapf(err, "exec sql %s failed", sql)
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
		return errors.Wrapf(err, "exec sql %s failed", sql)
	}
	return nil
}

// impl utils.Subject[specEvent]
func (s *Spec) Register(observer utils.Observer[specEvent]) {
	log.Debugf("register observer %v", observer)

	s.observers = append(s.observers, observer)
}

func (s *Spec) Unregister(observer utils.Observer[specEvent]) {
	log.Debugf("unregister observer %v", observer)

	for i, o := range s.observers {
		if o == observer {
			s.observers = append(s.observers[:i], s.observers[i+1:]...)
			break
		}
	}
}

func (s *Spec) Notify(event specEvent) {
	log.Debugf("notify observers, event: %v", feNotMasterEvent)

	for _, o := range s.observers {
		o.Update(event)
	}
}
