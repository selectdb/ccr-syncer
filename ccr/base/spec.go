package base

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"
	"go.uber.org/zap"
)

const (
	BACKUP_CHECK_DURATION  = time.Second * 3
	RESTORE_CHECK_DURATION = time.Second * 3
	MAX_CHECK_RETRY_TIMES  = 20
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

// TODO(Drogon): timeout config
type Spec struct {
	Host       string `json:"host,required"`
	Port       string `json:"port,required"`
	ThriftPort string `json:"thrift_port,required"`
	User       string `json:"user,required"`
	Password   string `json:"password,required"`
	Cluster    string `json:"cluster,required"`
	Database   string `json:"database,required"`
	DbId       int64  `json:"db_id"`
	Table      string `json:"table,required"`
	TableId    int64  `json:"table_id"`
}

// valid table spec
func (s *Spec) Valid() error {
	if s.Host == "" {
		return fmt.Errorf("host is empty")
	}

	// convert port to int16 and check port in range [0, 65535]
	port, err := strconv.ParseUint(s.Port, 10, 16)
	if err != nil {
		return fmt.Errorf("port is invalid: %s", s.Port)
	}
	if port > 65535 {
		return fmt.Errorf("port is invalid: %s", s.Port)
	}

	// convert thrift port to int16 and check port in range [0, 65535]
	thriftPort, err := strconv.ParseUint(s.ThriftPort, 10, 16)
	if err != nil {
		return fmt.Errorf("thrift_port is invalid: %s", s.ThriftPort)
	}
	if thriftPort > 65535 {
		return fmt.Errorf("thrift_port is invalid: %s", s.ThriftPort)
	}

	if s.User == "" {
		return fmt.Errorf("user is empty")
	}

	if s.Database == "" {
		return fmt.Errorf("database is empty")
	}

	return nil
}

func (s *Spec) String() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/", s.User, s.Password, s.Host, s.Port)
}

// create mysql connection from spec
func (s *Spec) Connect() (*sql.DB, error) {
	return sql.Open("mysql", s.String())
}

func (s *Spec) ConnectDB() (*sql.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", s.User, s.Password, s.Host, s.Port, s.Database)
	return sql.Open("mysql", dsn)
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
	defer db.Close()

	var _dbNmae string
	var createDBString string
	err = db.QueryRow("SHOW CREATE DATABASE "+s.Database).Scan(&_dbNmae, &createDBString)
	if err != nil {
		return false, err
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
	defer db.Close()

	var tableName string
	var createTableString string
	err = db.QueryRow("SHOW CREATE TABLE "+s.Database+"."+s.Table).Scan(&tableName, &createTableString)
	if err != nil {
		return false, err
	}
	log.Infof("table %s.%s create string: %s", s.Database, s.Table, createTableString)

	// check "binlog.enable" = "true" in create table string
	binlogEnableString := `"binlog.enable" = "true"`
	return strings.Contains(createTableString, binlogEnableString), nil
}

func (s *Spec) GetAllTables() ([]string, error) {
	log.Infof("get all tables in database %s", s.Database)

	db, err := s.ConnectDB()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var table string
		err = rows.Scan(&table)
		if err != nil {
			return nil, err
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
	defer db.Close()

	_, err = db.Exec("DROP TABLE " + s.Database + "." + table)
	return err
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

	return successTables, nil
}

func (s *Spec) ClearDB() error {
	log.Infof("clear database %s", s.Database)

	db, err := s.Connect()
	if err != nil {
		return err
	}
	defer db.Close()

	_, err = db.Exec("DROP DATABASE " + s.Database)
	if err != nil {
		return err
	}

	_, err = db.Exec("CREATE DATABASE " + s.Database)
	return err
}

func (s *Spec) CreateDatabase() error {
	log.Trace("create database")

	db, err := s.Connect()
	if err != nil {
		return nil
	}
	defer db.Close()

	_, err = db.Exec("CREATE DATABASE IF NOT EXISTS " + s.Database)
	return err
}

func (s *Spec) CreateTable(stmt string) error {
	db, err := s.Connect()
	if err != nil {
		return nil
	}
	defer db.Close()

	_, err = db.Exec(stmt)
	return err
}

func (s *Spec) CheckDatabaseExists() (bool, error) {
	log.Trace("check database exist by spec", zap.String("spec", s.String()))
	db, err := s.Connect()
	if err != nil {
		return false, err
	}
	defer db.Close()

	rows, err := db.Query("SHOW DATABASES LIKE '" + s.Database + "'")
	if err != nil {
		return false, err
	}
	defer rows.Close()

	var database string
	for rows.Next() {
		if err := rows.Scan(&database); err != nil {
			return false, err
		}
	}
	if err := rows.Err(); err != nil {
		return false, err
	}

	return database != "", nil
}

// check table exits in database dir by spec
func (s *Spec) CheckTableExists() (bool, error) {
	log.Trace("check table exists", zap.String("table", s.Table))

	db, err := s.Connect()
	if err != nil {
		return false, err
	}
	defer db.Close()

	rows, err := db.Query("SHOW TABLES FROM " + s.Database + " LIKE '" + s.Table + "'")
	if err != nil {
		return false, err
	}
	defer rows.Close()

	var table string
	for rows.Next() {
		if err := rows.Scan(&table); err != nil {
			return false, err
		}
	}
	if err := rows.Err(); err != nil {
		return false, err
	}

	return table != "", nil
}

// mysql> BACKUP SNAPSHOT ccr.snapshot_20230605 TO `__keep_on_local__` ON (      src_1 ) PROPERTIES ("type" = "full");
func (s *Spec) CreateSnapshotAndWaitForDone() (string, error) {
	// snapshot name format "ccr_snapshot_${db}_${table}_${timestamp}"
	snapshotName := fmt.Sprintf("ccr_snapshot_%s_%s_%d", s.Database, s.Table, time.Now().UnixNano())
	table := s.Table

	log.Infof("create snapshot %s.%s", s.Database, snapshotName)

	db, err := s.Connect()
	if err != nil {
		return "", err
	}
	defer db.Close()

	backupSnapshotSql := fmt.Sprintf("BACKUP SNAPSHOT %s.%s TO `__keep_on_local__` ON ( %s ) PROPERTIES (\"type\" = \"full\")", s.Database, snapshotName, table)
	log.Debugf("backup snapshot sql: %s", backupSnapshotSql)
	_, err = db.Exec(backupSnapshotSql)
	if err != nil {
		return "", err
	}

	backupFinished, err := s.CheckBackupFinished(snapshotName)
	if err != nil {
		log.Errorf("check backup state failed, err: %v", err)
		return "", err
	}
	if !backupFinished {
		err = fmt.Errorf("check backup state timeout, max try times: %d", MAX_CHECK_RETRY_TIMES)
		return "", err
	}

	return snapshotName, nil
}

func makeSingleColScanArgs[T interface{}](prefix int, col *T, suffix int) []interface{} {
	prefixCols := make([]sql.RawBytes, prefix)
	suffixCols := make([]sql.RawBytes, suffix)
	scanArgs := make([]interface{}, 0, prefix+suffix+1)
	for i := range prefixCols {
		scanArgs = append(scanArgs, &prefixCols[i])
	}
	scanArgs = append(scanArgs, col)
	for i := range suffixCols {
		scanArgs = append(scanArgs, &suffixCols[i])
	}

	return scanArgs
}

// TODO: Add TaskErrMsg
func (s *Spec) checkBackupFinished(snapshotName string) (BackupState, error) {
	log.Infof("check backup state of snapshot %s", snapshotName)

	db, err := s.Connect()
	if err != nil {
		return BackupStateUnknown, err
	}
	defer db.Close()

	var backupStateStr string
	scanArgs := makeSingleColScanArgs(3, &backupStateStr, 10)

	sql := fmt.Sprintf("SHOW BACKUP FROM %s WHERE SnapshotName = \"%s\"", s.Database, snapshotName)
	log.Tracef("check backup state sql: %s", sql)
	rows, err := db.Query(sql)
	if err != nil {
		return BackupStateUnknown, err
	}
	defer rows.Close()

	if rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			log.Fatal(err)
			return BackupStateUnknown, err
		}

		log.Tracef("check backup state: %v", backupStateStr)

		return ParseBackupState(backupStateStr), nil
	}
	return BackupStateUnknown, fmt.Errorf("no backup state found")
}

func (s *Spec) CheckBackupFinished(snapshotName string) (bool, error) {
	log.Trace("check backup state", zap.String("database", s.Database))

	for i := 0; i < MAX_CHECK_RETRY_TIMES; i++ {
		if backupState, err := s.checkBackupFinished(snapshotName); err != nil {
			return false, err
		} else if backupState == BackupStateFinished {
			return true, nil
		} else if backupState == BackupStateCancelled {
			return false, fmt.Errorf("backup failed or canceled")
		} else {
			// BackupStatePending, BackupStateUnknown
			time.Sleep(BACKUP_CHECK_DURATION)
		}
	}

	return false, fmt.Errorf("check backup state timeout, max try times: %d", MAX_CHECK_RETRY_TIMES)
}

// TODO: Add TaskErrMsg
func (s *Spec) checkRestoreFinished(snapshotName string) (RestoreState, error) {
	log.Tracef("check restore state %s", snapshotName)

	db, err := s.Connect()
	if err != nil {
		return RestoreStateUnknown, err
	}
	defer db.Close()

	var restoreStateStr string
	scanArgs := makeSingleColScanArgs(4, &restoreStateStr, 16)

	sql := fmt.Sprintf("SHOW RESTORE FROM %s WHERE Label = \"%s\"", s.Database, snapshotName)

	log.Tracef("check restore state sql: %s", sql)
	rows, err := db.Query(sql)
	if err != nil {
		return RestoreStateUnknown, err
	}
	defer rows.Close()

	if rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			log.Fatal(err)
			return RestoreStateUnknown, err
		}

		log.Tracef("check restore state: %v", restoreStateStr)

		return ParseRestoreState(restoreStateStr), nil
	}
	return RestoreStateUnknown, fmt.Errorf("no restore state found")
}

func (s *Spec) CheckRestoreFinished(snapshotName string) (bool, error) {
	log.Trace("check restore isfinished", zap.String("database", s.Database))

	for i := 0; i < MAX_CHECK_RETRY_TIMES; i++ {
		if backupState, err := s.checkRestoreFinished(snapshotName); err != nil {
			return false, err
		} else if backupState == RestoreStateFinished {
			return true, nil
		} else if backupState == RestoreStateCancelled {
			return false, fmt.Errorf("backup failed or canceled")
		} else {
			// RestoreStatePending, RestoreStateUnknown
			time.Sleep(RESTORE_CHECK_DURATION)
		}
	}

	return false, fmt.Errorf("check restore state timeout, max try times: %d", MAX_CHECK_RETRY_TIMES)
}
