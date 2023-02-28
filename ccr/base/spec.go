package base

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"
)

// TODO(Drogon): timeout config
type Spec struct {
	Host       string `json:"host,required"`
	Port       string `json:"port,required"`
	ThriftPort string `json:"thrift_port,required"`
	User       string `json:"user,required"`
	Password   string `json:"password,required"`
	Cluster    string `json:"cluster,required"`
	Database   string `json:"database,required"`
	Table      string `json:"table,required"`
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

// mysql> BACKUP SNAPSHOT ccr.snapshot_20230605 TO `__keep_on_local__` ON (      src_1 ) PROPERTIES ("type" = "full");
func (s *Spec) CreateSnapshotAndWaitForDone() (string, error) {
	// get unix timestamp
	snapshotName := "ccr_snapshot_" + strconv.FormatInt(time.Now().Unix(), 10)
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

	// TODO(impl): Add wait for done
	time.Sleep(100 * time.Second)

	return snapshotName, nil
}
