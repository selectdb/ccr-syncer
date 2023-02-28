package ccr

import (
	"database/sql"
	"fmt"
	"regexp"
	"strconv"

	log "github.com/sirupsen/logrus"
	"go.uber.org/zap"

	_ "github.com/go-sql-driver/mysql"
)

// TODO(Drogon): add cluster
// TODO(Drogon): timeout config
type TableSpec struct {
	Host     string
	Port     string
	User     string
	Password string
	Database string
	Table    string
}

// valid table spec
func (t *TableSpec) Valid() error {
	if t.Host == "" {
		return fmt.Errorf("host is empty")
	}

	// convert port to int16 and check port in range [0, 65535]
	port, err := strconv.ParseUint(t.Port, 10, 16)
	if err != nil {
		return fmt.Errorf("port is invalid: %s", t.Port)
	}
	if port > 65535 {
		return fmt.Errorf("port is invalid: %s", t.Port)
	}

	if t.User == "" {
		return fmt.Errorf("user is empty")
	}

	if t.Database == "" {
		return fmt.Errorf("database is empty")
	}

	if t.Table == "" {
		return fmt.Errorf("table is empty")
	}
	return nil
}

func (t *TableSpec) String() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", t.User, t.Password, t.Host, t.Port, t.Database)
}

// create mysql connection from table spec
func (t *TableSpec) Connect() (*sql.DB, error) {
	return sql.Open("mysql", t.String())
}

type Creator struct {
	Src  TableSpec
	Dest TableSpec
}

func NewCreator(src, dest TableSpec) (*Creator, error) {
	// check src dest valid
	if err := src.Valid(); err != nil {
		return nil, fmt.Errorf("src table spec is invalid: %s", err)
	}
	if err := dest.Valid(); err != nil {
		return nil, fmt.Errorf("dest table spec is invalid: %s", err)
	}

	return &Creator{
		Src:  src,
		Dest: dest,
	}, nil
}

// show remote table create statement, return table name and create statement
func (c *Creator) SrcShowCreateTable() (string, string, error) {
	db, err := c.Src.Connect()
	if err != nil {
		return "", "", err
	}
	defer db.Close()

	rows, err := db.Query("SHOW CREATE TABLE " + c.Src.Table)
	if err != nil {
		return "", "", err
	}
	defer rows.Close()

	var tableName, createTableStmt string
	for rows.Next() {
		if err := rows.Scan(&tableName, &createTableStmt); err != nil {
			return "", "", err
		}
	}
	if err := rows.Err(); err != nil {
		return "", "", err
	}

	return tableName, createTableStmt, nil
}

// FIXME(Drogon): check for more corner cases
// use regex replace remote create table stmt by src table db.table
func (c *Creator) ReplaceSrcCreateTableStmt(stmt string) string {
	spec := &c.Dest
	pattern := "^CREATE TABLE ([^\\s]+)"
	// replaceStr := "CREATE TABLE demo.ccr"
	replaceStr := fmt.Sprintf("CREATE TABLE %s.%s", spec.Database, spec.Table)

	re := regexp.MustCompile(pattern)
	replacedStmt := re.ReplaceAllString(stmt, replaceStr)

	return replacedStmt
}

// create database by dest table spec
func (c *Creator) DestCreateDatabase() error {
	db, err := c.Dest.Connect()
	if err != nil {
		return nil
	}
	defer db.Close()

	_, err = db.Exec("CREATE DATABASE IF NOT EXISTS " + c.Dest.Database)
	return err
}

// create table by replace remote create table stmt
func (c *Creator) DestCreateTable(stmt string) error {
	db, err := c.Dest.Connect()
	if err != nil {
		return nil
	}
	defer db.Close()

	_, err = db.Exec(stmt)
	return err
}

// check dest database exists
func (c *Creator) CheckDestDatabaseExists() (bool, error) {
	db, err := c.Dest.Connect()
	if err != nil {
		return false, err
	}
	defer db.Close()

	rows, err := db.Query("SHOW DATABASES LIKE '" + c.Dest.Database + "'")
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

// check dest table exits in database dir
func (c *Creator) CheckDestTableExists() (bool, error) {
	db, err := c.Dest.Connect()
	if err != nil {
		return false, err
	}
	defer db.Close()

	rows, err := db.Query("SHOW TABLES FROM " + c.Dest.Database + " LIKE '" + c.Dest.Table + "'")
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

// check src database && table exists
func (c *Creator) CheckSrcTableExists() (bool, error) {
	db, err := c.Src.Connect()
	if err != nil {
		return false, err
	}
	defer db.Close()

	rows, err := db.Query("SHOW TABLES FROM " + c.Src.Database + " LIKE '" + c.Src.Table + "'")
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

// enable table ccr
// TODO(Drogon): impl
func (c *Creator) EnableTableCcr(spec *TableSpec) error {
	return nil
}

func (c *Creator) Create() error {
	log.Info("start create table", zap.String("src", c.Src.String()), zap.String("dest", c.Dest.String()))
	// step 1: check dest database exists
	var err error
	dest_db_exists := false
	if dest_db_exists, err = c.CheckDestDatabaseExists(); err != nil {
		return err
	}

	// step 2: check dest table exists, if exist, return error
	if dest_table_exists, err := c.CheckDestTableExists(); err != nil {
		return err
	} else if dest_table_exists {
		return fmt.Errorf("dest table %s.%s already exists", c.Dest.Database, c.Dest.Table)
	}

	// step 3: check src table exists, if not exist, return error
	if exists, err := c.CheckSrcTableExists(); err != nil {
		return err
	} else if !exists {
		return fmt.Errorf("src table %s.%s not exists", c.Src.Database, c.Src.Table)
	}

	// step 4: show create table from src table
	var stmt string
	if _, stmt, err = c.SrcShowCreateTable(); err != nil {
		return err
	}

	// step 5: replace create table stmt by dest table spec
	stmt = c.ReplaceSrcCreateTableStmt(stmt)

	// step 6: create table by dest table spec
	if !dest_db_exists {
		if err := c.DestCreateDatabase(); err != nil {
			return err
		}
	}
	if err := c.DestCreateTable(stmt); err != nil {
		return err
	}

	// step 7: enable remote table ccr
	if err := c.EnableTableCcr(&c.Src); err != nil {
		return fmt.Errorf("enable remote table %s.%s ccr failed, err: %v", c.Dest.Database, c.Dest.Table, err)
	}

	// step 8: enable dest table ccr
	if err := c.EnableTableCcr(&c.Dest); err != nil {
		return fmt.Errorf("enable dest table %s.%s ccr failed, err: %v", c.Dest.Database, c.Dest.Table, err)
	}

	return nil
}
