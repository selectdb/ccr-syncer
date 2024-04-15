package base

import (
	"database/sql"

	"github.com/selectdb/ccr_syncer/pkg/utils"
)

type SpecEvent int

const (
	feNotMasterEvent  SpecEvent = 0
	httpNotFoundEvent SpecEvent = 1
)

// this interface is used to for spec operation, treat it as a mysql dao
type Specer interface {
	Valid() error
	Connect() (*sql.DB, error)
	ConnectDB() (*sql.DB, error)
	IsDatabaseEnableBinlog() (bool, error)
	IsTableEnableBinlog() (bool, error)
	GetAllTables() ([]string, error)
	ClearDB() error
	CreateDatabase() error
	CreateTable(stmt string) error
	CheckDatabaseExists() (bool, error)
	CheckTableExists() (bool, error)
	CreateSnapshotAndWaitForDone(tables []string) (string, error)
	CheckRestoreFinished(snapshotName string) (bool, error)
	GetRestoreSignatureNotMatchedTable(snapshotName string) (string, error)
	WaitTransactionDone(txnId int64) // busy wait

	Exec(sql string) error
	DbExec(sql string) error

	utils.Subject[SpecEvent]
}
