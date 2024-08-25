package base

import (
	"github.com/selectdb/ccr_syncer/pkg/ccr/record"
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
	IsDatabaseEnableBinlog() (bool, error)
	IsTableEnableBinlog() (bool, error)
	GetAllTables() ([]string, error)
	GetAllViewsFromTable(tableName string) ([]string, error)
	ClearDB() error
	CreateDatabase() error
	CreateTableOrView(createTable *record.CreateTable, srcDatabase string) error
	CheckDatabaseExists() (bool, error)
	CheckTableExists() (bool, error)
	CreatePartialSnapshotAndWaitForDone(table string, partitions []string) (string, error)
	CreateSnapshotAndWaitForDone(tables []string) (string, error)
	CheckRestoreFinished(snapshotName string) (bool, error)
	GetRestoreSignatureNotMatchedTable(snapshotName string) (string, error)

	LightningSchemaChange(srcDatabase string, changes *record.ModifyTableAddOrDropColumns) error
	TruncateTable(destTableName string, truncateTable *record.TruncateTable) error
	DropTable(tableName string, force bool) error
	DropView(viewName string) error

	AddPartition(destTableName string, addPartition *record.AddPartition) error
	DropPartition(destTableName string, dropPartition *record.DropPartition) error

	DesyncTables(tables ...string) error

	utils.Subject[SpecEvent]
}
