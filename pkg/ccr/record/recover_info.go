package record

import (
	"encoding/json"
	"fmt"

	"github.com/selectdb/ccr_syncer/pkg/xerror"
)

type RecoverInfo struct {
	DbId             int64  `json:"dbId"`
	NewDbName        string `json:"newDbName"`
	TableId          int64  `json:"tableId"`
	TableName        string `json:"tableName"`
	NewTableName     string `json:"newTableName"`
	PartitionId      int64  `json:"partitionId"`
	PartitionName    string `json:"partitionName"`
	NewPartitionName string `json:"newPartitionName"`
}

func NewRecoverInfoFromJson(data string) (*RecoverInfo, error) {
	var recoverInfo RecoverInfo
	err := json.Unmarshal([]byte(data), &recoverInfo)
	if err != nil {
		return nil, xerror.Wrap(err, xerror.Normal, "unmarshal create table error")
	}

	if recoverInfo.TableId == 0 {
		return nil, xerror.Errorf(xerror.Normal, "table id not found")
	}

	// table name must exist. partition name not checked since optional.
	if recoverInfo.TableName == "" {
		return nil, xerror.Errorf(xerror.Normal, "Table Name can not be null")
	}
	return &recoverInfo, nil
}

func (c *RecoverInfo) IsRecoverTable() bool {
	if c.PartitionName == "" || c.PartitionId == -1 {
		return true
	}
	return false
}

// String
func (c *RecoverInfo) String() string {
	return fmt.Sprintf("RecoverInfo: DbId: %d, NewDbName: %s, TableId: %d, TableName: %s, NewTableName: %s, PartitionId: %d, PartitionName: %s, NewPartitionName: %s",
		c.DbId, c.NewDbName, c.TableId, c.TableName, c.NewTableName, c.PartitionId, c.PartitionName, c.NewPartitionName)
}
