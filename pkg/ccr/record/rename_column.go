package record

import (
	"encoding/json"
	"fmt"

	"github.com/selectdb/ccr_syncer/pkg/xerror"
)

type RenameColumn struct {
	DbId                   int64           `json:"dbId"`
	TableId                int64           `json:"tableId"`
	ColName                string          `json:"colName"`
	NewColName             string          `json:"newColName"`
	IndexIdToSchemaVersion map[int64]int32 `json:"indexIdToSchemaVersion"`
}

func NewRenameColumnFromJson(data string) (*RenameColumn, error) {
	var renameColumn RenameColumn
	err := json.Unmarshal([]byte(data), &renameColumn)
	if err != nil {
		return nil, xerror.Wrap(err, xerror.Normal, "unmarshal rename column error")
	}

	if renameColumn.TableId == 0 {
		return nil, xerror.Errorf(xerror.Normal, "table id not found")
	}

	return &renameColumn, nil
}

// Stringer
func (r *RenameColumn) String() string {
	return fmt.Sprintf("RenameColumn: DbId: %d, TableId: %d, ColName: %s, NewColName: %s, IndexIdToSchemaVersion: %v", r.DbId, r.TableId, r.ColName, r.NewColName, r.IndexIdToSchemaVersion)
}
