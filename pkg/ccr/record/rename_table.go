package record

import (
	"encoding/json"
	"fmt"

	"github.com/selectdb/ccr_syncer/pkg/xerror"
)

type RenameTable struct {
	DbId            int64  `json:"db"`
	TableId         int64  `json:"tb"`
	IndexId         int64  `json:"ind"`
	ParititonId     int64  `json:"p"`
	NewTableName    string `json:"nT"`
	OldTableName    string `json:"oT"`
	NewRollupName   string `json:"nR"`
	OldRollupName   string `json:"oR"`
	NewParitionName string `json:"nP"`
	OldParitionName string `json:"oP"`
}

func NewRenameTableFromJson(data string) (*RenameTable, error) {
	var renameTable RenameTable
	err := json.Unmarshal([]byte(data), &renameTable)
	if err != nil {
		return nil, xerror.Wrap(err, xerror.Normal, "unmarshal rename table error")
	}

	if renameTable.TableId == 0 {
		return nil, xerror.Errorf(xerror.Normal, "table id not found")
	}

	return &renameTable, nil
}

// Stringer
func (r *RenameTable) String() string {
	return fmt.Sprintf("RenameTable: DbId: %d, TableId: %d, ParititonId: %d, IndexId: %d, NewTableName: %s, OldTableName: %s, NewRollupName: %s, OldRollupName: %s, NewParitionName: %s, OldParitionName: %s", r.DbId, r.TableId, r.ParititonId, r.IndexId, r.NewTableName, r.OldTableName, r.NewRollupName, r.OldRollupName, r.NewParitionName, r.OldParitionName)
}
