package record

import (
	"encoding/json"
	"fmt"

	"github.com/selectdb/ccr_syncer/pkg/xerror"
)

type ReplaceTableRecord struct {
	DbId            int64  `json:"dbId"`
	OriginTableId   int64  `json:"origTblId"`
	OriginTableName string `json:"origTblName"`
	NewTableId      int64  `json:"newTblName"`
	NewTableName    string `json:"actualNewTblName"`
	SwapTable       bool   `json:"swapTable"`
	IsForce         bool   `json:"isForce"`
}

func NewReplaceTableRecordFromJson(data string) (*ReplaceTableRecord, error) {
	record := &ReplaceTableRecord{}
	err := json.Unmarshal([]byte(data), record)
	if err != nil {
		return nil, xerror.Wrap(err, xerror.Normal, "unmarshal replace table record error")
	}

	if record.OriginTableId == 0 {
		return nil, xerror.Errorf(xerror.Normal, "table id of replace table record not found")
	}

	if record.OriginTableName == "" {
		return nil, xerror.Errorf(xerror.Normal, "table name of replace table record not found")
	}

	if record.NewTableId == 0 {
		return nil, xerror.Errorf(xerror.Normal, "new table id of replace table record not found")
	}

	if record.NewTableName == "" {
		return nil, xerror.Errorf(xerror.Normal, "new table name of replace table record not found")
	}

	return record, nil
}

// Stringer
func (r *ReplaceTableRecord) String() string {
	return fmt.Sprintf("ReplaceTableRecord: DbId: %d, OriginTableId: %d, OriginTableName: %s, NewTableId: %d, NewTableName: %s, SwapTable: %v, IsForce: %v",
		r.DbId, r.OriginTableId, r.OriginTableName, r.NewTableId, r.NewTableName, r.SwapTable, r.IsForce)
}
