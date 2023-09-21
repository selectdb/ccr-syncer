package record

import (
	"encoding/json"
	"fmt"

	"github.com/selectdb/ccr_syncer/xerror"
)

type ModifyTableAddOrDropColumns struct {
	DbId    int64  `json:"dbId"`
	TableId int64  `json:"tableId"`
	RawSql  string `json:"rawSql"`
}

func NewModifyTableAddOrDropColumnsFromJson(data string) (*ModifyTableAddOrDropColumns, error) {
	var modifyTableAddOrDropColumns ModifyTableAddOrDropColumns
	err := json.Unmarshal([]byte(data), &modifyTableAddOrDropColumns)
	if err != nil {
		return nil, xerror.Wrap(err, xerror.Normal, "unmarshal modify table add or drop columns error")
	}

	if modifyTableAddOrDropColumns.RawSql == "" {
		// TODO: fallback to create sql from other fields
		return nil, xerror.Errorf(xerror.Normal, "modify table add or drop columns sql is empty")
	}

	if modifyTableAddOrDropColumns.TableId == 0 {
		return nil, xerror.Errorf(xerror.Normal, "table id not found")
	}

	return &modifyTableAddOrDropColumns, nil
}

// String
func (c *ModifyTableAddOrDropColumns) String() string {
	return fmt.Sprintf("ModifyTableAddOrDropColumns: DbId: %d, TableId: %d, RawSql: %s", c.DbId, c.TableId, c.RawSql)
}
