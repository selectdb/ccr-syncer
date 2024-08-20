package record

import (
	"encoding/json"
	"fmt"

	"github.com/selectdb/ccr_syncer/pkg/xerror"
)

type DropTable struct {
	DbId      int64  `json:"dbId"`
	TableId   int64  `json:"tableId"`
	TableName string `json:"tableName"`
	IsView    bool   `json:"isView"`
	RawSql    string `json:"rawSql"`
}

func NewDropTableFromJson(data string) (*DropTable, error) {
	var dropTable DropTable
	err := json.Unmarshal([]byte(data), &dropTable)
	if err != nil {
		return nil, xerror.Wrap(err, xerror.Normal, "unmarshal drop table error")
	}

	if dropTable.TableId == 0 {
		return nil, xerror.Errorf(xerror.Normal, "table id not found")
	}

	return &dropTable, nil
}

// Stringer, all fields
func (c *DropTable) String() string {
	return fmt.Sprintf("DropTable: DbId: %d, TableId: %d, TableName: %s, IsView: %t, RawSql: %s", c.DbId, c.TableId, c.TableName, c.IsView, c.RawSql)
}
