package record

import (
	"encoding/json"
	"fmt"

	"github.com/selectdb/ccr_syncer/xerror"
)

// {
//   "dbId": 10079,
//   "db": "default_cluster:ccr",
//   "tblId": 77395,
//   "table": "src_1_alias",
//   "isEntireTable": false,
//   "rawSql": "PARTITIONS (src_1_alias)"
// }

type TruncateTable struct {
	DbId          int64  `json:"dbId"`
	DbName        string `json:"db"`
	TableId       int64  `json:"tblId"`
	TableName     string `json:"table"`
	IsEntireTable bool   `json:"isEntireTable"`
	RawSql        string `json:"rawSql"`
}

func NewTruncateTableFromJson(data string) (*TruncateTable, error) {
	var truncateTable TruncateTable
	err := json.Unmarshal([]byte(data), &truncateTable)
	if err != nil {
		return nil, xerror.Wrap(err, xerror.Normal, "unmarshal truncate table error")
	}

	if truncateTable.TableId == 0 {
		return nil, xerror.Errorf(xerror.Normal, "table id not found")
	}

	return &truncateTable, nil
}

// Stringer
func (t *TruncateTable) String() string {
	return fmt.Sprintf("TruncateTable: DbId: %d, Db: %s, TableId: %d, Table: %s, IsEntireTable: %v, RawSql: %s", t.DbId, t.DbName, t.TableId, t.TableName, t.IsEntireTable, t.RawSql)
}
