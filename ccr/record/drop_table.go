package record

import (
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"
)

type DropTable struct {
	DbId    int64 `json:"dbId"`
	TableId int64 `json:"tableId"`
}

func NewDropTableFromJson(data string) (*DropTable, error) {
	var dropTable DropTable
	err := json.Unmarshal([]byte(data), &dropTable)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshal drop table error")
	}

	if dropTable.TableId == 0 {
		return nil, errors.Errorf("table id not found")
	}

	return &dropTable, nil
}

// String
func (c *DropTable) String() string {
	return fmt.Sprintf("DropTable: DbId: %d, TableId: %d", c.DbId, c.TableId)
}
