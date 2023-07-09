package record

import (
	"encoding/json"
	"fmt"
)

type DropTable struct {
	DbId    int64 `json:"dbId"`
	TableId int64 `json:"tableId"`
}

func NewDropTableFromJson(data string) (*DropTable, error) {
	var dropTable DropTable
	err := json.Unmarshal([]byte(data), &dropTable)
	if err != nil {
		return nil, err
	}

	if dropTable.TableId == 0 {
		return nil, fmt.Errorf("table id not found")
	}

	return &dropTable, nil
}

// String
func (c *DropTable) String() string {
	return fmt.Sprintf("DropTable: DbId: %d, TableId: %d", c.DbId, c.TableId)
}
