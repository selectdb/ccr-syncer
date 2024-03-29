package record

import (
	"encoding/json"
	"fmt"

	"github.com/selectdb/ccr_syncer/pkg/xerror"
)

type CreateTable struct {
	DbId    int64  `json:"dbId"`
	TableId int64  `json:"tableId"`
	Sql     string `json:"sql"`
}

func NewCreateTableFromJson(data string) (*CreateTable, error) {
	var createTable CreateTable
	err := json.Unmarshal([]byte(data), &createTable)
	if err != nil {
		return nil, xerror.Wrap(err, xerror.Normal, "unmarshal create table error")
	}

	if createTable.Sql == "" {
		// TODO: fallback to create sql from other fields
		return nil, xerror.Errorf(xerror.Normal, "create table sql is empty")
	}

	if createTable.TableId == 0 {
		return nil, xerror.Errorf(xerror.Normal, "table id not found")
	}

	return &createTable, nil
}

// String
func (c *CreateTable) String() string {
	return fmt.Sprintf("CreateTable: DbId: %d, TableId: %d, Sql: %s", c.DbId, c.TableId, c.Sql)
}
