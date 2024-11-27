package record

import (
	"encoding/json"
	"fmt"
	"regexp"

	"github.com/selectdb/ccr_syncer/pkg/xerror"
)

type CreateTable struct {
	DbId    int64  `json:"dbId"`
	TableId int64  `json:"tableId"`
	Sql     string `json:"sql"`

	// Below fields was added in doris 2.0.3: https://github.com/apache/doris/pull/26901
	DbName    string `json:"dbName"`
	TableName string `json:"tableName"`
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

func (c *CreateTable) IsCreateView() bool {
	viewRegex := regexp.MustCompile(`(?i)^CREATE(\s+)VIEW`)
	return viewRegex.MatchString(c.Sql)
}

// String
func (c *CreateTable) String() string {
	return fmt.Sprintf("CreateTable: DbId: %d, DbName: %s, TableId: %d, TableName: %s, Sql: %s",
		c.DbId, c.DbName, c.TableId, c.TableName, c.Sql)
}
