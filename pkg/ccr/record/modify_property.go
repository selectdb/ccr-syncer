package record

import (
	"encoding/json"
	"fmt"

	"github.com/selectdb/ccr_syncer/pkg/xerror"
)

type ModifyTableProperty struct {
	DbId       int64             `json:"dbId"`
	TableId    int64             `json:"tableId"`
	TableName  string            `json:"tableName"`
	Properties map[string]string `json:"properties"`
	Sql        string            `json:"sql"`
}

func (modifyProperty *ModifyTableProperty) Deserialize(data string) error {
	err := json.Unmarshal([]byte(data), &modifyProperty)
	if err != nil {
		return xerror.Wrap(err, xerror.Normal, "unmarshal modify table property error")
	}

	if modifyProperty.TableId == 0 {
		return xerror.Errorf(xerror.Normal, "table id not found")
	}
	return nil
}

func NewModifyTablePropertyFromJson(data string) (*ModifyTableProperty, error) {
	var modifyProperty ModifyTableProperty
	if err := modifyProperty.Deserialize(data); err != nil {
		return nil, err
	}
	return &modifyProperty, nil
}

func (modifyProperty *ModifyTableProperty) GetTableId() int64 {
	return modifyProperty.TableId
}

func (m *ModifyTableProperty) String() string {
	return fmt.Sprintf("ModifyTableProperty: DbId: %d, TableId: %d, TableName: %s, Properties: %v, Sql: %s",
		m.DbId, m.TableId, m.TableName, m.Properties, m.Sql)
}
