package record

import (
	"encoding/json"

	"github.com/selectdb/ccr_syncer/pkg/xerror"
)

type AddPartition struct {
	DbId    int64  `json:"dbId"`
	TableId int64  `json:"tableId"`
	Sql     string `json:"sql"`
}

func NewAddPartitionFromJson(data string) (*AddPartition, error) {
	var addPartition AddPartition
	err := json.Unmarshal([]byte(data), &addPartition)
	if err != nil {
		return nil, xerror.Wrap(err, xerror.Normal, "unmarshal add partition error")
	}

	if addPartition.Sql == "" {
		return nil, xerror.Errorf(xerror.Normal, "add partition sql is empty")
	}

	if addPartition.TableId == 0 {
		return nil, xerror.Errorf(xerror.Normal, "table id not found")
	}

	return &addPartition, nil
}
