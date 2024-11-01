package record

import (
	"encoding/json"

	"github.com/selectdb/ccr_syncer/pkg/xerror"
)

type DropPartition struct {
	TableId int64  `json:"tableId"`
	Sql     string `json:"sql"`
	IsTemp  bool   `json:"isTempPartition"`
}

func NewDropPartitionFromJson(data string) (*DropPartition, error) {
	var dropPartition DropPartition
	err := json.Unmarshal([]byte(data), &dropPartition)
	if err != nil {
		return nil, xerror.Wrap(err, xerror.Normal, "unmarshal drop partition error")
	}

	if dropPartition.Sql == "" {
		// TODO: fallback to create sql from other fields
		return nil, xerror.Errorf(xerror.Normal, "drop partition sql is empty")
	}

	if dropPartition.TableId == 0 {
		return nil, xerror.Errorf(xerror.Normal, "table id not found")
	}

	return &dropPartition, nil
}
