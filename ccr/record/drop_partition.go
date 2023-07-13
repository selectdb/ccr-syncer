package record

import (
	"encoding/json"

	"github.com/pkg/errors"
)

type DropPartition struct {
	TableId int64  `json:"tableId"`
	Sql     string `json:"sql"`
}

func NewDropPartitionFromJson(data string) (*DropPartition, error) {
	var dropPartition DropPartition
	err := json.Unmarshal([]byte(data), &dropPartition)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshal drop partition error")
	}

	if dropPartition.Sql == "" {
		// TODO: fallback to create sql from other fields
		return nil, errors.Errorf("drop partition sql is empty")
	}

	if dropPartition.TableId == 0 {
		return nil, errors.Errorf("table id not found")
	}

	return &dropPartition, nil
}
