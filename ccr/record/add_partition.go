package record

import (
	"encoding/json"

	"github.com/pkg/errors"
)

type AddPartition struct {
	TableId int64  `json:"tableId"`
	Sql     string `json:"sql"`
}

func NewAddPartitionFromJson(data string) (*AddPartition, error) {
	var addPartition AddPartition
	err := json.Unmarshal([]byte(data), &addPartition)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshal add partition error")
	}

	if addPartition.Sql == "" {
		// TODO: fallback to create sql from other fields
		return nil, errors.Errorf("add partition sql is empty")
	}

	if addPartition.TableId == 0 {
		return nil, errors.Errorf("table id not found")
	}

	return &addPartition, nil
}
