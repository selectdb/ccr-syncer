package record

import (
	"encoding/json"
	"fmt"
)

type AddPartition struct {
	TableId int64  `json:"tableId"`
	Sql     string `json:"sql"`
}

func NewAddPartitionFromJson(data string) (*AddPartition, error) {
	var addPartition AddPartition
	err := json.Unmarshal([]byte(data), &addPartition)
	if err != nil {
		return nil, err
	}

	if addPartition.Sql == "" {
		// TODO: fallback to create sql from other fields
		return nil, fmt.Errorf("add partition sql is empty")
	}

	if addPartition.TableId == 0 {
		return nil, fmt.Errorf("table id not found")
	}

	return &addPartition, nil
}
