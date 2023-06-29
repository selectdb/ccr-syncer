package record

import (
	"encoding/json"
	"fmt"
)

type DropPartition struct {
	TableId int64  `json:"tableId"`
	Sql     string `json:"sql"`
}

func NewDropPartitionFromJson(data string) (*DropPartition, error) {
	var dropPartition DropPartition
	err := json.Unmarshal([]byte(data), &dropPartition)
	if err != nil {
		return nil, err
	}

	if dropPartition.Sql == "" {
		// TODO: fallback to create sql from other fields
		return nil, fmt.Errorf("drop partition sql is empty")
	}

	if dropPartition.TableId == 0 {
		return nil, fmt.Errorf("table id not found")
	}

	return &dropPartition, nil
}
