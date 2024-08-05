package record

import (
	"encoding/json"

	"github.com/selectdb/ccr_syncer/pkg/xerror"
)

type ReplacePartitionRecord struct {
	DbId           int64    `json:"dbId"`
	DbName         string   `json:"dbName"`
	TableId        int64    `json:"tblId"`
	TableName      string   `json:"tblName"`
	Partitions     []string `json:"partitions"`
	TempPartitions []string `json:"tempPartitions"`
	StrictRange    bool     `json:"strictRange"`
	UseTempName    bool     `json:"useTempPartitionName"`
}

func NewReplacePartitionFromJson(data string) (*ReplacePartitionRecord, error) {
	var replacePartition ReplacePartitionRecord
	err := json.Unmarshal([]byte(data), &replacePartition)
	if err != nil {
		return nil, xerror.Wrap(err, xerror.Normal, "unmarshal replace partition error")
	}

	if len(replacePartition.TempPartitions) == 0 {
		return nil, xerror.Errorf(xerror.Normal, "the temp partitions of the replace partition record is empty")
	}

	if replacePartition.TableId == 0 {
		return nil, xerror.Errorf(xerror.Normal, "table id not found")
	}

	if replacePartition.TableName == "" {
		return nil, xerror.Errorf(xerror.Normal, "table name is empty")
	}

	return &replacePartition, nil
}
