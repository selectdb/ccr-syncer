package record

import (
	"encoding/json"
	"fmt"

	"github.com/selectdb/ccr_syncer/pkg/xerror"
)

type DropRollup struct {
	DbId      int64  `json:"dbId"`
	TableId   int64  `json:"tableId"`
	TableName string `json:"tableName"`
	IndexId   int64  `json:"indexId"`
	IndexName string `json:"indexName"`
}

func NewDropRollupFromJson(data string) (*DropRollup, error) {
	var dropRollup DropRollup
	err := json.Unmarshal([]byte(data), &dropRollup)
	if err != nil {
		return nil, xerror.Wrap(err, xerror.Normal, "unmarshal drop rollup error")
	}

	if dropRollup.TableId == 0 {
		return nil, xerror.Errorf(xerror.Normal, "invalid drop rollup, table id not found")
	}

	if dropRollup.TableName == "" {
		return nil, xerror.Errorf(xerror.Normal, "invalid drop rollup, tableName is empty")
	}

	if dropRollup.IndexName == "" {
		return nil, xerror.Errorf(xerror.Normal, "invalid drop rollup, indexName is empty")
	}

	return &dropRollup, nil
}

func (d *DropRollup) String() string {
	return fmt.Sprintf("DropRollup{DbId: %d, TableId: %d, TableName: %s, IndexId: %d, IndexName: %s}",
		d.DbId, d.TableId, d.TableName, d.IndexId, d.IndexName)
}
