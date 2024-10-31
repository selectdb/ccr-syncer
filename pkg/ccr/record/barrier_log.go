package record

import (
	"encoding/json"

	"github.com/selectdb/ccr_syncer/pkg/xerror"
)

type BarrierLog struct {
	DbId       int64  `json:"dbId"`
	TableId    int64  `json:"tableId"`
	BinlogType int64  `json:"binlogType"`
	Binlog     string `json:"binlog"`
}

func NewBarrierLogFromJson(data string) (*BarrierLog, error) {
	var log BarrierLog
	err := json.Unmarshal([]byte(data), &log)
	if err != nil {
		return nil, xerror.Wrap(err, xerror.Normal, "unmarshal barrier log error")
	}
	return &log, nil
}
