package handle

import (
	"github.com/selectdb/ccr_syncer/pkg/ccr"
	"github.com/selectdb/ccr_syncer/pkg/ccr/record"
	festruct "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/frontendservice"
)

func init() {
	ccr.RegisterJobHandle[*record.DropRollup](festruct.TBinlogType_DROP_ROLLUP, &DropRollupHandle{})
}

type DropRollupHandle struct {
}

func (h *DropRollupHandle) IsIdempotent() bool {
	return false
}

func (h *DropRollupHandle) IsBinlogCommitted(j *ccr.Job, record *record.DropRollup) (bool, error) {
	destTableName, err := j.GetDestNameBySrcId(record.TableId)
	if err != nil {
		return false, err
	}

	descResult, err := j.GetDestMeta().DescribeTableAll(destTableName)
	if err != nil {
		return false, err
	}

	if _, ok := descResult[record.IndexName]; ok {
		return false, nil
	}

	return true, nil
}

func (h *DropRollupHandle) Handle(j *ccr.Job, commitSeq int64, dropRollup *record.DropRollup) error {
	var destTableName string
	if j.SyncType == ccr.TableSync {
		destTableName = j.Dest.Table
	} else {
		destTableName = dropRollup.TableName
	}

	return j.IDest.DropRollup(destTableName, dropRollup.IndexName)
}
