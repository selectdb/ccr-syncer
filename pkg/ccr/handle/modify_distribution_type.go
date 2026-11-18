package handle

import (
	"github.com/selectdb/ccr_syncer/pkg/ccr"
	"github.com/selectdb/ccr_syncer/pkg/ccr/record"
	festruct "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/frontendservice"
)

func init() {
	ccr.RegisterJobHandle[*record.ModifyDistributionType](festruct.TBinlogType_MODIFY_DISTRIBUTION_TYPE, &ModifyDistributionTypeHandle{})
}

type ModifyDistributionTypeHandle struct {
}

func (h *ModifyDistributionTypeHandle) IsBinlogCommitted(j *ccr.Job, r *record.ModifyDistributionType) (bool, error) {
	j.GetDestMeta().GetTable(r.GetTableId())
	destTableName, err := j.GetDestNameBySrcId(r.GetTableId())
	if err != nil {
		return false, err
	}
	return j.CheckCreateTable(destTableName, "DISTRIBUTED BY RANDOM")
}

func (h *ModifyDistributionTypeHandle) IsIdempotent() bool {
	return false
}

func (h *ModifyDistributionTypeHandle) Handle(j *ccr.Job, commitSeq int64, record *record.ModifyDistributionType) error {
	destTableName, err := j.GetDestNameBySrcId(record.TableId)
	if err != nil {
		return err
	}

	return j.IDest.ModifyDistributionType(destTableName)
}
