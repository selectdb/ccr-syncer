package handle

import (
	"github.com/selectdb/ccr_syncer/pkg/ccr"
	"github.com/selectdb/ccr_syncer/pkg/ccr/record"
	festruct "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/frontendservice"
)

func init() {
	ccr.RegisterJobHandle[*record.ModifyTableProperty](festruct.TBinlogType_MODIFY_PARTITIONS, &ModifyTablePropertyHandle{})
}

type ModifyTablePropertyHandle struct {
	// The modify table property binlog is idempotent
	IdempotentJobHandle[*record.ModifyTableProperty]
}

func (h *ModifyTablePropertyHandle) Handle(j *ccr.Job, commitSeq int64, modifyProperty *record.ModifyTableProperty) error {
	destTableName, err := j.GetDestNameBySrcId(modifyProperty.TableId)
	if err != nil {
		return err
	}
	return j.Dest.ModifyTableProperty(destTableName, modifyProperty)
}
