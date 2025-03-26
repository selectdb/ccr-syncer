package handle

import (
	"github.com/selectdb/ccr_syncer/pkg/ccr"
	"github.com/selectdb/ccr_syncer/pkg/ccr/record"
	festruct "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/frontendservice"
)

func init() {
	ccr.RegisterJobHandle[*record.AlterView](festruct.TBinlogType_MODIFY_VIEW_DEF, &AlterViewHandle{})
}

type AlterViewHandle struct {
	// The alter view binlog is idempotent
	IdempotentJobHandle[*record.AlterView]
}

func (h *AlterViewHandle) Handle(j *ccr.Job, commitSeq int64, alterView *record.AlterView) error {
	viewName, err := j.GetDestNameBySrcId(alterView.TableId)
	if err != nil {
		return err
	}

	return j.IDest.AlterViewDef(j.Src.Database, viewName, alterView)
}
