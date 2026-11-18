package handle

import (
	"strings"

	"github.com/selectdb/ccr_syncer/pkg/ccr"
	"github.com/selectdb/ccr_syncer/pkg/ccr/record"
	festruct "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/frontendservice"
	log "github.com/sirupsen/logrus"
)

func init() {
	ccr.RegisterJobHandle[*record.AlterView](festruct.TBinlogType_MODIFY_VIEW_DEF, &AlterViewDefHandle{})
}

type AlterViewDefHandle struct {
	// The alter view binlog is idempotent
	IdempotentJobHandle[*record.AlterView]
}

func (h *AlterViewDefHandle) Handle(j *ccr.Job, commitSeq int64, alterView *record.AlterView) error {
	viewName, err := j.GetDestNameBySrcId(alterView.TableId)
	if err != nil {
		return err
	}

	if err := j.IDest.AlterViewDef(j.Src.Database, viewName, alterView); err != nil {
		if strings.Contains(err.Error(), "Unknown column") {
			log.Warnf("alter view but the column is not found, trigger partial snapshot, commit seq: %d, msg: %s",
				commitSeq, err.Error())
			replace := false
			isView := true
			return j.NewPartialSnapshot(alterView.TableId, viewName, nil, replace, isView)
		}
	}
	return nil
}
