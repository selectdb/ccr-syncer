package handle

import (
	"github.com/cloudwego/kitex/tool/internal_pkg/log"
	"github.com/selectdb/ccr_syncer/pkg/ccr"
	"github.com/selectdb/ccr_syncer/pkg/ccr/record"
	festruct "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/frontendservice"
)

func init() {
	ccr.RegisterJobHandle[*record.RenameRollup](festruct.TBinlogType_RENAME_ROLLUP, &RenameRollupHandle{})
}

type RenameRollupHandle struct {
	// The rename rollup binlog is idempotent
	IdempotentJobHandle[*record.RenameRollup]
}

func (h *RenameRollupHandle) Handle(j *ccr.Job, commitSeq int64, renameRollup *record.RenameRollup) error {
	log.Infof("handle rename rollup binlog, prevCommitSeq: %d, commitSeq: %d",
		j.GetJobProgress().PrevCommitSeq, j.GetJobProgress().CommitSeq)

	destTableName, err := j.GetDestNameBySrcId(renameRollup.TableId)
	if err != nil {
		return nil
	}

	newRollup := renameRollup.NewRollupName
	oldRollup := renameRollup.OldRollupName
	if oldRollup == "" {
		log.Warnf("old rollup name is empty, sync rollup via partial snapshot, "+
			"new rollup: %s, index id: %d, table id: %d, commit seq: %d",
			newRollup, renameRollup.IndexId, renameRollup.TableId, commitSeq)
		replace := true
		tableName := destTableName
		if j.IsTableSyncWithAlias() {
			tableName = j.Src.Table
		}
		isView := false
		return j.NewPartialSnapshot(renameRollup.TableId, tableName, nil, replace, isView)
	}

	return j.IDest.RenameRollup(destTableName, oldRollup, newRollup)
}
