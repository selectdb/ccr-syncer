package handle

import (
	"github.com/selectdb/ccr_syncer/pkg/ccr"
	"github.com/selectdb/ccr_syncer/pkg/ccr/record"
	festruct "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/frontendservice"
	log "github.com/sirupsen/logrus"
)

func init() {
	ccr.RegisterJobHandle[*record.RenameRollup](festruct.TBinlogType_RENAME_ROLLUP, &RenameRollupHandle{})
}

type RenameRollupHandle struct {
}

func (h *RenameRollupHandle) IsBinlogCommitted(j *ccr.Job, record *record.RenameRollup) (bool, error) {
	destTableName, err := j.GetDestNameBySrcId(record.TableId)
	if err != nil {
		log.Errorf("get dest table name by src id %d failed, err: %v", record.TableId, err)
		return false, err
	}

	descResult, err := j.GetDestMeta().DescribeTableAll(destTableName)
	if err != nil {
		return false, err
	}

	if _, ok := descResult[record.NewRollupName]; !ok {
		log.Infof("rollup %s is not renamed to %s in dest table %s, this binlog is not committed",
			record.OldRollupName, record.NewRollupName, destTableName)
		return false, nil
	}

	log.Infof("rollup %s is renamed to %s in dest table %s, this binlog is committed",
		record.OldRollupName, record.NewRollupName, destTableName)
	return true, nil
}

func (h *RenameRollupHandle) IsIdempotent() bool {
	return false
}

func (h *RenameRollupHandle) Handle(j *ccr.Job, commitSeq int64, renameRollup *record.RenameRollup) error {
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
