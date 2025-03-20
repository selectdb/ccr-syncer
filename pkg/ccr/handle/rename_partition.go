package handle

import (
	"github.com/cloudwego/kitex/tool/internal_pkg/log"
	"github.com/selectdb/ccr_syncer/pkg/ccr"
	"github.com/selectdb/ccr_syncer/pkg/ccr/record"
	festruct "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/frontendservice"
)

func init() {
	ccr.RegisterJobHandle[*record.RenamePartition](festruct.TBinlogType_ADD_PARTITION, &RenamePartitionHandle{})
}

type RenamePartitionHandle struct {
	// The adding partition binlog is idempotent
	IdempotentJobHandle[*record.RenamePartition]
}

func (h *RenamePartitionHandle) Handle(j *ccr.Job, commitSeq int64, renamePartition *record.RenamePartition) error {
	log.Infof("handle rename partition binlog, prevCommitSeq: %d, commitSeq: %d",
		j.GetJobProgress().PrevCommitSeq, j.GetJobProgress().CommitSeq)

	if j.IsBinlogCommitted(renamePartition.TableId, commitSeq) {
		return nil
	}

	destTableName, err := j.GetDestNameBySrcId(renamePartition.TableId)
	if err != nil {
		return err
	}

	newPartition := renamePartition.NewPartitionName
	oldPartition := renamePartition.OldPartitionName
	if oldPartition == "" {
		log.Warnf("old partition name is empty, sync partition via partial snapshot, "+
			"new partition: %s, partition id: %d, table id: %d, commit seq: %d",
			newPartition, renamePartition.PartitionId, renamePartition.TableId, commitSeq)
		replace := true
		tableName := destTableName
		if j.IsTableSyncWithAlias() {
			tableName = j.Src.Table
		}
		isView := false
		return j.NewPartialSnapshot(renamePartition.TableId, tableName, nil, replace, isView)
	}
	return j.IDest.RenamePartition(destTableName, oldPartition, newPartition)
}
