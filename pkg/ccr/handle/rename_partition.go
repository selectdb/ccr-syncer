package handle

import (
	"github.com/selectdb/ccr_syncer/pkg/ccr"
	"github.com/selectdb/ccr_syncer/pkg/ccr/record"
	festruct "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/frontendservice"
	log "github.com/sirupsen/logrus"
)

func init() {
	ccr.RegisterJobHandle[*record.RenamePartition](festruct.TBinlogType_RENAME_PARTITION, &RenamePartitionHandle{})
}

type RenamePartitionHandle struct {
}

func (h *RenamePartitionHandle) IsBinlogCommitted(j *ccr.Job, record *record.RenamePartition) (bool, error) {
	destTableId, err := j.GetDestTableIdBySrc(record.TableId)
	if err != nil {
		return false, err
	}

	if err := j.GetDestMeta().UpdatePartitions(destTableId); err != nil {
		return false, err
	}

	partitions, err := j.GetDestMeta().GetPartitionIdMap(destTableId)
	if err != nil {
		return false, err
	}

	for _, partition := range partitions {
		if partition.Name == record.NewPartitionName {
			log.Infof("partition %s is not renamed to %s in dest table %d, this binlog is committed",
				record.OldPartitionName, record.NewPartitionName, destTableId)
			return true, nil
		}
	}

	log.Infof("partition %s is renamed to %s in dest table %d, this binlog is not committed",
		record.OldPartitionName, record.NewPartitionName, destTableId)
	return false, nil
}

func (h *RenamePartitionHandle) IsIdempotent() bool {
	return false
}

func (h *RenamePartitionHandle) Handle(j *ccr.Job, commitSeq int64, renamePartition *record.RenamePartition) error {
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
