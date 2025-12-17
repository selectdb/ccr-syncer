package handle

import (
	"strings"

	"github.com/selectdb/ccr_syncer/pkg/ccr"
	"github.com/selectdb/ccr_syncer/pkg/ccr/record"
	festruct "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/frontendservice"
	"github.com/selectdb/ccr_syncer/pkg/xerror"
	log "github.com/sirupsen/logrus"
)

func init() {
	ccr.RegisterJobHandle[*record.BatchModifyPartitionsInfo](festruct.TBinlogType_MODIFY_PARTITIONS, &ModifyPartitionsHandle{})
}

type ModifyPartitionsHandle struct {
	// The modify partitions binlog is idempotent
	IdempotentJobHandle[*record.BatchModifyPartitionsInfo]
}

// Filter partitions that have storage medium changes and are not temporary partitions
func filterStorageMediumChanges(infos []*record.ModifyPartitionInfo) []*record.ModifyPartitionInfo {
	filtered := make([]*record.ModifyPartitionInfo, 0)
	for _, info := range infos {
		// Skip temporary partitions (they are not synced)
		if info.IsTempPartition {
			log.Debugf("skip temporary partition %d", info.PartitionId)
			continue
		}
		// Only process partitions with storage medium specified
		if info.DataProperty != nil && info.DataProperty.StorageMedium != "" {
			filtered = append(filtered, info)
		}
	}
	return filtered
}

func (h *ModifyPartitionsHandle) Handle(j *ccr.Job, commitSeq int64, batchModifyPartitionsInfo *record.BatchModifyPartitionsInfo) error {
	// Skip if using fixed storage medium (hdd/ssd)
	// Only process when using same_with_upstream storage medium
	if j.StorageMedium == ccr.StorageMediumHDD ||
		j.StorageMedium == ccr.StorageMediumSSD {
		log.Infof("skip modify partitions for storage_medium is fixed to %s", j.StorageMedium)
		return nil
	}

	// Safety check: ensure we have partition infos to process
	if batchModifyPartitionsInfo == nil || len(batchModifyPartitionsInfo.Infos) == 0 {
		log.Warnf("batch modify partitions info is empty or nil, skip")
		return nil
	}

	// Filter to only process storage medium changes
	filteredInfos := filterStorageMediumChanges(batchModifyPartitionsInfo.Infos)
	if len(filteredInfos) == 0 {
		log.Infof("no storage medium changes in modify partitions binlog, skip")
		return nil
	}
	log.Infof("processing %d partition storage medium changes out of %d total modifications",
		len(filteredInfos), len(batchModifyPartitionsInfo.Infos))

	// Update to use filtered infos
	batchModifyPartitionsInfo.Infos = filteredInfos

	// Get table ID from the first partition info (all partitions should belong to the same table)
	tableId := batchModifyPartitionsInfo.GetTableId()
	if tableId <= 0 {
		log.Warnf("invalid table ID: %d, skip modify partitions", tableId)
		return nil
	}

	// Check if it's a materialized view table
	if isAsyncMv, err := j.IsMaterializedViewTable(tableId); err != nil {
		return err
	} else if isAsyncMv {
		log.Infof("skip modify partitions for materialized view table %d", tableId)
		return nil
	}

	// Get destination table name
	destTableName, err := j.GetDestNameBySrcId(tableId)
	if err != nil {
		errMsg := err.Error()
		// If table not found in mapping, it may not be synced yet or already dropped
		if strings.Contains(errMsg, "not found") || strings.Contains(errMsg, "does not exist") {
			log.Warnf("table %d not found in dest, skip modify partitions: %v", tableId, err)
			return nil
		}
		// Other errors (network, etc.): return error to retry
		return xerror.Wrapf(err, xerror.Normal, "failed to get dest table name for table %d", tableId)
	}

	// Execute modify partition property
	// Note: ModifyPartition only updates FE metadata, actual BE storage migration is async
	// So this SQL won't fail due to backend resource issues
	if err := j.Dest.ModifyPartitionProperty(destTableName, batchModifyPartitionsInfo); err != nil {
		// Return error to let job framework retry (network issues, etc.)
		return xerror.Wrapf(err, xerror.Normal, "modify partition storage medium failed for table %s", destTableName)
	}

	log.Infof("successfully modified storage medium for %d partitions in table %s",
		len(filteredInfos), destTableName)
	return nil
}
