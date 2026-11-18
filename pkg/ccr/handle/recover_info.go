package handle

import (
	"github.com/selectdb/ccr_syncer/pkg/ccr"
	"github.com/selectdb/ccr_syncer/pkg/ccr/record"
	festruct "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/frontendservice"
	log "github.com/sirupsen/logrus"
)

func init() {
	ccr.RegisterJobHandle[*record.RecoverInfo](festruct.TBinlogType_RECOVER_INFO, &RecoverInfoHandle{})
}

type RecoverInfoHandle struct {
	// The recover info binlog is idempotent
	IdempotentJobHandle[*record.RecoverInfo]
}

func (h *RecoverInfoHandle) Handle(j *ccr.Job, commitSeq int64, recoverInfo *record.RecoverInfo) error {
	if recoverInfo.IsRecoverTable() {
		var tableName string
		if recoverInfo.NewTableName != "" {
			tableName = recoverInfo.NewTableName
		} else {
			tableName = recoverInfo.TableName
		}
		log.Infof("recover info with for table %s, will trigger partial sync", tableName)
		isView := false
		return j.NewPartialSnapshot(recoverInfo.TableId, tableName, nil, true, isView)
	}

	var partitions []string
	if recoverInfo.NewPartitionName != "" {
		partitions = append(partitions, recoverInfo.NewPartitionName)
	} else {
		partitions = append(partitions, recoverInfo.PartitionName)
	}
	log.Infof("recover info with for partition(%s) for table %s, will trigger partial sync",
		partitions, recoverInfo.TableName)
	// if source does multiple recover of partition, then there is a race
	// condition and some recover might miss due to commitseq change after snapshot.
	isView := false
	return j.NewPartialSnapshot(recoverInfo.TableId, recoverInfo.TableName, nil, true, isView)
}
