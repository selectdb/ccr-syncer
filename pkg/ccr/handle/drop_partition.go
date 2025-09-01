package handle

import (
	"github.com/selectdb/ccr_syncer/pkg/ccr"
	"github.com/selectdb/ccr_syncer/pkg/ccr/record"
	festruct "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/frontendservice"
	log "github.com/sirupsen/logrus"
)

func init() {
	ccr.RegisterJobHandle[*record.DropPartition](festruct.TBinlogType_DROP_PARTITION, &DropPartitionHandle{})
}

type DropPartitionHandle struct {
	// The drop partition binlog is idempotent
	IdempotentJobHandle[*record.DropPartition]
}

func (h *DropPartitionHandle) Handle(j *ccr.Job, commitSeq int64, dropPartition *record.DropPartition) error {
	if dropPartition.IsTemp {
		log.Infof("Since the temporary partition is not synchronized to the downstream, this binlog is skipped.")
		return nil
	}

	if isAsyncMv, err := j.IsMaterializedViewTable(dropPartition.TableId); err != nil {
		return err
	} else if isAsyncMv {
		log.Warnf("skip drop partition for materialized view table %d", dropPartition.TableId)
		return nil
	}

	destTableName, err := j.GetDestNameBySrcId(dropPartition.TableId)
	if err != nil {
		return err
	}
	return j.IDest.DropPartition(destTableName, dropPartition)
}
