package handle

import (
	"github.com/cloudwego/kitex/tool/internal_pkg/log"
	"github.com/selectdb/ccr_syncer/pkg/ccr"
	"github.com/selectdb/ccr_syncer/pkg/ccr/record"
	festruct "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/frontendservice"
)

func init() {
	ccr.RegisterJobHandle[*record.AddPartition](festruct.TBinlogType_ADD_PARTITION, &AddPartitionHandle{})
}

type AddPartitionHandle struct {
	// The adding partition binlog is idempotent
	IdempotentJobHandle[*record.AddPartition]
}

func (h *AddPartitionHandle) Handle(j *ccr.Job, commitSeq int64, addPartition *record.AddPartition) error {
	if isAsyncMv, err := j.IsMaterializedViewTable(addPartition.TableId); err != nil {
		return err
	} else if isAsyncMv {
		log.Warnf("skip add partition for materialized view table %d", addPartition.TableId)
		return nil
	}

	if addPartition.IsTemp {
		log.Infof("skip add temporary partition because backup/restore table with temporary partitions is not supported yet")
		return nil
	}

	destTableName, err := j.GetDestNameBySrcId(addPartition.TableId)
	if err != nil {
		return err
	}

	return j.IDest.AddPartition(destTableName, addPartition)
}
