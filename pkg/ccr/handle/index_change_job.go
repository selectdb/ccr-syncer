package handle

import (
	"github.com/selectdb/ccr_syncer/pkg/ccr"
	"github.com/selectdb/ccr_syncer/pkg/ccr/record"
	festruct "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/frontendservice"
	log "github.com/sirupsen/logrus"
)

func init() {
	ccr.RegisterJobHandle[*record.IndexChangeJob](festruct.TBinlogType_INDEX_CHANGE_JOB, &IndexChangeJobHandle{})
}

type IndexChangeJobHandle struct {
	// The index change job binlog is idempotent
	IdempotentJobHandle[*record.IndexChangeJob]
}

func (h *IndexChangeJobHandle) Handle(j *ccr.Job, commitSeq int64, indexChangeJob *record.IndexChangeJob) error {
	if indexChangeJob.JobState != record.INDEX_CHANGE_JOB_STATE_FINISHED ||
		indexChangeJob.IsDropOp {
		log.Debugf("skip index change job binlog, job state: %s, is drop op: %t",
			indexChangeJob.JobState, indexChangeJob.IsDropOp)
		return nil
	}

	var destTableName string
	if j.SyncType == ccr.TableSync {
		destTableName = j.Dest.Table
	} else {
		destTableName = indexChangeJob.TableName
	}

	return j.IDest.BuildIndex(destTableName, indexChangeJob)
}
