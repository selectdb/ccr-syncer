package handle

import (
	"fmt"

	"github.com/selectdb/ccr_syncer/pkg/ccr"
	"github.com/selectdb/ccr_syncer/pkg/ccr/record"
	festruct "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/frontendservice"
	"github.com/selectdb/ccr_syncer/pkg/xerror"
	log "github.com/sirupsen/logrus"
)

func init() {
	ccr.RegisterJobHandle[*record.AlterJobV2](festruct.TBinlogType_ALTER_JOB, &AlterJobV2Handle{})
}

type AlterJobV2Handle struct {
	IdempotentJobHandle[*record.AlterJobV2]
}

func (*AlterJobV2Handle) Handle(j *ccr.Job, commitSeq int64, alterJob *record.AlterJobV2) error {
	if ccr.FeatureSkipRollupBinlogs && alterJob.Type == record.ALTER_JOB_ROLLUP {
		log.Warnf("skip rollup alter job: %s", alterJob)
		return nil
	}

	if isAsyncMv, err := j.IsMaterializedViewTable(alterJob.TableId); err != nil {
		return err
	} else if isAsyncMv {
		log.Warnf("skip alter job for materialized view table %d", alterJob.TableId)
		return nil
	}

	if alterJob.Type == record.ALTER_JOB_SCHEMA_CHANGE {
		return handleSchemaChange(j, alterJob)
	} else if alterJob.Type == record.ALTER_JOB_ROLLUP {
		return handleAlterRollup(j, alterJob)
	} else {
		return xerror.Errorf(xerror.Normal, "unsupported alter job type: %s", alterJob.Type)
	}
}

func handleAlterRollup(j *ccr.Job, alterJob *record.AlterJobV2) error {
	j.SaveAlterRollupShadowIndex(alterJob)
	if !alterJob.IsFinished() {
		return nil
	}

	return j.NewPartialSnapshot(alterJob.TableId, alterJob.TableName, nil, true, false)
}

func handleSchemaChange(j *ccr.Job, alterJob *record.AlterJobV2) error {
	job_progress := j.GetJobProgress()

	// drop table dropTableSql
	var destTableName string
	if j.SyncType == ccr.TableSync {
		destTableName = j.Dest.Table
	} else {
		destTableName = alterJob.TableName
	}

	j.SaveSchemaChangeShadowIndexes(alterJob)
	if !alterJob.IsFinished() {
		return nil
	}

	if ccr.FeatureSchemaChangePartialSync && alterJob.Type == record.ALTER_JOB_SCHEMA_CHANGE {
		return j.NewPartialSnapshot(alterJob.TableId, alterJob.TableName, nil, true, false)
	}

	var allViewDeleted bool = false
	for {
		// before drop table, drop related view firstly
		if !allViewDeleted {
			views, err := j.IDest.GetAllViewsFromTable(destTableName)
			if err != nil {
				log.Errorf("when alter job, get view from table %s failed, err : %v", err, destTableName)
				continue
			}

			var dropViewFailed bool = false
			for _, view := range views {
				if err := j.IDest.DropView(view); err != nil {
					log.Errorf("when alter job, drop view %s failed, err : %v", view, err)
					dropViewFailed = true
				}
			}
			if dropViewFailed {
				continue
			}

			allViewDeleted = true
		}

		if err := j.IDest.DropTable(destTableName, true); err == nil {
			break
		}
	}

	info := fmt.Sprintf("handle schema change job, need full sync, Table: %v", alterJob.TableName)

	return j.NewSnapshot(job_progress.CommitSeq, info)
}
