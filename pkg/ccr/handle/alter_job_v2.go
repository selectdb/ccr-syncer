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
	job_progress := j.GetJobProgress()
	if !alterJob.IsFinished() {
		switch alterJob.JobState {
		case record.ALTER_JOB_STATE_PENDING:
			// Once the rollup job step to WAITING_TXN, the upsert to the rollup index is allowed,
			// but the dest index of the downstream cluster hasn't been created.
			//
			// To filter the upsert to the rollup index, save the shadow index ids here.
			if job_progress.ShadowIndexes == nil {
				job_progress.ShadowIndexes = make(map[int64]int64)
			}
			job_progress.ShadowIndexes[alterJob.RollupIndexId] = alterJob.BaseIndexId
		case record.ALTER_JOB_STATE_CANCELLED:
			// clear the shadow indexes
			delete(job_progress.ShadowIndexes, alterJob.RollupIndexId)
		}
		return nil
	}

	// Once partial snapshot finished, the rollup indexes will be convert to normal index.
	delete(job_progress.ShadowIndexes, alterJob.RollupIndexId)

	return j.NewPartialSnapshot(alterJob.TableId, alterJob.TableName, nil, true, false)
}

func handleSchemaChange(j *ccr.Job, alterJob *record.AlterJobV2) error {
	job_progress := j.GetJobProgress()
	if !alterJob.IsFinished() {
		switch alterJob.JobState {
		case record.ALTER_JOB_STATE_PENDING:
			// Once the schema change step to WAITING_TXN, the upsert to the shadow indexes is allowed,
			// but the dest indexes of the downstream cluster hasn't been created.
			//
			// To filter the upsert to the shadow indexes, save the shadow index ids here.
			if job_progress.ShadowIndexes == nil {
				job_progress.ShadowIndexes = make(map[int64]int64)
			}
			for shadowIndexId, originIndexId := range alterJob.ShadowIndexes {
				job_progress.ShadowIndexes[shadowIndexId] = originIndexId
			}
		case record.ALTER_JOB_STATE_CANCELLED:
			// clear the shadow indexes
			for shadowIndexId := range alterJob.ShadowIndexes {
				delete(job_progress.ShadowIndexes, shadowIndexId)
			}
		}
		return nil
	}

	// drop table dropTableSql
	var destTableName string
	if j.SyncType == ccr.TableSync {
		destTableName = j.Dest.Table
	} else {
		destTableName = alterJob.TableName
	}

	if ccr.FeatureSchemaChangePartialSync && alterJob.Type == record.ALTER_JOB_SCHEMA_CHANGE {
		// Once partial snapshot finished, the shadow indexes will be convert to normal indexes.
		for shadowIndexId := range alterJob.ShadowIndexes {
			delete(job_progress.ShadowIndexes, shadowIndexId)
		}

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
