// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License
package ccr

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/selectdb/ccr_syncer/pkg/ccr/base"
	"github.com/selectdb/ccr_syncer/pkg/ccr/record"
	"github.com/selectdb/ccr_syncer/pkg/rpc"
	"github.com/selectdb/ccr_syncer/pkg/storage"
	utils "github.com/selectdb/ccr_syncer/pkg/utils"
	"github.com/selectdb/ccr_syncer/pkg/xerror"
	"github.com/selectdb/ccr_syncer/pkg/xmetrics"

	festruct "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/frontendservice"
	tstatus "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/status"
	ttypes "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/types"

	_ "github.com/go-sql-driver/mysql"
	"github.com/modern-go/gls"
	log "github.com/sirupsen/logrus"
)

const (
	SyncDuration = time.Second * 3

	SkipBySilence     = "silence"
	SkipByFullSync    = "fullsync"
	SkipByPartialSync = "partialsync"

	LockLastBinlogCommitSeq = -1
)

var (
	FeatureSchemaChangePartialSync      bool
	featureCleanTableAndPartitions      bool
	featureAtomicRestore                bool
	FeatureCreateViewDropExists         bool
	featureReplaceNotMatchedWithAlias   bool
	featureFilterShadowIndexesUpsert    bool
	featureReuseRunningBackupRestoreJob bool
	featureCompressedSnapshot           bool
	FeatureSkipRollupBinlogs            bool
	featureTxnInsert                    bool
	FeatureFilterStorageMedium          bool
	featureRestoreReplaceDiffSchema     bool
	featureIdempotentDDL                bool
	featureSkipWaitingTxnPublish        bool
	featureSkipCheckAsyncMvTable        bool
	featurePipelineCommit               bool
	featureSeperatedHandles             bool

	flagBinlogBatchSize int64

	ErrMaterializedViewTable = xerror.NewWithoutStack(xerror.Meta, "Not support table type: materialized view")
)

func init() {
	flag.BoolVar(&FeatureSchemaChangePartialSync, "feature_schema_change_partial_sync", true,
		"use partial sync when working with schema change")

	// The default value is false, since clean tables will erase views unexpectedly.
	flag.BoolVar(&featureCleanTableAndPartitions, "feature_clean_table_and_partitions", false,
		"clean non restored tables and partitions during fullsync")
	flag.BoolVar(&featureAtomicRestore, "feature_atomic_restore", true,
		"replace tables in atomic during fullsync (otherwise the dest table will not be able to read).")
	flag.BoolVar(&FeatureCreateViewDropExists, "feature_create_view_drop_exists", true,
		"drop the exists view if exists, when sync the creating view binlog")
	flag.BoolVar(&featureReplaceNotMatchedWithAlias, "feature_replace_not_matched_with_alias", true,
		"replace signature not matched tables with table alias during the full sync")
	flag.BoolVar(&featureFilterShadowIndexesUpsert, "feature_filter_shadow_indexes_upsert", true,
		"filter the upsert to the shadow indexes")
	flag.BoolVar(&featureReuseRunningBackupRestoreJob, "feature_reuse_running_backup_restore_job", true,
		"reuse the running backup/restore issued by the job self")
	flag.BoolVar(&featureCompressedSnapshot, "feature_compressed_snapshot", true,
		"compress the snapshot job info and meta")
	flag.BoolVar(&FeatureSkipRollupBinlogs, "feature_skip_rollup_binlogs", false,
		"skip the rollup related binlogs")
	flag.BoolVar(&featureTxnInsert, "feature_txn_insert", true,
		"enable txn insert support")
	flag.BoolVar(&FeatureFilterStorageMedium, "feature_filter_storage_medium", true,
		"enable filter storage medium property")
	flag.BoolVar(&featureRestoreReplaceDiffSchema, "feature_restore_replace_diff_schema", true,
		"replace the table with different schema during restore")
	flag.BoolVar(&featureIdempotentDDL, "feature_idempotent_ddl", true,
		"enable idempotent ddl by checking the dest table schema before rolling back")
	flag.BoolVar(&featureSkipWaitingTxnPublish, "feature_skip_waiting_txn_publish", true,
		"skip waiting for the txn publish")
	flag.BoolVar(&featureSkipCheckAsyncMvTable, "feature_skip_check_async_mv_table", true,
		"skip checking async mv table, the async mv binlogs will be filtered by doris")
	flag.BoolVar(&featurePipelineCommit, "feature_pipeline_commit", true,
		"enable pipeline commit for upsert binlogs")
	flag.BoolVar(&featureSeperatedHandles, "feature_seperated_handles", true,
		"enable the seperated handles (the refactor)")

	flag.Int64Var(&flagBinlogBatchSize, "binlog_batch_size", 16, "the max num of binlogs to get in a batch")
}

type SyncType int

const (
	DBSync    SyncType = 0
	TableSync SyncType = 1
)

func (s SyncType) String() string {
	switch s {
	case DBSync:
		return "db_sync"
	case TableSync:
		return "table_sync"
	default:
		return "unknown_sync"
	}
}

type JobState int

const (
	JobRunning JobState = 0
	JobPaused  JobState = 1
)

// JobState Stringer
func (j JobState) String() string {
	switch j {
	case JobRunning:
		return "running"
	case JobPaused:
		return "paused"
	default:
		return "unknown"
	}
}

type PartialSnapshotParams struct {
	TableId    int64    `json:"table_id"`
	TableName  string   `json:"table"`
	Partitions []string `json:"partitions"`
	IsView     bool     `json:"is_view"`
	Replace    bool     `json:"replace"`
}

type JobExtra struct {
	// Reuse the upstream txn label as the downstream txn label.
	ReuseBinlogLabel bool `json:"reuse_binlog_label,omitempty"`

	allowTableExists bool `json:"-"` // Only for FirstRun(), don't need to persist.

	// Skip a specified binlog or binlogs, don't need to persist.
	// See Job.SkipBinlog for more details.
	SkipBinlog    bool   `json:"skip_binlog,omitempty"`
	SkipCommitSeq int64  `json:"skip_commit_seq,omitempty"`
	SkipBy        string `json:"skip_by,omitempty"`
	SkipTable     string `json:"skip_table,omitempty"`
	SkipTableId   int64  `json:"skip_table_id,omitempty"`

	// The cached binlogs used for missing binlogs,don't need to persist.
	CachedBinlogs map[int64]*festruct.TBinlog `json:"-"`
	// The binlog already applied to the dest cluster, don't need to persist.
	AppliedBinlogs map[int64]any `json:"-"`
	// The dropped indexes, is used to avoid the parallel execution of drop index and upsert.
	// It does not need to be persisted and falls back to fullsync after restart.
	DroppedIndexes map[int64]any

	// A counter and a channel used to signal the job routine to release the lock.
	InterruptSignal int32    `json:"-"`
	InterruptCh     chan any `json:"-"`

	// New partial snapshot info
	PartialSnapshotParams *PartialSnapshotParams `json:"-"`
}

type Job struct {
	Name     string      `json:"name"`
	Id       string      `json:"id"`
	SyncType SyncType    `json:"sync_type"`
	Src      base.Spec   `json:"src"`
	ISrc     base.Specer `json:"-"`
	srcMeta  Metaer      `json:"-"`
	Dest     base.Spec   `json:"dest"`
	IDest    base.Specer `json:"-"`
	destMeta Metaer      `json:"-"`
	State    JobState    `json:"state"`
	Extra    JobExtra    `json:"extra"`

	factory *Factory `json:"-"`

	progress   *JobProgress `json:"-"`
	db         storage.DB   `json:"-"`
	jobFactory *JobFactory  `json:"-"`
	rawStatus  RawJobStatus `json:"-"`

	stop      chan struct{} `json:"-"`
	isDeleted atomic.Bool   `json:"-"`

	asyncMvTableCache  map[int64]struct{}      `json:"-"`
	concurrencyManager *rpc.ConcurrencyManager `json:"-"`
	pipelineCtx        *JobPipelineContext     `json:"-"`

	lock sync.Mutex `json:"-"`
}

type JobContext struct {
	context.Context
	Src              base.Spec
	Dest             base.Spec
	Db               storage.DB
	SkipError        bool
	AllowTableExists bool
	ReuseBinlogLabel bool
	Factory          *Factory
}

// new job
func NewJobFromService(name string, ctx context.Context) (*Job, error) {
	jobContext, ok := ctx.(*JobContext)
	if !ok {
		return nil, xerror.Errorf(xerror.Normal, "invalid context type: %T", ctx)
	}

	factory := jobContext.Factory
	src := jobContext.Src
	dest := jobContext.Dest
	id := getJobId(name, src, dest)
	job := &Job{
		Name:     name,
		Id:       id,
		Src:      src,
		ISrc:     factory.NewSpecer(&src),
		srcMeta:  factory.NewMeta(&jobContext.Src),
		Dest:     dest,
		IDest:    factory.NewSpecer(&dest),
		destMeta: factory.NewMeta(&jobContext.Dest),
		State:    JobRunning,

		Extra: JobExtra{
			allowTableExists: jobContext.AllowTableExists,
			ReuseBinlogLabel: jobContext.ReuseBinlogLabel,
			SkipBinlog:       false,
			InterruptCh:      make(chan any, 1),
		},

		factory: factory,

		progress: nil,
		db:       jobContext.Db,
		stop:     make(chan struct{}),

		concurrencyManager: rpc.NewConcurrencyManager(),
	}

	if err := job.valid(); err != nil {
		return nil, xerror.Wrap(err, xerror.Normal, "job is invalid")
	}

	if job.Src.Table == "" {
		job.SyncType = DBSync
	} else {
		job.SyncType = TableSync
	}

	job.jobFactory = NewJobFactory()

	return job, nil
}

func NewJobFromJson(jsonData string, db storage.DB, factory *Factory) (*Job, error) {
	var job Job
	err := json.Unmarshal([]byte(jsonData), &job)
	if err != nil {
		return nil, xerror.Wrapf(err, xerror.Normal, "unmarshal json failed, json: %s", jsonData)
	}

	if len(job.Id) == 0 {
		job.Id = getJobId(job.Name, job.Src, job.Dest)
	}

	// recover all not json fields
	job.factory = factory
	job.ISrc = factory.NewSpecer(&job.Src)
	job.IDest = factory.NewSpecer(&job.Dest)
	job.srcMeta = factory.NewMeta(&job.Src)
	job.destMeta = factory.NewMeta(&job.Dest)
	job.progress = nil
	job.db = db
	job.stop = make(chan struct{})
	job.jobFactory = NewJobFactory()
	job.concurrencyManager = rpc.NewConcurrencyManager()
	job.Extra.InterruptCh = make(chan any, 1)
	return &job, nil
}

func (j *Job) valid() error {
	var err error
	if exist, err := j.db.IsJobExist(j.Name); err != nil {
		return xerror.Wrap(err, xerror.Normal, "check job exist failed")
	} else if exist {
		return xerror.Errorf(xerror.Normal, "job %s already exist", j.Name)
	}

	if j.Name == "" {
		return xerror.New(xerror.Normal, "name is empty")
	}

	err = j.ISrc.Valid()
	if err != nil {
		return xerror.Wrap(err, xerror.Normal, "src spec is invalid")
	}

	err = j.IDest.Valid()
	if err != nil {
		return xerror.Wrap(err, xerror.Normal, "dest spec is invalid")
	}

	if (j.Src.Table == "" && j.Dest.Table != "") || (j.Src.Table != "" && j.Dest.Table == "") {
		return xerror.New(xerror.Normal, "src/dest are not both db or table sync")
	}

	return nil
}

func (j *Job) genExtraInfo() (*base.ExtraInfo, error) {
	meta := j.srcMeta
	masterToken, err := meta.GetMasterToken(j.factory)
	if err != nil {
		return nil, err
	}
	log.Infof("gen extra info with master token %s", masterToken)

	backends, err := meta.GetBackends()
	if err != nil {
		return nil, err
	}

	log.Tracef("found backends: %v", backends)

	beNetworkMap := make(map[int64]base.NetworkAddr)
	for _, backend := range backends {
		log.Infof("gen extra info with backend: %v", backend)
		addr := base.NetworkAddr{
			Ip:   backend.Host,
			Port: backend.HttpPort,
		}
		beNetworkMap[backend.Id] = addr
	}

	return &base.ExtraInfo{
		BeNetworkMap: beNetworkMap,
		Token:        masterToken,
	}, nil
}

func (j *Job) isIncrementalSync() bool {
	switch j.progress.SyncState {
	case TableIncrementalSync, DBIncrementalSync, DBTablesIncrementalSync:
		return true
	default:
		return false
	}
}

func (j *Job) IsTableSyncWithAlias() bool {
	return j.SyncType == TableSync && j.Src.Table != j.Dest.Table
}

func (j *Job) isTableDropped(tableId int64) (bool, error) {
	// Keep compatible with the old version, which doesn't have the table id in partial sync data.
	if tableId == 0 {
		return false, nil
	}

	srcMeta, err := j.getSourceThriftMeta(tableId)
	if err != nil {
		return false, err
	}
	return srcMeta.IsTableDropped(tableId), nil
}

func (j *Job) getSourceThriftMeta(tableId int64) (*ThriftMeta, error) {
	return j.factory.NewThriftMeta(&j.Src, j.factory, []int64{tableId})
}

func (j *Job) addExtraInfo(jobInfo []byte) ([]byte, error) {
	var jobInfoMap map[string]interface{}
	err := json.Unmarshal(jobInfo, &jobInfoMap)
	if err != nil {
		return nil, xerror.Wrapf(err, xerror.Normal, "unmarshal jobInfo failed, jobInfo: %s", string(jobInfo))
	}

	extraInfo, err := j.genExtraInfo()
	if err != nil {
		return nil, err
	}
	log.Tracef("snapshot extra info: %v", extraInfo)
	jobInfoMap["extra_info"] = extraInfo

	jobInfoBytes, err := json.Marshal(jobInfoMap)
	if err != nil {
		return nil, xerror.Errorf(xerror.Normal, "marshal jobInfo failed, jobInfo: %v", jobInfoMap)
	}

	return jobInfoBytes, nil
}

func (j *Job) handlePartialSyncTableNotFound() error {
	tableId := j.progress.PartialSyncData.TableId
	table := j.progress.PartialSyncData.Table

	if dropped, err := j.isTableDropped(tableId); err != nil {
		return err
	} else if dropped && j.SyncType == TableSync {
		return xerror.Errorf(xerror.Normal, "table sync but table %s has been dropped, table id %d",
			table, tableId)
	} else if dropped {
		// skip this partial sync because table has been dropped
		log.Warnf("skip this partial sync because table %s has been dropped, table id: %d", table, tableId)
		nextCommitSeq := j.progress.CommitSeq
		// Since we don't know the commit seq of the drop table binlog, we set it to the max value to
		// skip all binlogs.
		//
		// FIXME: it will skip drop table binlog too.
		if len(j.progress.TableCommitSeqMap) == 0 {
			j.progress.TableCommitSeqMap = make(map[int64]int64)
		}
		j.progress.TableCommitSeqMap[tableId] = math.MaxInt64
		j.progress.NextWithPersist(nextCommitSeq, DBTablesIncrementalSync, Done, "")
		return nil
	} else if newTableName, err := j.srcMeta.GetTableNameById(tableId); err != nil {
		return err
	} else if j.SyncType == DBSync {
		// The table might be renamed, so we need to update the table name.
		log.Warnf("force new partial snapshot, since table %d has renamed from %s to %s", tableId, table, newTableName)
		replace := true // replace the old data to avoid blocking reading
		isView := false
		return j.NewPartialSnapshot(tableId, newTableName, nil, replace, isView)
	} else {
		return xerror.Errorf(xerror.Normal, "table sync but table has renamed from %s to %s, table id %d",
			table, newTableName, tableId)
	}
}

// Like fullSync, but only backup and restore partial of the partitions of a table.
func (j *Job) partialSync() error {
	type inMemoryData struct {
		SnapshotName          string                        `json:"snapshot_name"`
		SnapshotResp          *festruct.TGetSnapshotResult_ `json:"snapshot_resp"`
		TableCommitSeqMap     map[int64]int64               `json:"table_commit_seq_map"`
		PartitionCommitSeqMap map[int64]int64               `json:"partition_commit_seq_map"`
		TableNameMapping      map[int64]string              `json:"table_name_mapping"`
		RestoreLabel          string                        `json:"restore_label"`
	}

	if j.progress.PartialSyncData == nil {
		return xerror.Errorf(xerror.Normal, "run partial sync but data is nil")
	}

	tableId := j.progress.PartialSyncData.TableId
	table := j.progress.PartialSyncData.Table
	partitions := j.progress.PartialSyncData.Partitions
	switch j.progress.SubSyncState {
	case Done:
		log.Infof("partial sync status: done")
		withAlias := len(j.progress.TableAliases) > 0
		isView := j.progress.PartialSyncData.IsView
		if err := j.NewPartialSnapshot(tableId, table, partitions, withAlias, isView); err != nil {
			return err
		}

	case BeginCreateSnapshot:
		// Step 1: Create snapshot
		prefix := NewPartialSnapshotLabelPrefix(j.Name, j.progress.SyncId)
		log.Infof("partial sync status: create snapshot with prefix %s", prefix)

		if featureReuseRunningBackupRestoreJob {
			snapshotName, err := j.ISrc.GetValidBackupJob(prefix)
			if err != nil {
				return err
			}
			if snapshotName != "" {
				log.Infof("partial sync status: there has a exist backup job %s", snapshotName)
				j.progress.NextSubVolatile(WaitBackupDone, snapshotName)
				return nil
			}
		}

		snapshotName := NewLabelWithTs(prefix)
		log.Infof("partial sync status: create snapshot %s", snapshotName)
		err := j.ISrc.CreatePartialSnapshot(snapshotName, table, partitions)
		if err != nil && err == base.ErrBackupPartitionNotFound {
			log.Warnf("partial sync status: partition not found in the upstream, step to table partial sync")
			replace := true // replace the old data to avoid blocking reading
			isView := false // partition not found, so it's not a view
			return j.NewPartialSnapshot(tableId, table, nil, replace, isView)
		} else if err != nil && err == base.ErrBackupTableNotFound {
			return j.handlePartialSyncTableNotFound()
		} else if err != nil {
			return err
		}

		j.progress.NextSubVolatile(WaitBackupDone, snapshotName)
		return nil

	case WaitBackupDone:
		// Step 2: Wait backup job done
		snapshotName := j.progress.InMemoryData.(string)
		backupFinished, err := j.ISrc.CheckBackupFinished(snapshotName)
		if err != nil {
			j.progress.NextSubVolatile(BeginCreateSnapshot, snapshotName)
			return err
		}

		if !backupFinished {
			// CheckBackupFinished already logs the info
			return nil
		}

		j.progress.NextSubCheckpoint(GetSnapshotInfo, snapshotName)

	case GetSnapshotInfo:
		// Step 3: Get snapshot info
		log.Infof("partial sync status: get snapshot info")

		snapshotName := j.progress.PersistData
		src := &j.Src
		srcRpc, err := j.factory.NewFeRpc(src)
		if err != nil {
			return err
		}

		log.Tracef("partial sync begin get snapshot %s", snapshotName)
		compress := false // partial snapshot no need to compress
		snapshotResp, err := srcRpc.GetSnapshot(src, snapshotName, compress)
		if err != nil {
			return err
		}

		if snapshotResp.Status.GetStatusCode() == tstatus.TStatusCode_SNAPSHOT_NOT_EXIST ||
			snapshotResp.Status.GetStatusCode() == tstatus.TStatusCode_SNAPSHOT_EXPIRED {
			log.Warnf("force new partial sync, because get snapshot %s: %s (%s)", snapshotName,
				utils.FirstOr(snapshotResp.Status.GetErrorMsgs(), "unknown"),
				snapshotResp.Status.GetStatusCode())
			replace := len(j.progress.TableAliases) > 0
			isView := j.progress.PartialSyncData.IsView
			return j.NewPartialSnapshot(tableId, table, partitions, replace, isView)
		} else if snapshotResp.Status.GetStatusCode() != tstatus.TStatusCode_OK {
			err = xerror.Errorf(xerror.FE, "get snapshot failed, status: %v", snapshotResp.Status)
			return err
		}

		if !snapshotResp.IsSetJobInfo() {
			return xerror.New(xerror.Normal, "jobInfo is not set")
		}

		log.Tracef("partial sync snapshot job: %.128s", snapshotResp.GetJobInfo())

		backupJobInfo, err := NewBackupJobInfoFromJson(snapshotResp.GetJobInfo())
		if err != nil {
			return err
		}

		partitionCommitSeqMap := make(map[int64]int64)
		tableCommitSeqMap := backupJobInfo.TableCommitSeqMap
		tableNameMapping := backupJobInfo.TableNameMapping()
		log.Debugf("table commit seq map: %v, table name mapping: %v", tableCommitSeqMap, tableNameMapping)
		if j.progress.PartialSyncData.IsView {
			view, ok := backupJobInfo.GetView(table)
			if !ok {
				return xerror.Errorf(xerror.Normal, "view %s not found in backup objects", table)
			}
			tableCommitSeqMap[view.Id] = snapshotResp.GetCommitSeq()
		} else if backupObject, ok := backupJobInfo.BackupObjects[table]; !ok {
			return xerror.Errorf(xerror.Normal, "table %s not found in backup objects", table)
		} else if backupObject.Id != tableId {
			var info string
			if j.SyncType == TableSync {
				info = fmt.Sprintf("partial sync table `%s` id not match, reset src table id from %d to %d",
					table, j.Src.TableId, backupObject.Id)
				log.Infof("force full sync, because %s", info)
				j.Src.TableId = backupObject.Id
			} else {
				info = fmt.Sprintf("partial sync table `%s` id not match, table id %d, backup object id %d",
					table, tableId, backupObject.Id)
				log.Warnf("force full sync, because %s", info)
			}
			return j.NewSnapshot(j.progress.CommitSeq, info)
		} else if commitSeq, ok := tableCommitSeqMap[backupObject.Id]; !ok {
			return xerror.Errorf(xerror.Normal, "commit seq not found, table id %d, table name: %s", backupObject.Id, table)
		} else {
			for _, name := range partitions {
				if part, ok := backupObject.Partitions[name]; !ok {
					return xerror.Errorf(xerror.Normal, "partition %s not found in backup objects", name)
				} else {
					partitionCommitSeqMap[part.Id] = commitSeq
				}
			}
			if len(partitionCommitSeqMap) > 0 {
				log.Debugf("partition commit seq map: %v", partitionCommitSeqMap)
			}
		}

		inMemoryData := &inMemoryData{
			SnapshotName:          snapshotName,
			SnapshotResp:          snapshotResp,
			TableCommitSeqMap:     tableCommitSeqMap,
			TableNameMapping:      tableNameMapping,
			PartitionCommitSeqMap: partitionCommitSeqMap,
		}
		j.progress.NextSubVolatile(AddExtraInfo, inMemoryData)

	case AddExtraInfo:
		// Step 4: Add extra info
		log.Infof("partial sync status: add extra info")

		inMemoryData := j.progress.InMemoryData.(*inMemoryData)
		snapshotResp := inMemoryData.SnapshotResp
		jobInfo := snapshotResp.GetJobInfo()

		log.Infof("partial snapshot %s response meta size: %d, job info size: %d, expired at: %d",
			inMemoryData.SnapshotName, len(snapshotResp.Meta), len(snapshotResp.JobInfo), snapshotResp.GetExpiredAt())

		jobInfoBytes, err := j.addExtraInfo(jobInfo)
		if err != nil {
			return err
		}

		log.Debugf("partial sync job info size: %d, bytes: %.128s", len(jobInfoBytes), string(jobInfoBytes))
		snapshotResp.SetJobInfo(jobInfoBytes)

		j.progress.NextSubVolatile(RestoreSnapshot, inMemoryData)

	case RestoreSnapshot:
		// Step 5: Restore snapshot
		log.Infof("partial sync status: restore snapshot")

		if j.progress.InMemoryData == nil {
			persistData := j.progress.PersistData
			inMemoryData := &inMemoryData{}
			if err := json.Unmarshal([]byte(persistData), inMemoryData); err != nil {
				return xerror.Errorf(xerror.Normal, "unmarshal persistData failed, persistData: %s", persistData)
			}
			j.progress.InMemoryData = inMemoryData
		}

		// Step 5.1: try reuse the exists restore job.
		inMemoryData := j.progress.InMemoryData.(*inMemoryData)
		snapshotName := inMemoryData.SnapshotName
		if featureReuseRunningBackupRestoreJob {
			name, err := j.IDest.GetValidRestoreJob(snapshotName)
			if err != nil {
				return nil
			}
			if name != "" {
				log.Infof("partial sync status: there has a exist restore job %s", name)
				inMemoryData.RestoreLabel = name
				j.progress.NextSubVolatile(WaitRestoreDone, inMemoryData)
				break
			}
		}

		// Step 5.2: start a new fullsync & restore snapshot to dest
		restoreSnapshotName := NewRestoreLabel(snapshotName)
		snapshotResp := inMemoryData.SnapshotResp

		dest := &j.Dest
		destRpc, err := j.factory.NewFeRpc(dest)
		if err != nil {
			return err
		}
		log.Infof("partial sync begin restore snapshot %s to %s", snapshotName, restoreSnapshotName)

		var tableRefs []*festruct.TTableRef

		// ATTN: The table name of the alias is from the source cluster.
		if j.progress.PartialSyncData.IsView {
			log.Infof("partial sync with view %s", table)
		} else if aliasName, ok := j.progress.TableAliases[table]; ok {
			log.Infof("partial sync with table alias, table: %s, alias: %s", table, aliasName)
			tableRefs = make([]*festruct.TTableRef, 0)
			tableRef := &festruct.TTableRef{
				Table:     &table,
				AliasName: &aliasName,
			}
			tableRefs = append(tableRefs, tableRef)
		} else if j.IsTableSyncWithAlias() {
			log.Infof("table sync snapshot not same name, table: %s, dest table: %s", j.Src.Table, j.Dest.Table)
			tableRefs = make([]*festruct.TTableRef, 0)
			tableRef := &festruct.TTableRef{
				Table:     &j.Src.Table,
				AliasName: &j.Dest.Table,
			}
			tableRefs = append(tableRefs, tableRef)
		}

		// view associated with the table may skip some operations due to the backup/restore of the table
		// resulting in different schema of upstream and downstream views. we need to force replace
		isForceReplace := featureRestoreReplaceDiffSchema && j.progress.PartialSyncData.IsView
		isAtomicRestore := featureAtomicRestore && isForceReplace

		restoreReq := rpc.RestoreSnapshotRequest{
			TableRefs:      tableRefs,
			SnapshotName:   restoreSnapshotName,
			SnapshotResult: snapshotResp,

			// DO NOT drop exists tables and partitions
			CleanPartitions: false,
			CleanTables:     false,
			AtomicRestore:   isAtomicRestore,
			Compress:        false,
			ForceReplace:    isForceReplace,
		}
		restoreResp, err := destRpc.RestoreSnapshot(dest, &restoreReq)
		if err != nil {
			return err
		}
		if restoreResp.Status.GetStatusCode() != tstatus.TStatusCode_OK {
			return xerror.Errorf(xerror.Normal, "restore snapshot failed, status: %v", restoreResp.Status)
		}
		log.Tracef("partial sync restore snapshot resp: %v", restoreResp)
		inMemoryData.RestoreLabel = restoreSnapshotName

		j.progress.NextSubVolatile(WaitRestoreDone, inMemoryData)
		return nil

	case WaitRestoreDone:
		// Step 6: Wait restore job done
		inMemoryData := j.progress.InMemoryData.(*inMemoryData)
		restoreSnapshotName := inMemoryData.RestoreLabel
		snapshotResp := inMemoryData.SnapshotResp

		if snapshotResp.GetExpiredAt() > 0 && time.Now().UnixMilli() > snapshotResp.GetExpiredAt() {
			log.Warnf("cancel the expired restore job %s", restoreSnapshotName)
			if err := j.IDest.CancelRestoreIfExists(restoreSnapshotName); err != nil {
				return err
			}
			log.Infof("force partial sync, because the snapshot %s is expired", restoreSnapshotName)
			replace := len(j.progress.TableAliases) > 0
			isView := j.progress.PartialSyncData.IsView
			return j.NewPartialSnapshot(tableId, table, partitions, replace, isView)
		}

		restoreFinished, err := j.IDest.CheckRestoreFinished(restoreSnapshotName)
		if errors.Is(err, base.ErrRestoreSignatureNotMatched) {
			log.Warnf("force partial sync with replace, because the snapshot %s signature is not matched", restoreSnapshotName)
			return j.NewPartialSnapshot(tableId, table, nil, true, false) // only in partition sync.
		} else if err != nil {
			j.progress.NextSubVolatile(RestoreSnapshot, inMemoryData)
			return err
		}

		if !restoreFinished {
			// CheckRestoreFinished already logs the info
			return nil
		}

		// save the entire table/partition commit seq map, this value will be used in PersistRestoreInfo.
		if len(partitions) > 0 {
			j.progress.PartitionCommitSeqMap = utils.MergeMap(
				j.progress.PartitionCommitSeqMap, inMemoryData.PartitionCommitSeqMap)
		} else {
			j.progress.TableCommitSeqMap = utils.MergeMap(
				j.progress.TableCommitSeqMap, inMemoryData.TableCommitSeqMap)
		}
		j.progress.TableNameMapping = utils.MergeMap(
			j.progress.TableNameMapping, inMemoryData.TableNameMapping)
		j.progress.NextSubCheckpoint(PersistRestoreInfo, restoreSnapshotName)

	case PersistRestoreInfo:
		// Step 7: Update job progress && dest table id
		// update job info, only for dest table id
		var targetName = table
		if j.IsTableSyncWithAlias() {
			targetName = j.Dest.Table
		}
		if alias, ok := j.progress.TableAliases[table]; ok {
			// check table exists to ensure the idempotent
			if exist, err := j.IDest.CheckTableExistsByName(alias); err != nil {
				return err
			} else if exist {
				if exists, err := j.IDest.CheckTableExistsByName(targetName); err != nil {
					return err
				} else if exists {
					log.Infof("partial sync swap table with alias, table: %s, alias: %s", targetName, alias)
					swap := false // drop the old table
					if err := j.IDest.ReplaceTable(alias, targetName, swap); err != nil {
						return err
					}
				} else {
					log.Infof("partial sync rename table alias %s to %s", alias, targetName)
					if err := j.IDest.RenameTableWithName(alias, targetName); err != nil {
						return err
					}
				}
				// Since the meta of dest table has been changed, refresh it.
				j.destMeta.ClearTablesCache()
			} else {
				log.Infof("partial sync the table alias has been swapped, table: %s, alias: %s", targetName, alias)
			}

			// Save the replace result
			j.progress.TableAliases = nil
			j.progress.NextSubCheckpoint(PersistRestoreInfo, j.progress.PersistData)
		}

		log.Infof("partial sync status: persist restore info")
		destTable, err := j.destMeta.UpdateTable(targetName, 0)
		if err != nil {
			return err
		}
		switch j.SyncType {
		case DBSync:
			if len(j.progress.TableMapping) == 0 {
				j.progress.TableMapping = make(map[int64]int64)
			}
			j.progress.TableMapping[tableId] = destTable.Id
			j.progress.NextWithPersist(j.progress.CommitSeq, DBTablesIncrementalSync, Done, "")
		case TableSync:
			commitSeq, ok := j.progress.TableCommitSeqMap[j.Src.TableId]
			if !ok {
				return xerror.Errorf(xerror.Normal, "table id %d, commit seq not found", j.Src.TableId)
			}
			if len(partitions) > 0 {
				// Only commit partition commit seq map, instead of the entire table commit seq.
				commitSeq = j.progress.CommitSeq
			}
			j.Dest.TableId = destTable.Id
			j.progress.TableMapping = nil
			j.progress.TableCommitSeqMap = nil
			j.progress.NextWithPersist(commitSeq, TableIncrementalSync, Done, "")
		default:
			return xerror.Errorf(xerror.Normal, "invalid sync type %d", j.SyncType)
		}

		return nil

	default:
		return xerror.Errorf(xerror.Normal, "invalid job sub sync state %d", j.progress.SubSyncState)
	}

	return j.partialSync()
}

func (j *Job) fullSync() error {
	type inMemoryData struct {
		SnapshotName      string                        `json:"snapshot_name"`
		SnapshotResp      *festruct.TGetSnapshotResult_ `json:"snapshot_resp"`
		TableCommitSeqMap map[int64]int64               `json:"table_commit_seq_map"`
		TableNameMapping  map[int64]string              `json:"table_name_mapping"`
		Views             []string                      `json:"views"`
		RestoreLabel      string                        `json:"restore_label"`
	}

	switch j.progress.SubSyncState {
	case Done:
		log.Infof("fullsync status: done")
		if err := j.NewSnapshot(j.progress.CommitSeq, ""); err != nil {
			return err
		}

	case BeginCreateSnapshot:
		// Step 1: Create snapshot
		prefix := NewSnapshotLabelPrefix(j.Name, j.progress.SyncId)
		log.Infof("fullsync status: create snapshot with prefix %s", prefix)

		if featureReuseRunningBackupRestoreJob {
			snapshotName, err := j.ISrc.GetValidBackupJob(prefix)
			if err != nil {
				return err
			}
			if snapshotName != "" {
				log.Infof("fullsync status: there has a exist backup job %s", snapshotName)
				j.progress.NextSubVolatile(WaitBackupDone, snapshotName)
				return nil
			}
		}

		// Step 1.1: Lock the binlogs, to avoid the binlog is deleted during the full sync.
		if err := j.lockBinlog(LockLastBinlogCommitSeq); err != nil {
			return err
		}

		// Step 1.2: Check the tables in the source db, if the tables are empty, retry later.
		backupTableList := make([]string, 0)
		switch j.SyncType {
		case DBSync:
			tables, err := j.srcMeta.GetTables()
			if err != nil {
				return err
			}
			count := 0
			for _, table := range tables {
				// See fe/fe-core/src/main/java/org/apache/doris/backup/BackupHandler.java:backup() for details
				if table.Type == record.TableTypeOlap || table.Type == record.TableTypeView {
					count += 1
				}
			}
			if count == 0 {
				log.Warnf("full sync but source db is empty! retry later")
				return nil
			}
		case TableSync:
			backupTableList = append(backupTableList, j.Src.Table)
		default:
			return xerror.Errorf(xerror.Normal, "invalid sync type %s", j.SyncType)
		}

		// Step 1.3: Create snapshot
		snapshotName := NewLabelWithTs(prefix)
		log.Infof("fullsync status: create snapshot %s", snapshotName)
		if err := j.ISrc.CreateSnapshot(snapshotName, backupTableList); err != nil {
			return err
		}
		j.progress.NextSubVolatile(WaitBackupDone, snapshotName)
		return nil

	case WaitBackupDone:
		// Step 2: Wait backup job done
		snapshotName := j.progress.InMemoryData.(string)
		backupFinished, err := j.ISrc.CheckBackupFinished(snapshotName)
		if err != nil {
			j.progress.NextSubVolatile(BeginCreateSnapshot, snapshotName)
			return err
		}
		if !backupFinished {
			// CheckBackupFinished already logs the info
			return nil
		}

		j.progress.NextSubCheckpoint(GetSnapshotInfo, snapshotName)

	case GetSnapshotInfo:
		// Step 3: Get snapshot info
		log.Infof("fullsync status: get snapshot info")

		snapshotName := j.progress.PersistData
		src := &j.Src
		srcRpc, err := j.factory.NewFeRpc(src)
		if err != nil {
			return err
		}

		log.Tracef("fullsync begin get snapshot %s", snapshotName)
		compress := false
		snapshotResp, err := srcRpc.GetSnapshot(src, snapshotName, compress)
		if err != nil {
			return err
		}

		if snapshotResp.Status.GetStatusCode() == tstatus.TStatusCode_SNAPSHOT_NOT_EXIST ||
			snapshotResp.Status.GetStatusCode() == tstatus.TStatusCode_SNAPSHOT_EXPIRED {
			info := fmt.Sprintf("get snapshot %s: %s (%s)", snapshotName,
				utils.FirstOr(snapshotResp.Status.GetErrorMsgs(), "unknown"),
				snapshotResp.Status.GetStatusCode())
			log.Warnf("force full sync, because %s", info)
			return j.NewSnapshot(j.progress.CommitSeq, info)
		} else if snapshotResp.Status.GetStatusCode() != tstatus.TStatusCode_OK {
			err = xerror.Errorf(xerror.FE, "get snapshot failed, status: %v", snapshotResp.Status)
			return err
		}

		if !snapshotResp.IsSetJobInfo() {
			return xerror.New(xerror.Normal, "jobInfo of the snapshot resp is not set")
		}

		if snapshotResp.GetCompressed() {
			if bytes, err := utils.GZIPDecompress(snapshotResp.GetJobInfo()); err != nil {
				return xerror.Wrap(err, xerror.Normal, "decompress snapshot job info failed")
			} else {
				snapshotResp.SetJobInfo(bytes)
			}
			if bytes, err := utils.GZIPDecompress(snapshotResp.GetMeta()); err != nil {
				return xerror.Wrap(err, xerror.Normal, "decompress snapshot meta failed")
			} else {
				snapshotResp.SetMeta(bytes)
			}
		}

		log.Tracef("fullsync snapshot job info: %.128s", snapshotResp.GetJobInfo())
		backupJobInfo, err := NewBackupJobInfoFromJson(snapshotResp.GetJobInfo())
		if err != nil {
			return err
		}

		tableCommitSeqMap := backupJobInfo.TableCommitSeqMap
		tableNameMapping := backupJobInfo.TableNameMapping()
		views := backupJobInfo.Views()

		if j.SyncType == TableSync {
			if backupObject, ok := backupJobInfo.BackupObjects[j.Src.Table]; !ok {
				return xerror.Errorf(xerror.Normal, "table %s not found in backup objects", j.Src.Table)
			} else if backupObject.Id != j.Src.TableId {
				// Might be the table has been replace.
				info := fmt.Sprintf("full sync table %s id not match, force full sync. table id %d, backup object id %d",
					j.Src.Table, j.Src.TableId, backupObject.Id)
				log.Warnf("%s", info)
				j.Src.TableId = backupObject.Id
				return j.NewSnapshot(j.progress.CommitSeq, info)
			} else if _, ok := tableCommitSeqMap[j.Src.TableId]; !ok {
				return xerror.Errorf(xerror.Normal, "table id %d, commit seq not found", j.Src.TableId)
			}
		} else {
			// save the view ids in the table commit seq map, to build the view mapping latter.
			for _, view := range backupJobInfo.NewBackupObjects.Views {
				tableNameMapping[view.Id] = view.Name
				tableCommitSeqMap[view.Id] = snapshotResp.GetCommitSeq() // zero if not exists
			}
		}

		inMemoryData := &inMemoryData{
			SnapshotName:      snapshotName,
			SnapshotResp:      snapshotResp,
			TableCommitSeqMap: tableCommitSeqMap,
			TableNameMapping:  tableNameMapping,
			Views:             views,
		}
		j.progress.NextSubVolatile(AddExtraInfo, inMemoryData)

	case AddExtraInfo:
		// Step 4: Add extra info
		log.Infof("fullsync status: add extra info")

		inMemoryData := j.progress.InMemoryData.(*inMemoryData)
		snapshotResp := inMemoryData.SnapshotResp
		jobInfo := snapshotResp.GetJobInfo()

		log.Infof("snapshot %s response meta size: %d, job info size: %d, expired at: %d, commit seq: %d",
			inMemoryData.SnapshotName, len(snapshotResp.Meta), len(snapshotResp.JobInfo),
			snapshotResp.GetExpiredAt(), snapshotResp.GetCommitSeq())

		jobInfoBytes, err := j.addExtraInfo(jobInfo)
		if err != nil {
			return err
		}
		log.Debugf("fullsync job info size: %d, bytes: %.128s", len(jobInfoBytes), string(jobInfoBytes))
		snapshotResp.SetJobInfo(jobInfoBytes)

		j.progress.NextSubVolatile(RestoreSnapshot, inMemoryData)

	case RestoreSnapshot:
		// Step 5: Restore snapshot
		log.Infof("fullsync status: restore snapshot")

		if j.progress.InMemoryData == nil {
			persistData := j.progress.PersistData
			inMemoryData := &inMemoryData{}
			if err := json.Unmarshal([]byte(persistData), inMemoryData); err != nil {
				return xerror.Errorf(xerror.Normal, "unmarshal persistData failed, persistData: %s", persistData)
			}
			j.progress.InMemoryData = inMemoryData
		}

		// Step 5.1: cancel the running restore job which by the former process, if exists
		inMemoryData := j.progress.InMemoryData.(*inMemoryData)
		snapshotName := inMemoryData.SnapshotName
		if featureReuseRunningBackupRestoreJob {
			restoreSnapshotName, err := j.IDest.GetValidRestoreJob(snapshotName)
			if err != nil {
				return nil
			}
			if restoreSnapshotName != "" {
				log.Infof("fullsync status: there has a exist restore job %s", restoreSnapshotName)
				inMemoryData.RestoreLabel = restoreSnapshotName
				j.progress.NextSubVolatile(WaitRestoreDone, inMemoryData)
				break
			}
		}

		// Step 5.2: start a new fullsync & restore snapshot to dest
		restoreSnapshotName := NewRestoreLabel(snapshotName)
		snapshotResp := inMemoryData.SnapshotResp
		tableNameMapping := inMemoryData.TableNameMapping

		dest := &j.Dest
		destRpc, err := j.factory.NewFeRpc(dest)
		if err != nil {
			return err
		}
		log.Infof("fullsync status: begin restore snapshot %s to %s", snapshotName, restoreSnapshotName)

		var tableRefs []*festruct.TTableRef
		if j.IsTableSyncWithAlias() {
			log.Debugf("table sync snapshot not same name, table: %s, dest table: %s", j.Src.Table, j.Dest.Table)
			tableRefs = make([]*festruct.TTableRef, 0)
			tableRef := &festruct.TTableRef{
				Table:     &j.Src.Table,
				AliasName: &j.Dest.Table,
			}
			if alias, ok := j.progress.TableAliases[j.Dest.Table]; ok {
				log.Infof("fullsync alias dest table %s to %s", j.Dest.Table, alias)
				tableRef.AliasName = &alias
			}
			tableRefs = append(tableRefs, tableRef)
		} else if len(j.progress.TableAliases) > 0 {
			tableRefs = make([]*festruct.TTableRef, 0)
			viewMap := make(map[string]interface{})
			for _, viewName := range inMemoryData.Views {
				log.Debugf("fullsync alias with view ref %s", viewName)
				viewMap[viewName] = nil
				tableRef := &festruct.TTableRef{Table: utils.ThriftValueWrapper(viewName)}
				tableRefs = append(tableRefs, tableRef)
			}
			for _, tableName := range tableNameMapping {
				if alias, ok := j.progress.TableAliases[tableName]; ok {
					log.Debugf("fullsync alias skip table ref %s because it has alias %s", tableName, alias)
					continue
				}
				if _, ok := viewMap[tableName]; ok {
					continue
				}
				log.Debugf("fullsync alias with table ref %s", tableName)
				tableRef := &festruct.TTableRef{Table: utils.ThriftValueWrapper(tableName)}
				tableRefs = append(tableRefs, tableRef)
			}
			for table, alias := range j.progress.TableAliases {
				log.Infof("fullsync alias table from %s to %s", table, alias)
				tableRef := &festruct.TTableRef{
					Table:     utils.ThriftValueWrapper(table),
					AliasName: utils.ThriftValueWrapper(alias),
				}
				tableRefs = append(tableRefs, tableRef)
			}
		}

		compress := false
		if featureCompressedSnapshot {
			if enable, err := j.IDest.IsEnableRestoreSnapshotCompression(); err != nil {
				return xerror.Wrap(err, xerror.Normal, "check enable restore snapshot compression failed")
			} else {
				compress = enable
			}
		}
		restoreReq := rpc.RestoreSnapshotRequest{
			TableRefs:       tableRefs,
			SnapshotName:    restoreSnapshotName,
			SnapshotResult:  snapshotResp,
			CleanPartitions: false,
			CleanTables:     false,
			AtomicRestore:   false,
			Compress:        compress,
		}
		if featureCleanTableAndPartitions {
			// drop exists partitions, and drop tables if in db sync.
			restoreReq.CleanPartitions = true
			if j.SyncType == DBSync {
				restoreReq.CleanTables = true
			}
		}
		if featureAtomicRestore {
			restoreReq.AtomicRestore = true
		}
		if featureRestoreReplaceDiffSchema {
			restoreReq.ForceReplace = true
		}
		restoreResp, err := destRpc.RestoreSnapshot(dest, &restoreReq)
		if err != nil {
			return err
		}
		if restoreResp.Status.GetStatusCode() != tstatus.TStatusCode_OK {
			return xerror.Errorf(xerror.Normal, "restore snapshot failed, status: %v", restoreResp.Status)
		}
		log.Tracef("fullsync restore snapshot resp: %v", restoreResp)

		inMemoryData.RestoreLabel = restoreSnapshotName
		j.progress.NextSubVolatile(WaitRestoreDone, inMemoryData)
		return nil

	case WaitRestoreDone:
		// Step 6: Wait restore job done
		inMemoryData := j.progress.InMemoryData.(*inMemoryData)
		restoreSnapshotName := inMemoryData.RestoreLabel
		tableNameMapping := inMemoryData.TableNameMapping
		snapshotResp := inMemoryData.SnapshotResp

		if snapshotResp.GetExpiredAt() > 0 && time.Now().UnixMilli() > snapshotResp.GetExpiredAt() {
			log.Warnf("cancel the expired restore job %s", restoreSnapshotName)
			if err := j.IDest.CancelRestoreIfExists(restoreSnapshotName); err != nil {
				return err
			}
			info := fmt.Sprintf("the snapshot %s is expired", restoreSnapshotName)
			log.Infof("force full sync, because %s", info)
			return j.NewSnapshot(j.progress.CommitSeq, info)
		}

		for {
			restoreFinished, err := j.IDest.CheckRestoreFinished(restoreSnapshotName)
			if err != nil && errors.Is(err, base.ErrRestoreSignatureNotMatched) {
				// We need rebuild the exists table.
				var tableName string
				var tableOrView bool = true
				if j.SyncType == TableSync {
					tableName = j.Dest.Table
				} else {
					tableName, tableOrView, err = j.IDest.GetRestoreSignatureNotMatchedTableOrView(restoreSnapshotName)
					if err != nil || len(tableName) == 0 {
						continue
					}
				}

				resource := "table"
				if !tableOrView {
					resource = "view"
				}
				log.Infof("the signature of %s %s is not matched with the target table in snapshot", resource, tableName)
				if tableOrView && featureReplaceNotMatchedWithAlias {
					if j.progress.TableAliases == nil {
						j.progress.TableAliases = make(map[string]string)
					}
					j.progress.TableAliases[tableName] = TableAlias(tableName)
					j.progress.NextSubCheckpoint(GetSnapshotInfo, inMemoryData.SnapshotName) // persist TableAliases
					j.progress.NextSubVolatile(RestoreSnapshot, inMemoryData)
					break
				}
				for {
					if tableOrView {
						if err := j.IDest.DropTable(tableName, false); err == nil {
							break
						}
					} else {
						if err := j.IDest.DropView(tableName); err == nil {
							break
						}
					}
				}
				log.Infof("the restore is cancelled, the unmatched %s %s is dropped, restore snapshot again", resource, tableName)
				j.progress.NextSubVolatile(RestoreSnapshot, inMemoryData)
				break
			} else if err != nil {
				j.progress.NextSubVolatile(RestoreSnapshot, inMemoryData)
				return err
			}

			if !restoreFinished {
				// CheckRestoreFinished already logs the info
				return nil
			}

			if utils.HasJobFailpoint(j.Name, "fullsync_restore_snapshot_rebooting") {
				log.Infof("hit failpoint fullsync_restore_snapshot_rebooting, step to RestoreSnapshot")
				utils.RemoveJobFailpoint(j.Name, "fullsync_restore_snapshot_rebooting")
				j.recoverJobProgress()
				return nil
			}

			tableCommitSeqMap := inMemoryData.TableCommitSeqMap
			var commitSeq int64 = math.MaxInt64
			switch j.SyncType {
			case DBSync:
				for tableId, seq := range tableCommitSeqMap {
					if seq == 0 {
						// Skip the views
						continue
					}
					commitSeq = utils.Min(commitSeq, seq)
					log.Debugf("fullsync table commit seq, table id: %d, commit seq: %d", tableId, seq)
				}
				if snapshotResp.GetCommitSeq() > 0 {
					commitSeq = utils.Min(commitSeq, snapshotResp.GetCommitSeq())
				}
				j.progress.TableCommitSeqMap = tableCommitSeqMap // persist in CommitNext
				j.progress.TableNameMapping = tableNameMapping
			case TableSync:
				commitSeq = tableCommitSeqMap[j.Src.TableId]
			}

			j.progress.CommitNextSubWithPersist(commitSeq, PersistRestoreInfo, restoreSnapshotName)
			break
		}

	case PersistRestoreInfo:
		// Step 7: Update job progress && dest table id
		// update job info, only for dest table id

		if len(j.progress.TableAliases) > 0 {
			log.Infof("fullsync swap %d tables with aliases", len(j.progress.TableAliases))

			var tables []string
			for table := range j.progress.TableAliases {
				tables = append(tables, table)
			}
			for _, table := range tables {
				alias := j.progress.TableAliases[table]
				targetName := table
				if j.IsTableSyncWithAlias() {
					targetName = j.Dest.Table
				}

				// check table exists to ensure the idempotent
				if exist, err := j.IDest.CheckTableExistsByName(alias); err != nil {
					return err
				} else if exist {
					log.Infof("fullsync swap table with alias, table: %s, alias: %s", targetName, alias)
					swap := false // drop the old table
					if err := j.IDest.ReplaceTable(alias, targetName, swap); err != nil {
						return err
					}
				} else {
					log.Infof("fullsync the table alias has been swapped, table: %s, alias: %s", targetName, alias)
				}
			}
			// Since the meta of dest table has been changed, refresh it.
			j.destMeta.ClearTablesCache()

			// Save the replace result
			j.progress.TableAliases = nil
			j.progress.NextSubCheckpoint(PersistRestoreInfo, j.progress.PersistData)
		}

		log.Infof("fullsync status: persist restore info")

		switch j.SyncType {
		case DBSync:
			// refresh dest meta cache before building table mapping.
			j.destMeta.ClearTablesCache()
			tableMapping := make(map[int64]int64)
			for srcTableId := range j.progress.TableCommitSeqMap {
				var srcTableName string
				if name, ok := j.progress.TableNameMapping[srcTableId]; ok {
					srcTableName = name
				} else {
					// Keep compatible, but once the upstream table is renamed, the
					// downstream table id will not be found here.
					name, err := j.srcMeta.GetTableNameById(srcTableId)
					if err != nil {
						return err
					}
					srcTableName = name

					// If srcTableName is empty, it may be deleted.
					// No need to map it to dest table
					if srcTableName == "" {
						log.Warnf("the name of source table id: %d is empty, no need to map it to dest table", srcTableId)
						continue
					}
				}

				destTableId, err := j.destMeta.GetTableId(srcTableName)
				if err != nil {
					return err
				}

				log.Debugf("fullsync table mapping, src: %d, dest: %d, name: %s",
					srcTableId, destTableId, srcTableName)
				tableMapping[srcTableId] = destTableId
			}

			j.progress.TableMapping = tableMapping
			j.progress.ShadowIndexes = nil
			j.progress.PartitionCommitSeqMap = nil
			j.progress.NextWithPersist(j.progress.CommitSeq, DBTablesIncrementalSync, Done, "")
		case TableSync:
			if destTable, err := j.destMeta.UpdateTable(j.Dest.Table, 0); err != nil {
				return err
			} else {
				j.Dest.TableId = destTable.Id
			}

			if err := j.persistJob(); err != nil {
				return err
			}

			j.progress.PartitionCommitSeqMap = nil
			j.progress.TableCommitSeqMap = nil
			j.progress.TableMapping = nil
			j.progress.ShadowIndexes = nil
			j.progress.NextWithPersist(j.progress.CommitSeq, TableIncrementalSync, Done, "")
		default:
			return xerror.Errorf(xerror.Normal, "invalid sync type %d", j.SyncType)
		}

		return nil
	default:
		return xerror.Errorf(xerror.Normal, "invalid job sub sync state %d", j.progress.SubSyncState)
	}

	return j.fullSync()
}

func (j *Job) persistJob() error {
	data, err := json.Marshal(j)
	if err != nil {
		return xerror.Errorf(xerror.Normal, "marshal job failed, job: %v", j)
	}

	if err := j.db.UpdateJob(j.Name, string(data)); err != nil {
		return err
	}

	return nil
}

func (j *Job) newLabel(commitSeq int64) string {
	src := &j.Src
	dest := &j.Dest
	randNum := rand.Intn(65536) // hex 4 chars
	if j.SyncType == DBSync {
		// label "ccrj-rand:${sync_type}:${src_db_id}:${dest_db_id}:${commit_seq}"
		return fmt.Sprintf("ccrj-%x:%s:%d:%d:%d", randNum, j.SyncType, src.DbId, dest.DbId, commitSeq)
	} else {
		// TableSync
		// label "ccrj-rand:${sync_type}:${src_db_id}_${src_table_id}:${dest_db_id}_${dest_table_id}:${commit_seq}"
		return fmt.Sprintf("ccrj-%x:%s:%d_%d:%d_%d:%d", randNum, j.SyncType, src.DbId, src.TableId, dest.DbId, dest.TableId, commitSeq)
	}
}

func (j *Job) IsMaterializedViewTable(srcTableId int64) (bool, error) {
	if featureSkipCheckAsyncMvTable {
		return false, nil
	}

	// 1. skip the OLAP tables
	if j.SyncType == TableSync && srcTableId == j.Src.TableId {
		return false, nil
	}

	if _, ok := j.progress.TableMapping[srcTableId]; ok {
		return false, nil
	}

	// 2. query the cached mv tables
	if _, ok := j.asyncMvTableCache[srcTableId]; ok {
		return true, nil
	}

	// 3. query table from src cluster
	srcTable, err := j.srcMeta.GetTable(srcTableId)
	if err != nil {
		return false, err
	}

	// 4. cache the table if it is a materialized view table
	if srcTable.Type == record.TableTypeMaterializedView {
		if j.asyncMvTableCache == nil {
			j.asyncMvTableCache = make(map[int64]struct{})
		}
		j.asyncMvTableCache[srcTableId] = struct{}{}
		return true, nil
	}

	return false, nil
}

// GetDestTableIdBySrc returns the dest table id by src table id.
// If the src table is dropped, return 0.
// If the src table is a materialized view, return ErrMaterializedViewTable.
func (j *Job) GetDestTableIdBySrc(srcTableId int64) (int64, error) {
	if j.SyncType == TableSync {
		return j.Dest.TableId, nil
	}

	if j.progress.TableMapping != nil {
		if destTableId, ok := j.progress.TableMapping[srcTableId]; ok {
			return destTableId, nil
		}
		log.Warnf("table mapping not found, src table id: %d", srcTableId)
	} else {
		log.Warnf("table mapping not found, src table id: %d", srcTableId)
		j.progress.TableMapping = make(map[int64]int64)
	}

	// WARNING: the table name might be changed, and the TableMapping has been updated in time,
	// only keep this for compatible.
	if srcMeta, err := j.getSourceThriftMeta(srcTableId); err != nil {
		return 0, err
	} else if srcMeta.IsTableDropped(srcTableId) {
		log.Warnf("table %d is dropped, no need to map it to dest table", srcTableId)
		return 0, nil
	} else if tableMeta, err := j.srcMeta.GetTable(srcTableId); err != nil {
		return 0, err
	} else if tableMeta.Type == record.TableTypeMaterializedView {
		return 0, ErrMaterializedViewTable
	} else if destTableId, err := j.destMeta.GetTableId(tableMeta.Name); err != nil {
		return 0, err
	} else {
		j.progress.TableMapping[srcTableId] = destTableId
		return destTableId, nil
	}
}

func (j *Job) GetDestNameBySrcId(srcTableId int64) (string, error) {
	if j.SyncType == TableSync {
		return j.Dest.Table, nil
	}

	destTableId, err := j.GetDestTableIdBySrc(srcTableId)
	if err != nil {
		return "", err
	} else if destTableId == 0 {
		return "", xerror.Errorf(xerror.Normal, "source table %d is dropped", srcTableId)
	}

	name, err := j.destMeta.GetTableNameById(destTableId)
	if err != nil {
		return "", err
	}

	if name == "" {
		return "", xerror.Errorf(xerror.Normal,
			"dest table name not found, src table id: %d, dest table id: %d", srcTableId, destTableId)
	}

	return name, nil
}

func (j *Job) IsBinlogCommitted(tableId int64, binlogCommitSeq int64) bool {
	if _, ok := j.Extra.AppliedBinlogs[binlogCommitSeq]; ok {
		return true
	}

	// FIXME:
	// what happend if the PartitionCommitSeqMap is not empty?
	if j.progress.SyncState == DBTablesIncrementalSync {
		tableCommitSeq, ok := j.progress.TableCommitSeqMap[tableId]
		if ok && binlogCommitSeq <= tableCommitSeq {
			log.Infof("filter the already committed binlog %d, table commit seq: %d, table: %d",
				binlogCommitSeq, tableCommitSeq, tableId)
			return true
		}
	}
	return false
}

func (j *Job) getDbSyncTableRecords(upsert *record.Upsert) []*record.TableRecord {
	commitSeq := upsert.CommitSeq
	tableCommitSeqMap := j.progress.TableCommitSeqMap
	partitionCommitSeqMap := j.progress.PartitionCommitSeqMap
	tableRecords := make([]*record.TableRecord, 0, len(upsert.TableRecords))

	for tableId, tableRecord := range upsert.TableRecords {
		if tableCommitSeq, ok := tableCommitSeqMap[tableId]; ok && commitSeq <= tableCommitSeq {
			// All the partition records of the table have been committed
			continue
		}

		// Filter the committed partitions
		if partitionCommitSeqMap != nil {
			partitionRecords := make([]record.PartitionRecord, 0, len(tableRecord.PartitionRecords))
			for _, partition := range tableRecord.PartitionRecords {
				if partitionCommitSeq, ok := partitionCommitSeqMap[partition.Id]; ok && commitSeq <= partitionCommitSeq {
					continue
				}
				partitionRecords = append(partitionRecords, partition) // copy
			}
			tableRecord.PartitionRecords = partitionRecords
		}

		if len(tableRecord.PartitionRecords) > 0 {
			tableRecords = append(tableRecords, tableRecord)
		}
	}

	return tableRecords
}

func (j *Job) getRelatedTableRecords(upsert *record.Upsert) ([]*record.TableRecord, error) {
	var tableRecords []*record.TableRecord //, 0, len(upsert.TableRecords))

	switch j.SyncType {
	case DBSync:
		records := j.getDbSyncTableRecords(upsert)
		if len(records) == 0 {
			return nil, nil
		}
		tableRecords = records
	case TableSync:
		tableRecord, ok := upsert.TableRecords[j.Src.TableId]
		if !ok {
			return nil, xerror.Errorf(xerror.Normal, "table record not found, table: %s", j.Src.Table)
		}

		// Filter the committed partitions
		partitionCommitSeqMap := j.progress.PartitionCommitSeqMap
		if partitionCommitSeqMap != nil {
			partitionRecords := make([]record.PartitionRecord, 0, len(tableRecord.PartitionRecords))
			for _, partition := range tableRecord.PartitionRecords {
				if partitionCommitSeq, ok := partitionCommitSeqMap[partition.Id]; ok && upsert.CommitSeq <= partitionCommitSeq {
					continue
				}
				partitionRecords = append(partitionRecords, partition) // copy
			}
			tableRecord.PartitionRecords = partitionRecords
		}
		if len(tableRecord.PartitionRecords) == 0 {
			return nil, nil
		}

		tableRecords = make([]*record.TableRecord, 0, 1)
		tableRecords = append(tableRecords, tableRecord)
	default:
		return nil, xerror.Errorf(xerror.Normal, "invalid sync type: %s", j.SyncType)
	}

	return tableRecords, nil
}

// Table ingestBinlog
func (j *Job) ingestBinlog(commitSeq, txnId int64, tableRecords []*record.TableRecord, stidMap map[int64]int64) (
	[]*ttypes.TTabletCommitInfo, []*festruct.TSubTxnInfo, error) {
	isTxnInsert := len(stidMap) > 0
	log.Tracef("txn %d ingest binlog, commitSeq: %d, is txn insert: %t", txnId, commitSeq, isTxnInsert)

	job, err := j.jobFactory.CreateJob(NewIngestContext(commitSeq, txnId, tableRecords, j.progress.TableMapping, stidMap), j, "IngestBinlog")
	if err != nil {
		return nil, nil, err
	}

	ingestBinlogJob, ok := job.(*IngestBinlogJob)
	if !ok {
		return nil, nil, xerror.Errorf(xerror.Normal, "invalid job type, job: %+v", job)
	}

	job.Run()
	if err := job.Error(); err != nil {
		return nil, nil, err
	}
	commitInfos := ingestBinlogJob.CommitInfos()
	if !isTxnInsert {
		return commitInfos, nil, nil
	}

	// When txn insert, use subTxnInfos to commit rather than commitInfos.
	subTxnInfos := ingestBinlogJob.SubTxnInfos()
	return commitInfos, subTxnInfos, nil
}

func (j *Job) handleUpsertWithRetry(binlog *festruct.TBinlog) error {
	err := j.handleUpsert(binlog)
	if !xerror.IsCategory(err, xerror.Meta) {
		return err
	}

	log.Warnf("a meta error occurred, retry to handle upsert binlog again, commitSeq: %d", binlog.GetCommitSeq())
	if j.progress.SubSyncState == RollbackTransaction {
		// rollback transaction firstly
		if err = j.handleUpsert(nil); err != nil {
			return err
		}
	}
	return j.handleUpsert(binlog)
}

func (j *Job) handleUpsert(binlog *festruct.TBinlog) error {
	log.Infof("handle upsert binlog, sub sync state: %s, prevCommitSeq: %d, commitSeq: %d",
		j.progress.SubSyncState, j.progress.PrevCommitSeq, j.progress.CommitSeq)

	// inMemory will be update in state machine, but progress keep any, so progress.inMemory is also latest, well call NextSubCheckpoint don't need to update inMemory in progress
	type inMemoryData struct {
		CommitSeq    int64                       `json:"commit_seq"`
		TxnId        int64                       `json:"txn_id"`
		DestTableIds []int64                     `json:"dest_table_ids"`
		TableRecords []*record.TableRecord       `json:"table_records"`
		CommitInfos  []*ttypes.TTabletCommitInfo `json:"commit_infos"`
		IsTxnInsert  bool                        `json:"is_txn_insert"`
		SourceStids  []int64                     `json:"source_stid"`
		DestStids    []int64                     `json:"desc_stid"`
		SubTxnInfos  []*festruct.TSubTxnInfo     `json:"sub_txn_infos"`
		Label        string                      `json:"label"`
	}

	updateInMemory := func() error {
		if j.progress.InMemoryData == nil {
			persistData := j.progress.PersistData
			inMemoryData := &inMemoryData{}
			if err := json.Unmarshal([]byte(persistData), inMemoryData); err != nil {
				return xerror.Errorf(xerror.Normal, "unmarshal persistData failed, persistData: %s", persistData)
			}
			j.progress.InMemoryData = inMemoryData
		}
		return nil
	}

	rollback := func(err error, inMemoryData *inMemoryData) {
		log.Errorf("txn %d need rollback, commitSeq: %d, label: %s, err: %+v",
			inMemoryData.TxnId, inMemoryData.CommitSeq, inMemoryData.Label, err)
		j.progress.NextSubCheckpoint(RollbackTransaction, inMemoryData)
	}

	committed := func() {
		inMemoryData := j.progress.InMemoryData.(*inMemoryData)
		log.Debugf("txn %d committed, commitSeq: %d, cleanup", inMemoryData.TxnId, j.progress.CommitSeq)
		commitSeq := j.progress.CommitSeq
		destTableIds := inMemoryData.DestTableIds
		j.progress.PrevTxnId = inMemoryData.TxnId
		if j.SyncType == DBSync && len(j.progress.TableCommitSeqMap) > 0 {
			for _, tableId := range destTableIds {
				tableCommitSeq, ok := j.progress.TableCommitSeqMap[tableId]
				if !ok {
					continue
				}

				if tableCommitSeq < commitSeq {
					j.progress.TableCommitSeqMap[tableId] = commitSeq
				}
			}

			j.progress.Persist()
		}
		j.progress.Done()
	}

	dest := &j.Dest
	switch j.progress.SubSyncState {
	case Done:
		if binlog == nil {
			log.Errorf("binlog is nil, %+v", xerror.Errorf(xerror.Normal, "handle nil upsert binlog"))
			return nil
		}

		data := binlog.GetData()
		upsert, err := record.NewUpsertFromJson(data)
		if err != nil {
			return err
		}
		log.Tracef("upsert: %v", upsert)

		// Step 1: get related tableRecords
		var isTxnInsert bool = false
		if len(upsert.Stids) > 0 {
			if !featureTxnInsert {
				log.Warnf("The txn insert is not supported yet")
				return xerror.Errorf(xerror.Normal, "The txn insert is not supported yet")
			}
			isTxnInsert = true
		}

		tableRecords, err := j.getRelatedTableRecords(upsert)
		if err != nil {
			log.Errorf("get related table records failed, err: %+v", err)
			return err
		}
		if len(tableRecords) == 0 {
			log.Debug("no related table records")
			return nil
		}

		destTableIds := make([]int64, 0, len(tableRecords))
		if j.SyncType == DBSync {
			savedRecords := make([]*record.TableRecord, 0, len(tableRecords))
			for _, tableRecord := range tableRecords {
				if isAsyncMv, err := j.IsMaterializedViewTable(tableRecord.Id); err != nil {
					return err
				} else if isAsyncMv {
					// ignore the upsert of materialized view table.
					continue
				} else if destTableId, err := j.GetDestTableIdBySrc(tableRecord.Id); err != nil {
					return err
				} else if destTableId == 0 {
					// Skip the table which is not in the table mapping
					log.Warnf("table %d is not in the table mapping, skip it", tableRecord.Id)
					continue
				} else {
					savedRecords = append(savedRecords, tableRecord)
					destTableIds = append(destTableIds, destTableId)
				}
			}
			tableRecords = savedRecords
		} else {
			destTableIds = append(destTableIds, j.Dest.TableId)
		}
		if len(tableRecords) == 0 {
			log.Debug("no related table records")
			return nil
		}

		log.Debugf("handle upsert, table records: %v", tableRecords)
		inMemoryData := &inMemoryData{
			CommitSeq:    upsert.CommitSeq,
			DestTableIds: destTableIds,
			TableRecords: tableRecords,
			IsTxnInsert:  isTxnInsert,
			SourceStids:  upsert.Stids,
			Label:        upsert.Label,
		}
		j.progress.NextSubVolatile(BeginTransaction, inMemoryData)

	case BeginTransaction:
		// Step 2: begin txn
		inMemoryData := j.progress.InMemoryData.(*inMemoryData)
		commitSeq := inMemoryData.CommitSeq
		sourceStids := inMemoryData.SourceStids
		isTxnInsert := inMemoryData.IsTxnInsert

		destRpc, err := j.factory.NewFeRpc(dest)
		if err != nil {
			return err
		}

		var label string
		if j.Extra.ReuseBinlogLabel {
			label = inMemoryData.Label
		} else {
			label = j.newLabel(commitSeq)
		}
		log.Tracef("begin txn, label: %s, dest: %v, commitSeq: %d", label, dest, commitSeq)

		var beginTxnResp *festruct.TBeginTxnResult_
		if isTxnInsert {
			// when txn insert, give an array length in BeginTransaction, it will return a list of stid
			beginTxnResp, err = destRpc.BeginTransactionForTxnInsert(dest, label, inMemoryData.DestTableIds, int64(len(sourceStids)))
		} else {
			beginTxnResp, err = destRpc.BeginTransaction(dest, label, inMemoryData.DestTableIds)
		}
		if err != nil {
			return err
		}
		log.Tracef("begin txn resp: %v", beginTxnResp)

		if beginTxnResp.GetStatus().GetStatusCode() != tstatus.TStatusCode_OK {
			if isTableNotFound(beginTxnResp.GetStatus()) && j.SyncType == DBSync {
				// It might caused by the staled TableMapping entries.
				// In order to rebuild the dest table ids, this progress should be rollback.
				j.progress.Rollback()
				for _, tableRecord := range inMemoryData.TableRecords {
					delete(j.progress.TableMapping, tableRecord.Id)
				}
			}
			return xerror.Errorf(xerror.Normal, "begin txn failed, status: %v", beginTxnResp.GetStatus())
		}
		txnId := beginTxnResp.GetTxnId()
		if isTxnInsert {
			destStids := beginTxnResp.GetSubTxnIds()
			inMemoryData.DestStids = destStids
			log.Infof("begin txn %d, label: %s, db: %d, destStids: %v",
				txnId, label, beginTxnResp.GetDbId(), destStids)
		} else {
			log.Infof("begin txn %d, label: %s, db: %d", txnId, label, beginTxnResp.GetDbId())
		}

		inMemoryData.TxnId = txnId
		j.progress.NextSubCheckpoint(IngestBinlog, inMemoryData)

	case IngestBinlog:
		log.Trace("ingest binlog")
		if err := updateInMemory(); err != nil {
			return err
		}
		inMemoryData := j.progress.InMemoryData.(*inMemoryData)
		tableRecords := inMemoryData.TableRecords
		txnId := inMemoryData.TxnId
		isTxnInsert := inMemoryData.IsTxnInsert
		commitSeq := inMemoryData.CommitSeq

		// make stidMap, source_stid to dest_stid
		stidMap := make(map[int64]int64)
		if isTxnInsert {
			sourceStids := inMemoryData.SourceStids
			destStids := inMemoryData.DestStids
			if len(sourceStids) == len(destStids) {
				for i := 0; i < len(sourceStids); i++ {
					stidMap[sourceStids[i]] = destStids[i]
				}
			}
		}

		// Step 3: ingest binlog
		commitInfos, subTxnInfos, err := j.ingestBinlog(commitSeq, txnId, tableRecords, stidMap)
		if err == errTriggerPartialSnapshot {
			if j.Extra.PartialSnapshotParams == nil {
				panic("partial snapshot params is nil when trigger partial snapshot")
			}
			j.progress.NextSubCheckpoint(RollbackTransaction, inMemoryData)
		} else if err != nil {
			rollback(err, inMemoryData)
			return err
		} else {
			inMemoryData.CommitInfos = commitInfos
			inMemoryData.SubTxnInfos = subTxnInfos
			j.progress.NextSubCheckpoint(CommitTransaction, inMemoryData)
		}

	case CommitTransaction:
		// Step 4: commit txn
		log.Tracef("commit txn")
		if err := updateInMemory(); err != nil {
			return err
		}
		inMemoryData := j.progress.InMemoryData.(*inMemoryData)
		txnId := inMemoryData.TxnId
		commitInfos := inMemoryData.CommitInfos

		destRpc, err := j.factory.NewFeRpc(dest)
		if err != nil {
			rollback(err, inMemoryData)
			return err
		}

		isTxnInsert := inMemoryData.IsTxnInsert
		subTxnInfos := inMemoryData.SubTxnInfos
		var resp *festruct.TCommitTxnResult_
		if isTxnInsert {
			resp, err = destRpc.CommitTransactionForTxnInsert(dest, txnId, true, subTxnInfos)
		} else {
			onlyCommitTxn := featureSkipWaitingTxnPublish
			resp, err = destRpc.CommitTransaction(dest, txnId, commitInfos, onlyCommitTxn)
		}
		if err != nil {
			rollback(err, inMemoryData)
			return err
		}
		log.Tracef("commit txn %d resp: %v", txnId, resp)

		if statusCode := resp.Status.GetStatusCode(); statusCode == tstatus.TStatusCode_PUBLISH_TIMEOUT {
			dest.WaitTransactionDone(txnId)
		} else if statusCode != tstatus.TStatusCode_OK {
			err := xerror.Errorf(xerror.Normal, "commit txn failed, status: %v", resp.Status)
			rollback(err, inMemoryData)
			return err
		}

		log.Infof("commit txn %d success", txnId)
		committed()
		return nil

	case RollbackTransaction:
		log.Tracef("Rollback txn")
		// Not Step 5: just rollback txn
		if err := updateInMemory(); err != nil {
			return err
		}

		inMemoryData := j.progress.InMemoryData.(*inMemoryData)
		txnId := inMemoryData.TxnId
		destRpc, err := j.factory.NewFeRpc(dest)
		if err != nil {
			return err
		}

		resp, err := destRpc.RollbackTransaction(dest, txnId)
		if err != nil {
			return err
		}
		if resp.Status.GetStatusCode() != tstatus.TStatusCode_OK {
			if isTxnNotFound(resp.Status) {
				log.Warnf("txn not found, txnId: %d", txnId)
			} else if isTxnAborted(resp.Status) {
				log.Infof("txn already aborted, txnId: %d", txnId)
			} else if isTxnCommitted(resp.Status) {
				log.Infof("txn already committed, txnId: %d", txnId)
				committed()
				return nil
			} else {
				return xerror.Errorf(xerror.Normal, "rollback txn failed, status: %v", resp.Status)
			}
		}

		log.Infof("rollback txn %d success", txnId)
		j.progress.Rollback()
		return nil

	default:
		return xerror.Errorf(xerror.Normal, "invalid job sub sync state %d", j.progress.SubSyncState)
	}

	return j.handleUpsert(binlog)
}

// handleAddPartition
func (j *Job) handleAddPartition(binlog *festruct.TBinlog) error {
	log.Infof("handle add partition binlog, prevCommitSeq: %d, commitSeq: %d",
		j.progress.PrevCommitSeq, j.progress.CommitSeq)

	data := binlog.GetData()
	addPartition, err := record.NewAddPartitionFromJson(data)
	if err != nil {
		return err
	}

	if j.IsBinlogCommitted(addPartition.TableId, binlog.GetCommitSeq()) {
		return nil
	}

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

// handleDropPartition
func (j *Job) handleDropPartition(binlog *festruct.TBinlog) error {
	log.Infof("handle drop partition binlog, prevCommitSeq: %d, commitSeq: %d",
		j.progress.PrevCommitSeq, j.progress.CommitSeq)

	data := binlog.GetData()
	dropPartition, err := record.NewDropPartitionFromJson(data)
	if err != nil {
		return err
	}

	if dropPartition.IsTemp {
		log.Infof("Since the temporary partition is not synchronized to the downstream, this binlog is skipped.")
		return nil
	}

	if j.IsBinlogCommitted(dropPartition.TableId, binlog.GetCommitSeq()) {
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

// handleCreateTable
func (j *Job) handleCreateTable(binlog *festruct.TBinlog) error {
	log.Infof("handle create table binlog, prevCommitSeq: %d, commitSeq: %d",
		j.progress.PrevCommitSeq, j.progress.CommitSeq)

	if j.SyncType != DBSync {
		return xerror.Errorf(xerror.Normal, "invalid sync type: %v", j.SyncType)
	}

	data := binlog.GetData()
	createTable, err := record.NewCreateTableFromJson(data)
	if err != nil {
		return err
	}

	if j.IsBinlogCommitted(createTable.TableId, binlog.GetCommitSeq()) {
		return nil
	}

	if createTable.IsCreateElasticSearch() {
		log.Warnf("create table with elasticsearch is not supported yet, skip this binlog")
		return nil
	}

	if createTable.IsCreateMaterializedView() {
		log.Warnf("create async materialized view is not supported yet, skip this binlog")
		return nil
	}

	if FeatureCreateViewDropExists {
		tableName := strings.TrimSpace(createTable.TableName)
		if createTable.IsCreateView() && len(tableName) > 0 {
			// drop view if exists
			log.Infof("feature_create_view_drop_exists is enabled, try drop view %s before creating", tableName)
			if err = j.IDest.DropView(tableName); err != nil {
				return xerror.Wrapf(err, xerror.Normal, "drop view before create view %s, table id=%d",
					tableName, createTable.TableId)
			}
		}
	}

	if createTable.IsCreateTableWithInvertedIndex() {
		log.Infof("create table %s with inverted index, force partial snapshot, commit seq : %d", createTable.TableName, binlog.GetCommitSeq())
		// we need to force replace table to ensure the index id is consistent
		return j.NewPartialSnapshot(createTable.TableId, createTable.TableName, nil, true, false)
	}

	// Some operations, such as DROP TABLE, will be skiped in the partial/full snapshot,
	// in that case, the dest table might already exists, so we need to check it before creating.
	// If the dest table already exists, we need to do a partial snapshot.
	//
	// See test_cds_fullsync_tbl_drop_create.groovy for details
	if j.SyncType == DBSync && !createTable.IsCreateView() {
		if exists, err := j.IDest.CheckTableExistsByName(createTable.TableName); err != nil {
			return err
		} else if exists {
			log.Warnf("the dest table %s already exists, force partial snapshot, commit seq: %d",
				createTable.TableName, binlog.GetCommitSeq())
			replace := true
			isView := false
			return j.NewPartialSnapshot(createTable.TableId, createTable.TableName, nil, replace, isView)
		}
	}

	if FeatureFilterStorageMedium {
		createTable.Sql = FilterStorageMediumFromCreateTableSql(createTable.Sql)
	}
	createTable.Sql = FilterDynamicPartitionStoragePolicyFromCreateTableSql(createTable.Sql)

	if err = j.IDest.CreateTableOrView(createTable, j.Src.Database); err != nil {
		errMsg := err.Error()
		if strings.Contains(errMsg, "Can not found function") {
			log.Warnf("skip creating table/view because the UDF function is not supported yet: %s", errMsg)
			return nil
		} else if strings.Contains(errMsg, "Can not find resource") {
			log.Warnf("skip creating table/view for the resource is not supported yet: %s", errMsg)
			return nil
		} else if createTable.IsCreateView() && strings.Contains(errMsg, "Unknown column") {
			log.Warnf("create view but the column is not found, trigger partial snapshot, commit seq: %d, msg: %s",
				binlog.GetCommitSeq(), errMsg)
			replace := false // new view no need to replace
			isView := true
			return j.NewPartialSnapshot(createTable.TableId, createTable.TableName, nil, replace, isView)
		}
		if len(createTable.TableName) > 0 && IsSessionVariableRequired(errMsg) { // ignore doris 2.0.3
			log.Infof("a session variable is required to create table %s, force partial snapshot, commit seq: %d, msg: %s",
				createTable.TableName, binlog.GetCommitSeq(), errMsg)
			replace := false // new table no need to replace
			isView := false
			return j.NewPartialSnapshot(createTable.TableId, createTable.TableName, nil, replace, isView)
		}
		return xerror.Wrapf(err, xerror.Normal, "create table %d", createTable.TableId)
	}

	j.srcMeta.ClearTablesCache()
	j.destMeta.ClearTablesCache()

	srcTableName := createTable.TableName
	if len(srcTableName) == 0 {
		// the field `TableName` is added after doris 2.0.3, to keep compatible, try read src table
		// name from upstream, but the result might be wrong if upstream has executed rename/replace.
		log.Infof("the table id %d is not found in the binlog record, get the name from the upstream", createTable.TableId)
		srcTableName, err = j.srcMeta.GetTableNameById(createTable.TableId)
		if err != nil {
			return xerror.Errorf(xerror.Normal, "the table with id %d is not found in the upstream cluster, create table: %s",
				createTable.TableId, createTable.String())
		}
	}

	var destTableId int64
	destTableId, err = j.destMeta.GetTableId(srcTableName)
	if err != nil {
		return err
	}

	if j.progress.TableMapping == nil {
		j.progress.TableMapping = make(map[int64]int64)
	}
	j.progress.TableMapping[createTable.TableId] = destTableId
	if j.progress.TableNameMapping == nil {
		j.progress.TableNameMapping = make(map[int64]string)
	}
	j.progress.TableNameMapping[createTable.TableId] = srcTableName
	return nil
}

// handleDropTable
func (j *Job) handleDropTable(binlog *festruct.TBinlog) error {
	log.Infof("handle drop table binlog, prevCommitSeq: %d, commitSeq: %d",
		j.progress.PrevCommitSeq, j.progress.CommitSeq)

	if j.SyncType != DBSync {
		return xerror.Errorf(xerror.Normal, "invalid sync type: %v", j.SyncType)
	}

	data := binlog.GetData()
	dropTable, err := record.NewDropTableFromJson(data)
	if err != nil {
		return err
	}

	if !dropTable.IsView {
		if _, ok := j.progress.TableMapping[dropTable.TableId]; !ok {
			log.Warnf("the dest table is not found, skip drop table binlog, src table id: %d, commit seq: %d",
				dropTable.TableId, binlog.GetCommitSeq())
			// So that the sync state would convert to DBIncrementalSync,
			// see handlePartialSyncTableNotFound for details.
			return nil
		}
	}

	if j.IsBinlogCommitted(dropTable.TableId, binlog.GetCommitSeq()) {
		// So that the sync state would convert to DBIncrementalSync,
		// see handlePartialSyncTableNotFound for details.
		return nil
	}

	tableName := dropTable.TableName
	// deprecated, `TableName` has been added after doris 2.0.0
	if tableName == "" {
		dirtySrcTables := j.srcMeta.DirtyGetTables()
		srcTable, ok := dirtySrcTables[dropTable.TableId]
		if !ok {
			return xerror.Errorf(xerror.Normal, "table not found, tableId: %d", dropTable.TableId)
		}

		tableName = srcTable.Name
	}

	if dropTable.IsView {
		if err = j.IDest.DropView(tableName); err != nil {
			return xerror.Wrapf(err, xerror.Normal, "drop view %s", tableName)
		}
	} else {
		if err = j.IDest.DropTable(tableName, true); err != nil {
			// In apache/doris/common/ErrorCode.java
			//
			// ERR_WRONG_OBJECT(1347, new byte[]{'H', 'Y', '0', '0', '0'}, "'%s.%s' is not %s. %s.")
			if !strings.Contains(err.Error(), "is not TABLE") {
				return xerror.Wrapf(err, xerror.Normal, "drop table %s", tableName)
			} else if err = j.IDest.DropView(tableName); err != nil { // retry with drop view.
				return xerror.Wrapf(err, xerror.Normal, "drop view %s", tableName)
			}
		}
	}

	j.srcMeta.ClearTablesCache()
	j.destMeta.ClearTablesCache()
	delete(j.progress.TableNameMapping, dropTable.TableId)
	delete(j.progress.TableMapping, dropTable.TableId)
	return nil
}

func (j *Job) handleDummy(binlog *festruct.TBinlog) error {
	dummyCommitSeq := binlog.GetCommitSeq()

	info := fmt.Sprintf("handle dummy binlog, need full sync. SyncType: %v, seq: %v", j.SyncType, dummyCommitSeq)
	log.Infof("%s", info)

	return j.NewSnapshot(dummyCommitSeq, info)
}

func (j *Job) handleModifyProperty(binlog *festruct.TBinlog) error {
	log.Infof("handle modify property binlog, prevCommitSeq: %d, commitSeq: %d",
		j.progress.PrevCommitSeq, j.progress.CommitSeq)

	data := binlog.GetData()
	modifyProperty, err := record.NewModifyTablePropertyFromJson(data)
	if err != nil {
		return err
	}

	if j.IsBinlogCommitted(modifyProperty.TableId, binlog.GetCommitSeq()) {
		return nil
	}

	destTableName, err := j.GetDestNameBySrcId(modifyProperty.TableId)
	if err != nil {
		return err
	}
	return j.Dest.ModifyTableProperty(destTableName, modifyProperty)
}

// handleAlterJob
func (j *Job) handleAlterJob(binlog *festruct.TBinlog) error {
	log.Infof("handle alter job binlog, prevCommitSeq: %d, commitSeq: %d",
		j.progress.PrevCommitSeq, j.progress.CommitSeq)

	data := binlog.GetData()
	alterJob, err := record.NewAlterJobV2FromJson(data)
	if err != nil {
		return err
	}

	if j.IsBinlogCommitted(alterJob.TableId, binlog.GetCommitSeq()) {
		if alterJob.Type == record.ALTER_JOB_SCHEMA_CHANGE {
			j.SaveSchemaChangeShadowIndexes(alterJob)
		} else if !FeatureSkipRollupBinlogs && alterJob.Type == record.ALTER_JOB_ROLLUP {
			j.SaveAlterRollupShadowIndex(alterJob)
		}
		return nil
	}

	if isAsyncMv, err := j.IsMaterializedViewTable(alterJob.TableId); err != nil {
		return err
	} else if isAsyncMv {
		log.Warnf("skip alter job for materialized view table %d", alterJob.TableId)
		return nil
	}

	if FeatureSkipRollupBinlogs && alterJob.Type == record.ALTER_JOB_ROLLUP {
		log.Warnf("skip rollup alter job: %s", alterJob)
		return nil
	}

	if alterJob.Type == record.ALTER_JOB_SCHEMA_CHANGE {
		return j.HandleSchemaChange(alterJob)
	} else if alterJob.Type == record.ALTER_JOB_ROLLUP {
		return j.HandleAlterRollup(alterJob)
	} else {
		return xerror.Errorf(xerror.Normal, "unsupported alter job type: %s", alterJob.Type)
	}
}

func (j *Job) SaveAlterRollupShadowIndex(alterJob *record.AlterJobV2) {
	switch alterJob.JobState {
	case record.ALTER_JOB_STATE_PENDING:
		fallthrough
	case record.ALTER_JOB_STATE_WAITING_TXN:
		fallthrough
	case record.ALTER_JOB_STATE_RUNNING:
		// Once the rollup job step to WAITING_TXN, the upsert to the rollup index is allowed,
		// but the dest index of the downstream cluster hasn't been created.
		//
		// To filter the upsert to the rollup index, save the shadow index ids here.
		if j.progress.ShadowIndexes == nil {
			j.progress.ShadowIndexes = make(map[int64]int64)
		}
		if _, ok := j.progress.ShadowIndexes[alterJob.RollupIndexId]; !ok {
			j.progress.ShadowIndexes[alterJob.RollupIndexId] = alterJob.BaseIndexId
			log.Infof("table %d alter rollup save shadow index %d, base index id: %d",
				alterJob.TableId, alterJob.RollupIndexId, alterJob.BaseIndexId)
		}
	case record.ALTER_JOB_STATE_CANCELLED:
		// clear the shadow indexes
		delete(j.progress.ShadowIndexes, alterJob.RollupIndexId)
	case record.ALTER_JOB_STATE_FINISHED:
		// Once partial snapshot finished, the rollup indexes will be convert to normal index.
		delete(j.progress.ShadowIndexes, alterJob.RollupIndexId)
	}
}

func (j *Job) HandleAlterRollup(alterJob *record.AlterJobV2) error {
	j.SaveAlterRollupShadowIndex(alterJob)
	if !alterJob.IsFinished() {
		return nil
	}

	replace := true
	isView := false
	return j.NewPartialSnapshot(alterJob.TableId, alterJob.TableName, nil, replace, isView)
}

func (j *Job) SaveSchemaChangeShadowIndexes(alterJob *record.AlterJobV2) {
	switch alterJob.JobState {
	case record.ALTER_JOB_STATE_PENDING:
		fallthrough
	case record.ALTER_JOB_STATE_WAITING_TXN:
		fallthrough
	case record.ALTER_JOB_STATE_RUNNING:
		// Once the schema change step to WAITING_TXN, the upsert to the shadow indexes is allowed,
		// but the dest indexes of the downstream cluster hasn't been created.
		//
		// To filter the upsert to the shadow indexes, save the shadow index ids here.
		if j.progress.ShadowIndexes == nil {
			j.progress.ShadowIndexes = make(map[int64]int64)
		}
		for shadowIndexId, originIndexId := range alterJob.ShadowIndexes {
			if _, ok := j.progress.ShadowIndexes[shadowIndexId]; !ok {
				j.progress.ShadowIndexes[shadowIndexId] = originIndexId
				log.Infof("table %d schema change job save shadow index %d, origin index id: %d",
					alterJob.TableId, shadowIndexId, originIndexId)
			}
		}
	case record.ALTER_JOB_STATE_CANCELLED:
		// clear the shadow indexes
		for shadowIndexId := range alterJob.ShadowIndexes {
			delete(j.progress.ShadowIndexes, shadowIndexId)
		}
	case record.ALTER_JOB_STATE_FINISHED:
		// Once partial snapshot finished, the shadow indexes will be convert to normal indexes.
		for shadowIndexId := range alterJob.ShadowIndexes {
			delete(j.progress.ShadowIndexes, shadowIndexId)
		}
	}
}

func (j *Job) HandleSchemaChange(alterJob *record.AlterJobV2) error {
	// drop table dropTableSql
	var destTableName string
	if j.SyncType == TableSync {
		destTableName = j.Dest.Table
	} else {
		destTableName = alterJob.TableName
	}

	j.SaveSchemaChangeShadowIndexes(alterJob)
	if !alterJob.IsFinished() {
		return nil
	}

	if FeatureSchemaChangePartialSync && alterJob.Type == record.ALTER_JOB_SCHEMA_CHANGE {
		replaceTable := true
		isView := false
		return j.NewPartialSnapshot(alterJob.TableId, alterJob.TableName, nil, replaceTable, isView)
	}

	var allViewDeleted bool = false
	for {
		// before drop table, drop related view firstly
		if !allViewDeleted {
			views, err := j.IDest.GetAllViewsFromTable(destTableName)
			if err != nil {
				log.Errorf("when alter job, get view from table failed, err : %v", err)
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

	return j.NewSnapshot(j.progress.CommitSeq, info)
}

// handleLightningSchemaChange
func (j *Job) handleLightningSchemaChange(binlog *festruct.TBinlog) error {
	log.Infof("handle lightning schema change binlog, prevCommitSeq: %d, commitSeq: %d",
		j.progress.PrevCommitSeq, j.progress.CommitSeq)

	data := binlog.GetData()
	lightningSchemaChange, err := record.NewModifyTableAddOrDropColumnsFromJson(data)
	if err != nil {
		return err
	}

	if j.IsBinlogCommitted(lightningSchemaChange.TableId, binlog.GetCommitSeq()) {
		return nil
	}

	tableAlias := ""
	if j.IsTableSyncWithAlias() {
		tableAlias = j.Dest.Table
	}
	return j.IDest.LightningSchemaChange(j.Src.Database, tableAlias, lightningSchemaChange)
}

// handle rename column
func (j *Job) handleRenameColumn(binlog *festruct.TBinlog) error {
	log.Infof("handle rename column binlog, prevCommitSeq: %d, commitSeq: %d",
		j.progress.PrevCommitSeq, j.progress.CommitSeq)

	data := binlog.GetData()
	renameColumn, err := record.NewRenameColumnFromJson(data)
	if err != nil {
		return err
	}

	commitSeq := binlog.GetCommitSeq()
	if j.IsBinlogCommitted(renameColumn.TableId, commitSeq) {
		return nil
	}

	destTableName, err := j.GetDestNameBySrcId(renameColumn.TableId)
	if err != nil {
		return err
	}

	return j.IDest.RenameColumn(destTableName, renameColumn)
}

// handle modify comment
func (j *Job) handleModifyComment(binlog *festruct.TBinlog) error {
	log.Infof("handle modify comment binlog, prevCommitSeq: %d, commitSeq: %d",
		j.progress.PrevCommitSeq, j.progress.CommitSeq)

	data := binlog.GetData()
	modifyComment, err := record.NewModifyCommentFromJson(data)
	if err != nil {
		return err
	}

	commitSeq := binlog.GetCommitSeq()
	if j.IsBinlogCommitted(modifyComment.TableId, commitSeq) {
		return nil
	}

	destTableName, err := j.GetDestNameBySrcId(modifyComment.TableId)
	if err != nil {
		return err
	}

	return j.IDest.ModifyComment(destTableName, modifyComment)
}

func (j *Job) handleTruncateTable(binlog *festruct.TBinlog) error {
	log.Infof("handle truncate table binlog, prevCommitSeq: %d, commitSeq: %d",
		j.progress.PrevCommitSeq, j.progress.CommitSeq)

	data := binlog.GetData()
	truncateTable, err := record.NewTruncateTableFromJson(data)
	if err != nil {
		return err
	}

	if j.IsBinlogCommitted(truncateTable.TableId, binlog.GetCommitSeq()) {
		return nil
	}

	var destTableName string
	switch j.SyncType {
	case DBSync:
		destTableName = truncateTable.TableName
	case TableSync:
		destTableName = j.Dest.Table
	default:
		return xerror.Panicf(xerror.Normal, "invalid sync type: %v", j.SyncType)
	}

	err = j.IDest.TruncateTable(destTableName, truncateTable)
	if err == nil {
		j.srcMeta.ClearTable(j.Src.Database, truncateTable.TableName)
		j.destMeta.ClearTable(j.Dest.Database, destTableName)
	}

	return err
}

func (j *Job) handleReplacePartitions(binlog *festruct.TBinlog) error {
	log.Infof("handle replace partitions binlog, prevCommitSeq: %d, commitSeq: %d",
		j.progress.PrevCommitSeq, j.progress.CommitSeq)

	data := binlog.GetData()
	replacePartition, err := record.NewReplacePartitionFromJson(data)
	if err != nil {
		return err
	}

	if j.IsBinlogCommitted(replacePartition.TableId, binlog.GetCommitSeq()) {
		return nil
	}

	if isAsyncMv, err := j.IsMaterializedViewTable(replacePartition.TableId); err != nil {
		return err
	} else if isAsyncMv {
		log.Warnf("skip replace partitions for materialized view table %d", replacePartition.TableId)
		return nil
	}

	if !replacePartition.StrictRange {
		log.Warnf("replace partitions with non strict range is not supported yet, replace partition record: %s", string(data))
		replace := true
		isView := false
		return j.NewPartialSnapshot(replacePartition.TableId, replacePartition.TableName, nil, replace, isView)
	}

	if replacePartition.UseTempName {
		log.Warnf("replace partitions with use tmp name is not supported yet, replace partition record: %s", string(data))
		replace := true
		isView := false
		return j.NewPartialSnapshot(replacePartition.TableId, replacePartition.TableName, nil, replace, isView)
	}

	oldPartitions := strings.Join(replacePartition.Partitions, ",")
	newPartitions := strings.Join(replacePartition.TempPartitions, ",")
	log.Infof("table %s replace partitions %s with temp partitions %s",
		replacePartition.TableName, oldPartitions, newPartitions)

	partitions := replacePartition.Partitions
	isView := false
	return j.NewPartialSnapshot(replacePartition.TableId, replacePartition.TableName, partitions, false, isView)
}

func (j *Job) handleModifyPartitions(binlog *festruct.TBinlog) error {
	log.Infof("handle modify partitions binlog, prevCommitSeq: %d, commitSeq: %d",
		j.progress.PrevCommitSeq, j.progress.CommitSeq)

	log.Warnf("modify partitions is not supported now, binlog data: %s", binlog.GetData())
	return nil
}

// handle rename table
func (j *Job) handleRenameTable(binlog *festruct.TBinlog) error {
	log.Infof("handle rename table binlog, prevCommitSeq: %d, commitSeq: %d",
		j.progress.PrevCommitSeq, j.progress.CommitSeq)

	data := binlog.GetData()
	renameTable, err := record.NewRenameTableFromJson(data)
	if err != nil {
		return err
	}

	// don't support rename table when table sync
	if j.SyncType == TableSync {
		log.Warnf("rename table is not supported when table sync, consider rebuilding this job instead")
		return xerror.Errorf(xerror.Normal, "rename table is not supported when table sync, consider rebuilding this job instead")
	}

	commitSeq := binlog.GetCommitSeq()
	if j.IsBinlogCommitted(renameTable.TableId, commitSeq) {
		return nil
	}

	if isAsyncMv, err := j.IsMaterializedViewTable(renameTable.TableId); err != nil {
		return err
	} else if isAsyncMv {
		log.Warnf("skip rename table for materialized view table %d", renameTable.TableId)
		return nil
	}

	destTableName, err := j.GetDestNameBySrcId(renameTable.TableId)
	if err != nil {
		return err
	}

	if renameTable.NewTableName != "" && renameTable.OldTableName == "" {
		// for compatible with old doris version
		//
		// If we synchronize all operations accurately, then the old table name should be equal to
		// the destination table name.
		renameTable.OldTableName = destTableName
	}

	err = j.IDest.RenameTable(destTableName, renameTable)
	if err != nil {
		return err
	}

	j.destMeta.GetTables()
	if j.progress.TableNameMapping == nil {
		j.progress.TableNameMapping = make(map[int64]string)
	}
	j.progress.TableNameMapping[renameTable.TableId] = renameTable.NewTableName

	return nil
}

func (j *Job) handleReplaceTable(binlog *festruct.TBinlog) error {
	log.Infof("handle replace table binlog, prevCommitSeq: %d, commitSeq: %d",
		j.progress.PrevCommitSeq, j.progress.CommitSeq)

	record, err := record.NewReplaceTableRecordFromJson(binlog.GetData())
	if err != nil {
		return err
	}

	commitSeq := binlog.GetCommitSeq()
	if j.SyncType == TableSync {
		info := fmt.Sprintf("replace table %s with fullsync in table sync, reset src table id from %d to %d, swap: %t",
			record.OriginTableName, record.OriginTableId, record.NewTableId, record.SwapTable)
		log.Infof("%s", info)
		j.Src.TableId = record.NewTableId
		return j.NewSnapshot(commitSeq, info)
	}

	if isAsyncMv, err := j.IsMaterializedViewTable(record.OriginTableId); err != nil {
		return err
	} else if isAsyncMv {
		log.Warnf("skip replace table for materialized view table %d", record.OriginTableId)
		return nil
	}

	if j.progress.SyncState == DBTablesIncrementalSync {
		// if original table already committed, new partial snapshot with the new table
		// if new table already committed, new partial snapshot with the original table
		// if both table are committed, skip this binlog
		originTableSynced := j.progress.TableCommitSeqMap[record.OriginTableId] >= commitSeq
		newTableSynced := j.progress.TableCommitSeqMap[record.NewTableId] >= commitSeq
		if originTableSynced && newTableSynced {
			log.Infof("filter replace table binlog, both tables are synced, origin table %s id: %d, new table %s id: %d, commit seq: %d",
				record.OriginTableName, record.OriginTableId, record.NewTableName, record.NewTableId, commitSeq)
			return nil
		} else if originTableSynced && !record.SwapTable {
			log.Infof("filter replace table binlog, the origin table %s id %d already synced, commit seq: %d, swap = false",
				record.OriginTableName, record.OriginTableId, commitSeq)
			return nil
		} else if originTableSynced && record.SwapTable {
			log.Infof("force new partial snapshot, origin table %s id %d already synced, commit seq: %d",
				record.OriginTableName, record.OriginTableId, commitSeq)
			return j.NewPartialSnapshot(record.NewTableId, record.OriginTableName, nil, false, false)
		} else if newTableSynced && !record.SwapTable {
			log.Infof("filter replace table binlog, the new table %s id %d already synced, commit seq: %d, swap = false",
				record.NewTableName, record.NewTableId, commitSeq)
			return nil
		} else if newTableSynced && record.SwapTable {
			log.Infof("force new partial snapshot, new table %s id %d already synced, commit seq: %d",
				record.NewTableName, record.NewTableId, commitSeq)
			return j.NewPartialSnapshot(record.OriginTableId, record.NewTableName, nil, false, false)
		}
	}

	toName := record.OriginTableName
	fromName := record.NewTableName
	if err := j.IDest.ReplaceTable(fromName, toName, record.SwapTable); err != nil {
		return err
	}

	j.destMeta.GetTables() // update id <=> name cache
	if j.progress.TableNameMapping == nil {
		j.progress.TableNameMapping = make(map[int64]string)
	}
	if record.SwapTable {
		// keep table mapping
		j.progress.TableNameMapping[record.OriginTableId] = record.NewTableName
		j.progress.TableNameMapping[record.NewTableId] = record.OriginTableName
	} else { // delete table1
		j.progress.TableNameMapping[record.NewTableId] = record.OriginTableName
		delete(j.progress.TableNameMapping, record.OriginTableId)
		delete(j.progress.TableMapping, record.OriginTableId)
	}

	return nil
}

func (j *Job) handleModifyTableAddOrDropInvertedIndices(binlog *festruct.TBinlog) error {
	log.Infof("handle modify table add or drop inverted indices binlog, prevCommitSeq: %d, commitSeq: %d",
		j.progress.PrevCommitSeq, j.progress.CommitSeq)

	data := binlog.GetData()
	record, err := record.NewModifyTableAddOrDropInvertedIndicesFromJson(data)
	if err != nil {
		return err
	}

	commitSeq := binlog.GetCommitSeq()
	if j.IsBinlogCommitted(record.TableId, commitSeq) {
		return nil
	}

	if record.IsDropInvertedIndex {
		destTableName, err := j.GetDestNameBySrcId(record.TableId)
		if err != nil {
			return err
		}

		return j.IDest.LightningIndexChange(destTableName, record)
	}

	// Get the source table name, and trigger a partial snapshot
	var tableName string
	if j.SyncType == TableSync {
		// for table sync with alias
		tableName = j.Src.Table
	} else {
		if name, err := j.GetDestNameBySrcId(record.TableId); err != nil {
			return xerror.Errorf(xerror.Normal, "get dest table name by src id %d failed, err: %v", record.TableId, err)
		} else {
			tableName = name
		}
	}

	replace := true
	isView := false
	return j.NewPartialSnapshot(record.TableId, tableName, nil, replace, isView)
}

func (j *Job) handleIndexChangeJob(binlog *festruct.TBinlog) error {
	log.Infof("handle index change job binlog, prevCommitSeq: %d, commitSeq: %d",
		j.progress.PrevCommitSeq, j.progress.CommitSeq)

	data := binlog.GetData()
	indexChangeJob, err := record.NewIndexChangeJobFromJson(data)
	if err != nil {
		return err
	}

	commitSeq := binlog.GetCommitSeq()
	if j.IsBinlogCommitted(indexChangeJob.TableId, commitSeq) {
		return nil
	}

	if indexChangeJob.JobState != record.INDEX_CHANGE_JOB_STATE_FINISHED ||
		indexChangeJob.IsDropOp {
		log.Debugf("skip index change job binlog, job state: %s, is drop op: %t",
			indexChangeJob.JobState, indexChangeJob.IsDropOp)
		return nil
	}

	var destTableName string
	if j.SyncType == TableSync {
		destTableName = j.Dest.Table
	} else {
		destTableName = indexChangeJob.TableName
	}

	return j.IDest.BuildIndex(destTableName, indexChangeJob)
}

// handle alter view def
func (j *Job) handleAlterViewDef(binlog *festruct.TBinlog) error {
	log.Infof("handle alter view def binlog, prevCommitSeq: %d, commitSeq: %d",
		j.progress.PrevCommitSeq, j.progress.CommitSeq)

	data := binlog.GetData()
	alterView, err := record.NewAlterViewFromJson(data)
	if err != nil {
		return err
	}

	commitSeq := binlog.GetCommitSeq()
	if j.IsBinlogCommitted(alterView.TableId, commitSeq) {
		return nil
	}

	viewName, err := j.GetDestNameBySrcId(alterView.TableId)
	if err != nil {
		return err
	}

	if err := j.IDest.AlterViewDef(j.Src.Database, viewName, alterView); err != nil {
		if strings.Contains(err.Error(), "Unknown column") {
			log.Warnf("alter view but the column is not found, trigger partial snapshot, commit seq: %d, msg: %s",
				binlog.GetCommitSeq(), err.Error())
			replace := false
			isView := true
			return j.NewPartialSnapshot(alterView.TableId, viewName, nil, replace, isView)
		}
	}
	return nil
}

func (j *Job) handleRenamePartition(binlog *festruct.TBinlog) error {
	log.Infof("handle rename partition binlog, prevCommitSeq: %d, commitSeq: %d",
		j.progress.PrevCommitSeq, j.progress.CommitSeq)

	data := binlog.GetData()
	renamePartition, err := record.NewRenamePartitionFromJson(data)
	if err != nil {
		return err
	}

	commitSeq := binlog.GetCommitSeq()
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

func (j *Job) handleRenameRollup(binlog *festruct.TBinlog) error {
	log.Infof("handle rename rollup binlog, prevCommitSeq: %d, commitSeq: %d",
		j.progress.PrevCommitSeq, j.progress.CommitSeq)

	data := binlog.GetData()
	renameRollup, err := record.NewRenameRollupFromJson(data)
	if err != nil {
		return err
	}

	commitSeq := binlog.GetCommitSeq()
	if j.IsBinlogCommitted(renameRollup.TableId, commitSeq) {
		return nil
	}

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

func (j *Job) handleDropRollup(binlog *festruct.TBinlog, allowNotExists bool) error {
	log.Infof("handle drop rollup binlog, prevCommitSeq: %d, commitSeq: %d, allowNotExists: %t",
		j.progress.PrevCommitSeq, j.progress.CommitSeq, allowNotExists)

	data := binlog.GetData()
	dropRollup, err := record.NewDropRollupFromJson(data)
	if err != nil {
		return err
	}

	commitSeq := binlog.GetCommitSeq()
	if j.IsBinlogCommitted(dropRollup.TableId, commitSeq) {
		return nil
	}

	var destTableName string
	if j.SyncType == TableSync {
		destTableName = j.Dest.Table
	} else {
		destTableName = dropRollup.TableName
	}

	err = j.IDest.DropRollup(destTableName, dropRollup.IndexName)
	if err != nil {
		msg := fmt.Sprintf("Materialized view [%s] does not exist", dropRollup.IndexName)
		if allowNotExists && strings.Contains(err.Error(), msg) {
			return nil
		}
		return err
	}
	return nil
}

func (j *Job) handleRecoverInfo(binlog *festruct.TBinlog) error {
	log.Infof("handle recoverInfo binlog, prevCommitSeq: %d, commitSeq: %d",
		j.progress.PrevCommitSeq, j.progress.CommitSeq)

	data := binlog.GetData()
	recoverInfo, err := record.NewRecoverInfoFromJson(data)
	if err != nil {
		return err
	}

	commitSeq := binlog.GetCommitSeq()
	if j.IsBinlogCommitted(recoverInfo.TableId, commitSeq) {
		return nil
	}

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

func (j *Job) handleBarrier(binlog *festruct.TBinlog) error {
	data := binlog.GetData()
	barrierLog, err := record.NewBarrierLogFromJson(data)
	if err != nil {
		return err
	}

	if barrierLog.Binlog == "" {
		log.Info("handle barrier binlog, ignore it")
		return nil
	}

	// keep compatible with old version
	binlogType := festruct.TBinlogType(barrierLog.BinlogType)
	log.Infof("handle barrier binlog with type %s, prevCommitSeq: %d, commitSeq: %d",
		binlogType, j.progress.PrevCommitSeq, j.progress.CommitSeq)

	newBinlog := festruct.NewTBinlog()
	newBinlog.SetCommitSeq(utils.ThriftValueWrapper(binlog.GetCommitSeq()))
	newBinlog.SetTimestamp(utils.ThriftValueWrapper(binlog.GetTimestamp()))
	newBinlog.SetType(&binlogType)
	newBinlog.SetDbId(utils.ThriftValueWrapper(binlog.GetDbId()))
	newBinlog.SetData(&barrierLog.Binlog)
	newBinlog.SetTableIds(binlog.GetTableIds())
	return j.handleNonBarrierBinlog(newBinlog)
}

// return: error && bool backToRunLoop
func (j *Job) handleBinlogs(binlogs []*festruct.TBinlog) (error, bool) {
	log.Tracef("handle binlogs, binlogs size: %d", len(binlogs))

	for _, binlog := range binlogs {
		// Step 1: dispatch handle binlog
		if err, ok := j.handleBinlog(binlog); err != nil {
			return err, false
		} else if ok || j.hasInterruptSignal() {
			return nil, true // back to run loop
		}
	}
	return nil, false
}

func (j *Job) handleBinlog(binlog *festruct.TBinlog) (error, bool) {
	if err := j.handleBinlogInternal(binlog); err != nil {
		log.Errorf("handle binlog failed, prevCommitSeq: %d, commitSeq: %d, binlog type: %s, binlog data: %s",
			j.progress.PrevCommitSeq, j.progress.CommitSeq, binlog.GetType(), binlog.GetData())
		return err, false
	}

	if j.Extra.PartialSnapshotParams != nil {
		params := j.Extra.PartialSnapshotParams
		if err := j.NewPartialSnapshot(params.TableId, params.TableName, params.Partitions, params.Replace, params.IsView); err != nil {
			return err, false
		}
		return nil, true
	}

	// Step 2: check job state, if not incrementalSync, such as DBPartialSync, break
	if !j.isIncrementalSync() {
		log.Tracef("job state is not incremental sync, back to run loop, job state: %s", j.progress.SyncState)
		return nil, true
	}

	// Step 3: update progress
	j.afterHandleBinlog(binlog.GetCommitSeq())

	// Step 4: update progress to db
	if !j.progress.IsDone() {
		j.progress.Done()
	}

	// release the binlogs before PrevCommitSeq.
	if err := j.lockBinlog(j.progress.PrevCommitSeq); err != nil {
		return err, false
	}

	j.updateJobStatus()

	return nil, false
}

// After the binlog is handled ...
func (j *Job) afterHandleBinlog(commitSeq int64) {
	if j.SyncType == DBSync && j.progress.TableCommitSeqMap != nil {
		// when all table commit seq > commitSeq, it's true
		reachSwitchToDBIncrementalSync := true
		for _, tableCommitSeq := range j.progress.TableCommitSeqMap {
			if tableCommitSeq > commitSeq {
				reachSwitchToDBIncrementalSync = false
				break
			}
		}
		for _, partitionCommitSeq := range j.progress.PartitionCommitSeqMap {
			if partitionCommitSeq > commitSeq {
				reachSwitchToDBIncrementalSync = false
				break
			}
		}

		if reachSwitchToDBIncrementalSync {
			log.Infof("all table/partition commit seq reach the commit seq, switch to incremental sync, commit seq: %d", commitSeq)
			j.progress.TableCommitSeqMap = nil
			j.progress.PartitionCommitSeqMap = nil
			j.progress.SyncState = DBIncrementalSync
		}
	}
}

func (j *Job) isModifyTableColumnsCommitted(record *record.ModifyTableAddOrDropColumns) (bool, error) {
	if record.BaseIndexId == 0 {
		log.Warnf("modify table columns record has no base index id, assume it is not committed, table id %d", record.TableId)
		return false, nil
	}

	tableName, err := j.GetDestNameBySrcId(record.TableId)
	if err != nil {
		log.Errorf("get dest table name by src id %d failed, err: %v", record.TableId, err)
		return false, err
	}

	materializedIndexes, err := j.destMeta.DescribeTableAll(tableName)
	if err != nil {
		return false, err
	}

	baseIndex, ok := materializedIndexes[tableName]
	if !ok {
		return false, xerror.Errorf(xerror.Normal, "the base index %s not exists in DESC table ALL result", tableName)
	}

	log.Debugf("desc all table: %v", baseIndex)

	destColumnMap := make(map[string]struct{})
	for _, col := range baseIndex.ColumnDesc {
		destColumnMap[col.Name] = struct{}{}
	}
	columnSchema := record.IndexSchemaMap[record.BaseIndexId]
	for _, c := range columnSchema {
		if !c.Visible {
			continue
		}
		exists := false
		for _, col := range baseIndex.ColumnDesc {
			// To keep the logical simple, we don't compare the column type and other properties.
			if c.Name == col.Name {
				exists = true
			}
		}
		if !exists {
			log.Debugf("column %s not exists in dest table %s, it is not committed",
				c.Name, tableName)
			return false, nil
		}
		delete(destColumnMap, c.Name)
	}

	if len(destColumnMap) > 0 {
		log.Debugf("the modify table columns binlog is not contains columns'%s' in dest table %s",
			strings.Join(utils.Keys(destColumnMap), ","), tableName)
		return false, nil
	}

	return true, nil
}

func (j *Job) isDropRollupCommitted(record *record.DropRollup) (bool, error) {
	destTableName, err := j.GetDestNameBySrcId(record.TableId)
	if err != nil {
		log.Errorf("get dest table name by src id %d failed, err: %v", record.TableId, err)
		return false, err
	}

	descResult, err := j.destMeta.DescribeTableAll(destTableName)
	if err != nil {
		return false, err
	}

	if _, ok := descResult[record.IndexName]; ok {
		return false, nil
	}

	return true, nil
}

func (j *Job) isRenameTableCommitted(record *record.RenameTable) (bool, error) {
	showResult, err := j.destMeta.ShowTables()
	if err != nil {
		return false, err
	}

	return utils.Contains(showResult, record.NewTableName), nil
}

func (j *Job) isModifyTableInvertedIndicesCommitted(record *record.ModifyTableAddOrDropInvertedIndices) (bool, error) {
	tableName, err := j.GetDestNameBySrcId(record.TableId)
	if err != nil {
		log.Errorf("get dest table name by src id %d failed, err: %v", record.TableId, err)
		return false, err
	}

	indices, err := j.destMeta.ShowIndexes(tableName)
	if err != nil {
		return false, err
	}

	invertedIndices := make(map[string]struct{})
	for _, index := range indices {
		if index.IndexType == IndexTypeInverted {
			invertedIndices[index.Name] = struct{}{}
		}
	}

	if record.IsDropInvertedIndex {
		for _, index := range record.AlternativeIndexes {
			if _, ok := invertedIndices[index.GetIndexName()]; ok {
				log.Infof("inverted index %s is not dropped in dest table %s, this binlog is not committed",
					index.GetIndexName(), tableName)
				return false, nil
			}
		}
	} else {
		for _, index := range record.AlternativeIndexes {
			if _, ok := invertedIndices[index.GetIndexName()]; !ok {
				log.Infof("inverted index %s is not added in dest table %s, this binlog is not committed",
					index.GetIndexName(), tableName)
				return false, nil
			}
		}
	}

	return true, nil
}

func (j *Job) isRenameColumnCommitted(record *record.RenameColumn) (bool, error) {
	destTableName, err := j.GetDestNameBySrcId(record.TableId)
	if err != nil {
		log.Errorf("get dest table name by src id %d failed, err: %v", record.TableId, err)
		return false, err
	}

	descResult, err := j.destMeta.DescribeTableAll(destTableName)
	if err != nil {
		return false, err
	}

	baseIndex, ok := descResult[destTableName]
	if !ok {
		return false, xerror.Errorf(xerror.Normal, "the base index %s not exists in DESC table ALL result", destTableName)
	}

	log.Debugf("desc all table: %v", baseIndex)

	for _, column := range baseIndex.ColumnDesc {
		if column.Name == record.ColName {
			log.Infof("column %s is not renamed to %s in dest table %s, this binlog is not committed",
				record.ColName, record.NewColName, destTableName)
			return false, nil
		}
	}

	log.Infof("column %s is renamed to %s in dest table %s, this binlog is committed",
		record.ColName, record.NewColName, destTableName)
	return true, nil
}

func (j *Job) isRenameRollupCommitted(record *record.RenameRollup) (bool, error) {
	destTableName, err := j.GetDestNameBySrcId(record.TableId)
	if err != nil {
		log.Errorf("get dest table name by src id %d failed, err: %v", record.TableId, err)
		return false, err
	}

	descResult, err := j.destMeta.DescribeTableAll(destTableName)
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

func (j *Job) isRenamePartitionCommitted(record *record.RenamePartition) (bool, error) {
	destTableId, err := j.GetDestTableIdBySrc(record.TableId)
	if err != nil {
		return false, err
	} else if destTableId == 0 {
		return false, nil
	}

	if err := j.destMeta.UpdatePartitions(destTableId); err != nil {
		return false, err
	}

	partitions, err := j.destMeta.GetPartitionIdMap(destTableId)
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

func (j *Job) isModifyDistributionTypeCommitted(r *record.ModifyDistributionType) (bool, error) {
	j.GetDestMeta().GetTable(r.GetTableId())
	destTableName, err := j.GetDestNameBySrcId(r.GetTableId())
	if err != nil {
		return false, err
	}
	return j.CheckCreateTable(destTableName, "DISTRIBUTED BY RANDOM")
}

// determineBinlogState determines whether the unknown binlog is committed or not.
// The result is true if the binlog is committed, otherwise false.
func (j *Job) determineBinlogState(binlog *festruct.TBinlog) (bool, error) {
	commitSeq := binlog.GetCommitSeq()
	if commitSeq != j.progress.UnknownCommitSeq {
		panic("commit seq not match")
	}

	if IsJobHandleRegistered(binlog.GetType()) {
		return IsBinlogCommitted(j, binlog)
	}

	isIdempotent := func(binlogType festruct.TBinlogType) bool {
		switch binlogType {
		// Idempotent when EXISTS/NOT EXISTS
		case festruct.TBinlogType_CREATE_TABLE:
		case festruct.TBinlogType_DROP_PARTITION:
		case festruct.TBinlogType_ADD_PARTITION:
		case festruct.TBinlogType_DROP_TABLE:

		// Idempotent when executing twice
		case festruct.TBinlogType_MODIFY_VIEW_DEF:
		case festruct.TBinlogType_TRUNCATE_TABLE:
		case festruct.TBinlogType_MODIFY_COMMENT:
		case festruct.TBinlogType_ALTER_DATABASE_PROPERTY:
		case festruct.TBinlogType_MODIFY_TABLE_PROPERTY:
		case festruct.TBinlogType_MODIFY_PARTITIONS:
		case festruct.TBinlogType_INDEX_CHANGE_JOB:
		case festruct.TBinlogType_MODIFY_DISTRIBUTION_BUCKET_NUM:

		default:
			return false
		}
		return true
	}

	isSyncedBySnapshot := func(binlogType festruct.TBinlogType) bool {
		// These binlog types will trigger partial snapshot, so we can skip them.
		switch binlogType {
		case festruct.TBinlogType_REPLACE_PARTITIONS:
		case festruct.TBinlogType_RECOVER_INFO:
		case festruct.TBinlogType_ALTER_JOB:
		default:
			return false
		}
		return true
	}

	binlogType := binlog.GetType()
	if isIdempotent(binlogType) || isSyncedBySnapshot(binlogType) {
		return false, nil
	}

	if binlogType == festruct.TBinlogType_REPLACE_TABLE {
		// We can't determine whether the binlog is committed or not, trigger full sync.
		return true, j.NewSnapshot(commitSeq, "the REPLACE_TABLE binlog state is unknown")
	}

	switch binlogType {
	case festruct.TBinlogType_DUMMY:
		return true, nil
	case festruct.TBinlogType_UPSERT:
		return false, xerror.Errorf(xerror.Normal, "UPSERT binlog should not step into here, commit seq %d", commitSeq)

	case festruct.TBinlogType_DROP_ROLLUP:
		dropRollup, err := record.NewDropRollupFromJson(binlog.GetData())
		if err != nil {
			return false, err
		}
		return j.isDropRollupCommitted(dropRollup)

	case festruct.TBinlogType_MODIFY_TABLE_ADD_OR_DROP_COLUMNS:
		modifyTableAddOrDropColumns, err := record.NewModifyTableAddOrDropColumnsFromJson(binlog.GetData())
		if err != nil {
			return false, nil
		}
		return j.isModifyTableColumnsCommitted(modifyTableAddOrDropColumns)
	case festruct.TBinlogType_MODIFY_TABLE_ADD_OR_DROP_INVERTED_INDICES:
		modifyTableAddOrDropInvertedIndices, err := record.NewModifyTableAddOrDropInvertedIndicesFromJson(binlog.GetData())
		if err != nil {
			return false, nil
		}
		return j.isModifyTableInvertedIndicesCommitted(modifyTableAddOrDropInvertedIndices)
	case festruct.TBinlogType_RENAME_TABLE:
		renameTable, err := record.NewRenameTableFromJson(binlog.GetData())
		if err != nil {
			return false, err
		}
		return j.isRenameTableCommitted(renameTable)
	case festruct.TBinlogType_RENAME_PARTITION:
		renamePartitionRecord, err := record.NewRenamePartitionFromJson(binlog.GetData())
		if err != nil {
			return false, err
		}
		return j.isRenamePartitionCommitted(renamePartitionRecord)
	case festruct.TBinlogType_RENAME_ROLLUP:
		renameRollupRecord, err := record.NewRenameRollupFromJson(binlog.GetData())
		if err != nil {
			return false, err
		}
		return j.isRenameRollupCommitted(renameRollupRecord)
	case festruct.TBinlogType_RENAME_COLUMN:
		renameColumnRecord, err := record.NewRenameColumnFromJson(binlog.GetData())
		if err != nil {
			return false, nil
		}
		return j.isRenameColumnCommitted(renameColumnRecord)
	case festruct.TBinlogType_MODIFY_DISTRIBUTION_TYPE:
		modifyDistributionType, err := record.NewModifyDistributionTypeFromJson(binlog.GetData())
		if err != nil {
			return false, nil
		}
		return j.isModifyDistributionTypeCommitted(modifyDistributionType)

	default:
		return false, xerror.Errorf(xerror.Normal, "unknown binlog type: %v, commit seq %d, data %s",
			binlogType, commitSeq, binlog.GetData())
	}
}

func (j *Job) handleBinlogInternal(binlog *festruct.TBinlog) error {
	if binlog == nil || !binlog.IsSetCommitSeq() {
		return xerror.Errorf(xerror.Normal, "invalid binlog: %v", binlog)
	}

	if !j.progress.IsDone() {
		return xerror.Errorf(xerror.Normal, "the progress isn't done, need rollback, commit seq: %d", j.progress.CommitSeq)
	}

	commitSeq := binlog.GetCommitSeq()
	binlogType := binlog.GetType()
	log.Debugf("binlog type: %s, commit seq: %d, binlog data: %s", binlogType, commitSeq, binlog.GetData())

	// Step 2: update job progress
	j.progress.StartHandle(commitSeq)
	defer xmetrics.RecordHandlingBinlog(j.Name, commitSeq)()

	// Skip binlog conditionally
	if j.Extra.SkipBinlog && j.Extra.SkipBy == SkipBySilence && j.Extra.SkipCommitSeq == commitSeq {
		log.Warnf("silently skip binlog %d by user, binlog type: %s, binlog data: %s",
			commitSeq, binlogType, binlog.GetData())
		return nil
	}

	if utils.HasJobFailpoint(j.Name, "handle_binlog_failed") {
		log.Warnf("fail to handle binlog by failpoint, binlog type: %s, binlog data: %s", binlogType, binlog.GetData())
		return xerror.Errorf(xerror.Normal, "fail to handle binlog by failpoint")
	}

	if binlogType == festruct.TBinlogType_BARRIER {
		return j.handleBarrier(binlog)
	}

	return j.handleNonBarrierBinlog(binlog)
}

func (j *Job) handleNonBarrierBinlog(binlog *festruct.TBinlog) error {
	binlogType := binlog.GetType()
	commitSeq := binlog.GetCommitSeq()

	if binlogType == festruct.TBinlogType_UPSERT {
		return j.handleUpsertWithRetry(binlog)
	}

	if prevTxnId := j.progress.PrevTxnId; prevTxnId != -1 {
		dest := &j.Dest
		log.Infof("wait prev txn id: %d published", prevTxnId)
		dest.WaitTransactionDone(prevTxnId)
		j.progress.PrevTxnId = -1
	}

	// handle ddl binlog
	if featureIdempotentDDL && j.progress.UnknownCommitSeq == commitSeq {
		// Need to determine the state unknown binlog.
		if isCommitted, err := j.determineBinlogState(binlog); err != nil {
			return err
		} else if isCommitted {
			log.Infof("the binlog %s has been committed, commit seq %d", binlogType, commitSeq)
			return nil
		}

		// If the state unknown binlog is not committed, continue to handle the binlog.
	}

	if featureIdempotentDDL && utils.IsJobFailpointExpected(j.Name, "handle_binlog_idempotent:before", binlogType) {
		log.Warnf("fail to handle binlog by failpoint, binlog type: %s, binlog data: %s", binlogType, binlog.GetData())
		utils.RemoveJobFailpoint(j.Name, "handle_binlog_idempotent:before") // only work once
		return xerror.Errorf(xerror.Normal, "fail to handle binlog by failpoint handle_binlog_idempotent:before")
	}

	var err error
	if featureSeperatedHandles && IsJobHandleRegistered(binlogType) {
		err = HandleBinlog(j, binlog)
	} else {
		switch binlogType {
		case festruct.TBinlogType_ADD_PARTITION:
			err = j.handleAddPartition(binlog)
		case festruct.TBinlogType_CREATE_TABLE:
			err = j.handleCreateTable(binlog)
		case festruct.TBinlogType_DROP_PARTITION:
			err = j.handleDropPartition(binlog)
		case festruct.TBinlogType_DROP_TABLE:
			err = j.handleDropTable(binlog)
		case festruct.TBinlogType_ALTER_JOB:
			err = j.handleAlterJob(binlog)
		case festruct.TBinlogType_MODIFY_TABLE_ADD_OR_DROP_COLUMNS:
			err = j.handleLightningSchemaChange(binlog)
		case festruct.TBinlogType_RENAME_COLUMN:
			err = j.handleRenameColumn(binlog)
		case festruct.TBinlogType_MODIFY_COMMENT:
			err = j.handleModifyComment(binlog)
		case festruct.TBinlogType_DUMMY:
			err = j.handleDummy(binlog)
		case festruct.TBinlogType_ALTER_DATABASE_PROPERTY:
			log.Infof("handle alter database property binlog, ignore it, commit seq %d", commitSeq)
		case festruct.TBinlogType_MODIFY_TABLE_PROPERTY:
			err = j.handleModifyProperty(binlog)
		case festruct.TBinlogType_TRUNCATE_TABLE:
			err = j.handleTruncateTable(binlog)
		case festruct.TBinlogType_RENAME_TABLE:
			err = j.handleRenameTable(binlog)
		case festruct.TBinlogType_REPLACE_PARTITIONS:
			err = j.handleReplacePartitions(binlog)
		case festruct.TBinlogType_MODIFY_PARTITIONS:
			err = j.handleModifyPartitions(binlog)
		case festruct.TBinlogType_REPLACE_TABLE:
			err = j.handleReplaceTable(binlog)
		case festruct.TBinlogType_MODIFY_VIEW_DEF:
			err = j.handleAlterViewDef(binlog)
		case festruct.TBinlogType_MODIFY_TABLE_ADD_OR_DROP_INVERTED_INDICES:
			err = j.handleModifyTableAddOrDropInvertedIndices(binlog)
		case festruct.TBinlogType_INDEX_CHANGE_JOB:
			err = j.handleIndexChangeJob(binlog)
		case festruct.TBinlogType_RENAME_PARTITION:
			err = j.handleRenamePartition(binlog)
		case festruct.TBinlogType_RENAME_ROLLUP:
			err = j.handleRenameRollup(binlog)
		case festruct.TBinlogType_DROP_ROLLUP:
			err = j.handleDropRollup(binlog, false)
		case festruct.TBinlogType_RECOVER_INFO:
			err = j.handleRecoverInfo(binlog)
		default:
			return xerror.Errorf(xerror.Normal, "unknown binlog type: %v, commit seq %d, data %s",
				binlogType, commitSeq, binlog.GetData())
		}
	}

	if featureIdempotentDDL && err == nil && utils.IsJobFailpointExpected(
		j.Name, "handle_binlog_idempotent:after", binlogType) {
		log.Warnf("fail to handle binlog by failpoint, binlog type: %s, binlog data: %s", binlogType, binlog.GetData())
		utils.RemoveJobFailpoint(j.Name, "handle_binlog_idempotent:after") // only work once
		return xerror.Errorf(xerror.Normal, "fail to handle binlog by failpoint handle_binlog_idempotent:after")
	}

	return err
}

func (j *Job) recoverIncrementalSync() error {
	switch j.progress.SubSyncState.BinlogType {
	case BinlogUpsert:
		return j.handleUpsert(nil)
	default:
		j.progress.Rollback()
	}

	return nil
}

func (j *Job) incrementalSync() error {
	if featurePipelineCommit {
		return j.pipelineSync()
	} else {
		return j.incrementalSyncInternal()
	}
}

func (j *Job) maySkipBinlog() (bool, error) {
	// Force fullsync unconditionally
	if j.Extra.SkipBinlog && j.Extra.SkipBy == SkipByFullSync {
		info := fmt.Sprintf("the user required skipping the binlog, commit seq %d", j.progress.CommitSeq)
		log.Warnf("force full sync, because %s", info)
		return true, j.NewSnapshot(j.progress.CommitSeq, info)
	} else if j.Extra.SkipBinlog && j.Extra.SkipBy == SkipByPartialSync {
		log.Warnf("force partial sync, because the user required skipping the binlog, commit seq %d, table id %d, table %s",
			j.progress.CommitSeq, j.Extra.SkipTableId, j.Extra.SkipTable)
		if exists, err := j.IsSourceTableExists(j.Extra.SkipTableId, j.Extra.SkipTable); err != nil {
			return false, err
		} else if !exists {
			log.Warnf("the user required table %s (id %d) is not exists in source, ignore this skipping requirement",
				j.Extra.SkipTable, j.Extra.SkipTableId)
			j.Extra.SkipBinlog = false
		} else {
			replace, isView := true, false
			return true, j.NewPartialSnapshot(j.Extra.SkipTableId, j.Extra.SkipTable, nil, replace, isView)
		}
	}
	return false, nil
}

func (j *Job) incrementalSyncInternal() error {
	if !j.progress.IsDone() {
		log.Infof("job progress is not done, need recover. state: %s, prevCommitSeq: %d, commitSeq: %d",
			j.progress.SubSyncState, j.progress.PrevCommitSeq, j.progress.CommitSeq)

		return j.recoverIncrementalSync()
	}

	if exit, err := j.maySkipBinlog(); err != nil {
		return err
	} else if exit {
		return nil
	}

	// Step 1: get binlog
	log.Trace("start incremental sync")
	src := &j.Src
	srcRpc, err := j.factory.NewFeRpc(src)
	if err != nil {
		log.Errorf("new fe rpc failed, src: %v, err: %+v", src, err)
		return err
	}

	// Step 2: handle all binlog
	for !j.hasInterruptSignal() {
		// The CommitSeq is equals to PrevCommitSeq in here.
		commitSeq := j.progress.CommitSeq
		log.Tracef("src: %s, commitSeq: %d", src, commitSeq)

		getBinlogResp, err := srcRpc.GetBinlog(src, commitSeq, flagBinlogBatchSize)
		if err != nil {
			return err
		}
		log.Tracef("get binlog resp: %v", getBinlogResp)

		// Step 2.1: check binlog status
		status := getBinlogResp.GetStatus()
		switch status.StatusCode {
		case tstatus.TStatusCode_OK:
		case tstatus.TStatusCode_BINLOG_TOO_OLD_COMMIT_SEQ:
		case tstatus.TStatusCode_BINLOG_TOO_NEW_COMMIT_SEQ:
			// consume prev txn id for not to check and wait prev transaction finished
			if j.progress.PrevTxnId != -1 {
				log.Infof("consume prev txn id: %d", j.progress.PrevTxnId)
				j.Dest.WaitTransactionDone(j.progress.PrevTxnId)
				j.progress.PrevTxnId = -1
				j.progress.Persist()
			}
			return nil
		case tstatus.TStatusCode_BINLOG_DISABLE:
			return xerror.Errorf(xerror.Normal, "binlog is disabled")
		case tstatus.TStatusCode_BINLOG_NOT_FOUND_DB:
			return xerror.Errorf(xerror.Normal, "can't found db")
		case tstatus.TStatusCode_BINLOG_NOT_FOUND_TABLE:
			return xerror.Errorf(xerror.Normal, "can't found table")
		default:
			return xerror.Errorf(xerror.Normal, "invalid binlog status type: %v, msg: %s",
				status.StatusCode, utils.FirstOr(status.GetErrorMsgs(), ""))
		}

		// Step 2.2: handle binlogs records if has job
		binlogs := getBinlogResp.GetBinlogs()
		if len(binlogs) == 0 {
			return xerror.Errorf(xerror.Normal, "no binlog, but status code is: %v", status.StatusCode)
		}

		// Step 2.3: dispatch handle binlogs
		if err, backToRunLoop := j.handleBinlogs(binlogs); err != nil {
			return err
		} else if backToRunLoop {
			return nil
		}
	}
	return nil
}

func (j *Job) recoverJobProgress() error {
	// parse progress
	if progress, err := NewJobProgressFromJson(j.Name, j.db); err != nil {
		log.Errorf("parse job progress failed, job: %s, err: %+v", j.Name, err)
		return err
	} else {
		j.progress = progress
		return nil
	}
}

// tableSync is a function that synchronizes a table between the source and destination databases.
// If it is the first synchronization, it performs a full sync of the table.
// If it is not the first synchronization, it recovers the job progress and performs an incremental sync.
func (j *Job) tableSync() error {
	switch j.progress.SyncState {
	case TableFullSync:
		log.Trace("run table full sync")
		return j.fullSync()
	case TableIncrementalSync:
		log.Trace("run table incremental sync")
		return j.incrementalSync()
	case TablePartialSync:
		log.Trace("run table partial sync")
		return j.partialSync()
	default:
		return xerror.Errorf(xerror.Normal, "unknown sync state: %v", j.progress.SyncState)
	}
}

func (j *Job) dbTablesIncrementalSync() error {
	return j.incrementalSync()
}

func (j *Job) dbSync() error {
	switch j.progress.SyncState {
	case DBFullSync:
		log.Trace("run db full sync")
		return j.fullSync()
	case DBTablesIncrementalSync:
		log.Trace("run db tables incremental sync")
		return j.dbTablesIncrementalSync()
	case DBSpecificTableFullSync:
		log.Trace("run db specific table full sync")
		return nil
	case DBIncrementalSync:
		log.Trace("run db incremental sync")
		return j.incrementalSync()
	case DBPartialSync:
		log.Trace("run db partial sync")
		return j.partialSync()
	default:
		return xerror.Errorf(xerror.Normal, "unknown db sync state: %v", j.progress.SyncState)
	}
}

func (j *Job) sync() error {
	j.lock.Lock()
	defer j.lock.Unlock()

	// Update the skip state
	if j.Extra.SkipBinlog {
		committed := false
		switch j.Extra.SkipBy {
		case SkipBySilence:
			if j.Extra.SkipCommitSeq <= j.progress.PrevCommitSeq {
				// The binlog has been committed.
				committed = true
			}
		case SkipByFullSync:
			if j.progress.SyncState == DBFullSync || j.progress.SyncState == TableFullSync {
				// The fullsync has been triggered.
				committed = true
			}
		case SkipByPartialSync:
			if (j.progress.SyncState == DBPartialSync || j.progress.SyncState == TablePartialSync) &&
				j.progress.PartialSyncData.Table == j.Extra.SkipTable &&
				j.progress.PartialSyncData.TableId == j.Extra.SkipTableId &&
				j.progress.PartialSyncData.Partitions == nil {
				// The partial sync has been triggered.
				committed = true
			}
		}
		if committed {
			j.Extra.SkipBinlog = false
			log.Infof("reset skip binlog, skip by: %s, skip commit seq: %d, prev commit seq: %d, skip table: %s, skip table id: %d",
				j.Extra.SkipBy, j.Extra.SkipCommitSeq, j.progress.PrevCommitSeq, j.Extra.SkipTable, j.Extra.SkipTableId)
		}
	}

	j.updateJobStatus()
	switch j.SyncType {
	case TableSync:
		return j.tableSync()
	case DBSync:
		return j.dbSync()
	default:
		return xerror.Errorf(xerror.Normal, "unknown table sync type: %v", j.SyncType)
	}
}

// if err is Panic, return it
func (j *Job) handleError(jobName string, err error) error {
	var xerr *xerror.XError
	if !errors.As(err, &xerr) {
		log.Errorf("convert error to xerror failed, err: %+v", err)
		return nil
	}

	xmetrics.RecordError(jobName, xerr)
	if xerr.IsPanic() {
		log.Errorf("job panic, job: %s, err: %+v", j.Name, err)
		return err
	}

	if xerr.Category() == xerror.Meta {
		info := fmt.Sprintf("receive meta category error, make new snapshot, job: %s, err: %v", j.Name, err)
		log.Warnf("%s", info)
		_ = j.NewSnapshot(j.progress.CommitSeq, info)
	}
	return nil
}

func (j *Job) run() {
	ticker := time.NewTicker(SyncDuration)
	defer ticker.Stop()

	var panicError error

	for {
		// do maybeDeleted first to avoid mark job deleted after job stopped & before job run & close stop chan gap in Delete, so job will not run
		if j.maybeDeleted() {
			return
		}

		// Wait for the interrupt channel to be empty
		j.consumeInterruptSignals()

		select {
		case <-j.stop:
			log.Infof("job stopped, job: %s", j.Name)
			return

		case <-ticker.C:
			// loop to print error, not panic, waiting for user to pause/stop/remove Job
			if j.getJobState() != JobRunning {
				break
			}

			if panicError != nil {
				log.Errorf("job panic, job: %s, err: %+v", j.Name, panicError)
				break
			}

			err := j.sync()
			if err == nil {
				break
			}

			log.Warnf("job sync failed, job: %s, err: %+v", j.Name, err)
			panicError = j.handleError(j.Name, err)
		}
	}
}

func (j *Job) NewSnapshot(commitSeq int64, fullSyncInfo string) error {
	log.Infof("new snapshot, commitSeq: %d, prevCommitSeq: %d, prevSyncState: %s, prevSubSyncState: %s",
		commitSeq, j.progress.PrevCommitSeq, j.progress.SyncState, j.progress.SubSyncState)

	if fullSyncInfo != "" {
		j.progress.SetFullSyncInfo(fullSyncInfo)
	}

	j.progress.PartialSyncData = nil
	j.progress.TableAliases = nil
	j.progress.SyncId += 1
	switch j.SyncType {
	case TableSync:
		j.progress.NextWithPersist(commitSeq, TableFullSync, BeginCreateSnapshot, "")
		return nil
	case DBSync:
		j.progress.NextWithPersist(commitSeq, DBFullSync, BeginCreateSnapshot, "")
		return nil
	default:
		err := xerror.Panicf(xerror.Normal, "unknown table sync type: %v", j.SyncType)
		log.Fatalf("new snapshot: %+v", err)
		return err
	}
}

// New partial snapshot, with the source cluster table name and the partitions to sync.
// A empty partitions means to sync the whole table.
//
// If the replace is true, the restore task will load data into a new table and replaces the old
// one when restore finished. So replace requires whole table partial sync.
func (j *Job) NewPartialSnapshot(tableId int64, table string, partitions []string, replace, isView bool) error {
	if !isView && j.SyncType == TableSync && table != j.Src.Table {
		return xerror.Errorf(xerror.Normal,
			"partial sync table name is not equals to the source name %s, table: %s, sync type: table", j.Src.Table, table)
	}

	if replace && len(partitions) != 0 {
		return xerror.Errorf(xerror.Normal,
			"partial sync with replace but partitions is not empty, table: %s, len: %d", table, len(partitions))
	}

	// The binlog of commitSeq will be skipped once the partial snapshot finished.
	commitSeq := j.progress.CommitSeq

	syncData := &JobPartialSyncData{
		TableId:    tableId,
		Table:      table,
		IsView:     isView,
		Partitions: partitions,
	}
	j.progress.PartialSyncData = syncData
	j.progress.TableAliases = nil
	j.progress.SyncId += 1
	if replace {
		alias := TableAlias(table)
		j.progress.TableAliases = make(map[string]string)
		j.progress.TableAliases[table] = alias
		log.Infof("new partial snapshot, commitSeq: %d, table id: %d, table: %s, alias: %s, isView: %t",
			commitSeq, tableId, table, alias, isView)
	} else {
		log.Infof("new partial snapshot, commitSeq: %d, table id: %d, table: %s, partitions: %v, isView: %t",
			commitSeq, tableId, table, partitions, isView)
	}

	j.Extra.PartialSnapshotParams = nil
	switch j.SyncType {
	case TableSync:
		j.progress.NextWithPersist(commitSeq, TablePartialSync, BeginCreateSnapshot, "")
		return nil
	case DBSync:
		j.progress.NextWithPersist(commitSeq, DBPartialSync, BeginCreateSnapshot, "")
		return nil
	default:
		err := xerror.Panicf(xerror.Normal, "unknown table sync type: %v", j.SyncType)
		log.Fatalf("run %+v", err)
		return err
	}
}

// run job
func (j *Job) Run() error {
	gls.ResetGls(gls.GoID(), map[any]any{"job": j.Name})
	defer gls.DeleteGls(gls.GoID())

	// retry 3 times to check IsProgressExist
	var isProgressExist bool
	var err error
	for i := 0; i < 3; i++ {
		isProgressExist, err = j.db.IsProgressExist(j.Name)
		if err == nil {
			break
		}
		log.Errorf("check progress exist failed, error: %+v", err)
	}
	if err != nil {
		return err
	}

	if isProgressExist {
		if err := j.recoverJobProgress(); err != nil {
			log.Errorf("recover job %s progress failed: %+v", j.Name, err)
			return err
		}
	} else {
		j.progress = NewJobProgress(j.Name, j.SyncType, j.db)
		info := fmt.Sprintf("new job, job: %s, sync type: %v", j.Name, j.SyncType)
		if err := j.NewSnapshot(0, info); err != nil {
			return err
		}
	}

	// Hack: for drop table
	if j.SyncType == DBSync {
		j.srcMeta.ClearTablesCache()
		j.destMeta.ClearTablesCache()
	}

	j.run()
	return nil
}

func (j *Job) desyncTable() error {
	log.Debugf("desync table")

	tableName, err := j.destMeta.GetTableNameById(j.Dest.TableId)
	if err != nil {
		return err
	}
	return j.IDest.DesyncTables(tableName)
}

func (j *Job) desyncDB() error {
	log.Debugf("desync db")

	tables, err := j.destMeta.GetTables()
	if err != nil {
		return err
	}

	tableNames := []string{}
	for _, tableMeta := range tables {
		tableNames = append(tableNames, tableMeta.Name)
	}

	return j.IDest.DesyncTables(tableNames...)
}

func (j *Job) Sync() error {
	defer j.raiseInterruptSignal()()
	j.lock.Lock()
	defer j.lock.Unlock()

	if j.SyncType == DBSync {
		return j.syncDB()
	} else {
		return j.syncTable()
	}
}

func (j *Job) syncTable() error {
	log.Debugf("sync table")

	tableName, err := j.destMeta.GetTableNameById(j.Dest.TableId)
	if err != nil {
		return err
	}

	return j.IDest.SyncTables(tableName)
}

func (j *Job) syncDB() error {
	log.Debugf("sync db")

	tables, err := j.destMeta.GetTables()
	if err != nil {
		return err
	}

	tableNames := []string{}
	for _, tableMeta := range tables {
		tableNames = append(tableNames, tableMeta.Name)
	}

	return j.IDest.SyncTables(tableNames...)
}

func (j *Job) Desync() error {
	defer j.raiseInterruptSignal()()
	j.lock.Lock()
	defer j.lock.Unlock()

	if j.SyncType == DBSync {
		return j.desyncDB()
	} else {
		return j.desyncTable()
	}
}

// check show create table contain some string
func (j *Job) CheckCreateTable(tableName, expectedStr string) (bool, error) {
	db, err := j.Dest.Connect()
	if err != nil {
		return false, err
	}

	dbName := utils.FormatKeywordName(j.Dest.Database)
	tableName = utils.FormatKeywordName(tableName)
	query := fmt.Sprintf("SHOW CREATE TABLE %s.%s", dbName, tableName)
	log.Infof("show create table sql: %s", query)
	rows, err := db.Query(query)
	if err != nil {
		return false, xerror.Wrapf(err, xerror.Normal, "show create table %s", tableName)
	}
	defer rows.Close()
	rowParser := utils.NewRowParser()
	if err := rowParser.Parse(rows); err != nil {
		return false, xerror.Wrapf(err, xerror.Normal, "parse show create table %s rows", tableName)
	}
	createSql, err := rowParser.GetString("Create Table")
	if err != nil {
		return false, xerror.Wrapf(err, xerror.Normal, query)
	}
	return strings.Contains(createSql, expectedStr), nil
}

// stop job
func (j *Job) Stop() {
	close(j.stop)
}

// delete job
func (j *Job) Delete() {
	j.isDeleted.Store(true)
	close(j.stop)
}

func (j *Job) maybeDeleted() bool {
	if !j.isDeleted.Load() {
		return false
	}

	// job had been deleted
	log.Infof("job deleted, job: %s, remove in db", j.Name)
	if err := j.db.RemoveJob(j.Name); err != nil {
		log.Errorf("remove job failed, job: %s, err: %+v", j.Name, err)
	}
	return true
}

func (j *Job) updateFrontends() error {
	if frontends, err := j.srcMeta.GetFrontends(); err != nil {
		log.Warnf("get src frontends failed, fe: %+v", j.Src)
		return err
	} else {
		for _, frontend := range frontends {
			j.Src.Frontends = append(j.Src.Frontends, *frontend)
		}
	}
	log.Debugf("src frontends %+v", j.Src.Frontends)

	if frontends, err := j.destMeta.GetFrontends(); err != nil {
		log.Warnf("get dest frontends failed, fe: %+v", j.Dest)
		return err
	} else {
		for _, frontend := range frontends {
			j.Dest.Frontends = append(j.Dest.Frontends, *frontend)
		}
	}
	log.Debugf("dest frontends %+v", j.Dest.Frontends)

	return nil
}

func (j *Job) FirstRun() error {
	log.Infof("first run check job, name: %s, src: %s, dest: %s", j.Name, &j.Src, &j.Dest)

	// Step 0: get all frontends
	if err := j.updateFrontends(); err != nil {
		return err
	}

	// Step 1: check fe and be binlog feature is enabled
	if err := j.srcMeta.CheckBinlogFeature(); err != nil {
		return err
	}
	if err := j.destMeta.CheckBinlogFeature(); err != nil {
		return err
	}

	// Step 2: check src database
	if src_db_exists, err := j.ISrc.CheckDatabaseExists(); err != nil {
		return err
	} else if !src_db_exists {
		return xerror.Errorf(xerror.Normal, "src database %s not exists", j.Src.Database)
	}
	if j.SyncType == DBSync {
		if enable, err := j.ISrc.IsDatabaseEnableBinlog(); err != nil {
			return err
		} else if !enable {
			return xerror.Errorf(xerror.Normal, "src database %s not enable binlog", j.Src.Database)
		}
	}
	if srcDbId, err := j.srcMeta.GetDbId(); err != nil {
		return err
	} else {
		j.Src.DbId = srcDbId
	}

	// Step 3: check src table exists, if not exists, return err
	if j.SyncType == TableSync {
		if src_table_exists, err := j.ISrc.CheckTableExists(); err != nil {
			return err
		} else if !src_table_exists {
			return xerror.Errorf(xerror.Normal, "src table %s.%s not exists", j.Src.Database, j.Src.Table)
		}

		if invalidProperty, err := j.ISrc.CheckTablePropertyValid(); err != nil {
			return err
		} else if len(invalidProperty) != 0 {
			return xerror.Errorf(xerror.Normal, "src table %s.%s only support property: %s", j.Src.Database, j.Src.Table, strings.Join(invalidProperty, ", "))
		}

		if srcTableId, err := j.srcMeta.GetTableId(j.Src.Table); err != nil {
			return err
		} else {
			j.Src.TableId = srcTableId
		}
	}

	// Step 4: check dest database && table exists
	// if dest database && table exists, return err
	dest_db_exists, err := j.IDest.CheckDatabaseExists()
	if err != nil {
		return err
	}
	if !dest_db_exists {
		if err := j.IDest.CreateDatabase(); err != nil {
			return err
		}
	}
	if destDbId, err := j.destMeta.GetDbId(); err != nil {
		return err
	} else {
		j.Dest.DbId = destDbId
	}
	if j.SyncType == TableSync && !j.Extra.allowTableExists {
		dest_table_exists, err := j.IDest.CheckTableExists()
		if err != nil {
			return err
		}
		if dest_table_exists {
			return xerror.Errorf(xerror.Normal, "dest table %s.%s already exists", j.Dest.Database, j.Dest.Table)
		}
	}

	return nil
}

func (j *Job) getJobState() JobState {
	j.lock.Lock()
	defer j.lock.Unlock()

	return j.State
}

func (j *Job) changeJobState(state JobState) error {
	defer j.raiseInterruptSignal()()
	j.lock.Lock()
	defer j.lock.Unlock()

	if j.State == state {
		log.Debugf("job %s state is already %s", j.Name, state)
		return nil
	}

	originState := j.State
	j.State = state
	if err := j.persistJob(); err != nil {
		j.State = originState
		return err
	}
	log.Debugf("change job %s state from %s to %s", j.Name, originState, state)
	return nil
}

func (j *Job) Pause() error {
	log.Infof("pause job %s", j.Name)

	return j.changeJobState(JobPaused)
}

func (j *Job) Resume() error {
	log.Infof("resume job %s", j.Name)

	return j.changeJobState(JobRunning)
}

type RawJobStatus struct {
	state         int32
	progressState int32
	commitSeq     int64
}

func (j *Job) updateJobStatus() {
	atomic.StoreInt64(&j.rawStatus.commitSeq, j.progress.PrevCommitSeq)
	atomic.StoreInt32(&j.rawStatus.state, int32(j.State))
	if j.progress != nil {
		atomic.StoreInt32(&j.rawStatus.progressState, int32(j.progress.SyncState))
	}
}

type JobStatus struct {
	Name          string `json:"name"`
	State         string `json:"state"`
	ProgressState string `json:"progress_state"`
	CommitSeq     int64  `json:"commit_seq"`
}

func (j *Job) Status() *JobStatus {
	state := JobState(atomic.LoadInt32(&j.rawStatus.state)).String()
	progressState := SyncState(atomic.LoadInt32(&j.rawStatus.progressState)).String()
	commitSeq := atomic.LoadInt64(&j.rawStatus.commitSeq)

	return &JobStatus{
		Name:          j.Name,
		State:         state,
		ProgressState: progressState,
		CommitSeq:     commitSeq,
	}
}

func (j *Job) UpdateHostMapping(srcHostMaps, destHostMaps map[string]string) error {
	defer j.raiseInterruptSignal()()
	j.lock.Lock()
	defer j.lock.Unlock()

	oldSrcHostMapping := j.Src.HostMapping
	if j.Src.HostMapping == nil {
		j.Src.HostMapping = make(map[string]string)
	}
	for private, public := range srcHostMaps {
		if public == "" {
			delete(j.Src.HostMapping, private)
		} else {
			j.Src.HostMapping[private] = public
		}
	}

	oldDestHostMapping := j.Dest.HostMapping
	if j.Dest.HostMapping == nil {
		j.Dest.HostMapping = make(map[string]string)
	}
	for private, public := range destHostMaps {
		if public == "" {
			delete(j.Dest.HostMapping, private)
		} else {
			j.Dest.HostMapping[private] = public
		}
	}

	if err := j.persistJob(); err != nil {
		j.Src.HostMapping = oldSrcHostMapping
		j.Dest.HostMapping = oldDestHostMapping
		return err
	}

	log.Debugf("update job %s src host mapping %+v, dest host mapping: %+v", j.Name, srcHostMaps, destHostMaps)
	return nil
}

type SkipBinlogParams struct {
	// The skip method, supports: ["fullsync", "silence", "partialsync"]
	SkipBy string
	// The commit seq to skip
	SkipCommitSeq int64
	// The skip table, required for partial sync
	SkipTable string
	// The skip table id, required for partial sync
	SkipTableId int64
}

func (j *Job) SkipBinlog(params SkipBinlogParams) error {
	if params.SkipBy == SkipByPartialSync {
		if params.SkipTable == "" || params.SkipTableId == 0 {
			return xerror.Errorf(xerror.Normal, "invalid skip table: %s, table id: %d", params.SkipTable, params.SkipTableId)
		}
	} else if params.SkipBy == SkipBySilence {
		if params.SkipCommitSeq <= 0 {
			return xerror.Errorf(xerror.Normal, "invalid skip commit seq: %d", params.SkipCommitSeq)
		}
	} else if params.SkipBy != SkipByFullSync {
		return xerror.Errorf(xerror.Normal, "invalid skip method: %s", params.SkipBy)
	}

	defer j.raiseInterruptSignal()()
	j.lock.Lock()
	defer j.lock.Unlock()

	j.Extra.SkipBinlog = true
	j.Extra.SkipCommitSeq = params.SkipCommitSeq
	j.Extra.SkipBy = params.SkipBy
	j.Extra.SkipTable = params.SkipTable
	j.Extra.SkipTableId = params.SkipTableId

	log.Infof("skip binlog by %s, commit seq %d, skip table %s, skip table id %d, job %s",
		params.SkipBy, params.SkipCommitSeq, params.SkipTable, params.SkipTableId, j.Name)
	return nil
}

func (j *Job) GetSpecifiedBinlog(commitSeq int64) (*festruct.TBinlog, error) {
	if binlog, ok := j.Extra.CachedBinlogs[commitSeq]; ok {
		return binlog, nil
	}

	// Get binlog from src
	src := &j.Src
	srcRpc, err := j.factory.NewFeRpc(src)
	if err != nil {
		log.Errorf("new fe rpc failed, src: %v, err: %+v", src, err)
		return nil, err
	}

	prevCommitSeq := commitSeq - 1
	getBinlogResp, err := srcRpc.GetBinlog(src, prevCommitSeq, 1)
	if err != nil {
		log.Errorf("get specified binlog %d failed, src: %v, err: %+v", commitSeq, src, err)
		return nil, err
	}

	log.Tracef("get specified binlog %d resp: %v", commitSeq, getBinlogResp)

	status := getBinlogResp.GetStatus()
	switch status.StatusCode {
	case tstatus.TStatusCode_OK:
	case tstatus.TStatusCode_BINLOG_TOO_OLD_COMMIT_SEQ:
	case tstatus.TStatusCode_BINLOG_TOO_NEW_COMMIT_SEQ:
		return nil, xerror.Errorf(xerror.Normal, "binlog is too new, commit seq: %d", commitSeq)
	case tstatus.TStatusCode_BINLOG_DISABLE:
		return nil, xerror.Errorf(xerror.Normal, "binlog is disabled, commit seq: %d", commitSeq)
	case tstatus.TStatusCode_BINLOG_NOT_FOUND_DB:
		return nil, xerror.Errorf(xerror.Normal, "can't found db, commit seq: %d", commitSeq)
	case tstatus.TStatusCode_BINLOG_NOT_FOUND_TABLE:
		return nil, xerror.Errorf(xerror.Normal, "can't found table, commit seq: %d", commitSeq)
	default:
		return nil, xerror.Errorf(xerror.Normal, "invalid binlog status type: %v, msg: %s",
			status.StatusCode, utils.FirstOr(status.GetErrorMsgs(), ""))
	}

	binlogs := getBinlogResp.GetBinlogs()
	if len(binlogs) == 0 {
		return nil, xerror.Errorf(xerror.Normal, "no binlog, but status code is: %v", status.StatusCode)
	}
	if len(binlogs) > 1 {
		return nil, xerror.Errorf(xerror.Normal, "more than one binlog, but status code is: %v", status.StatusCode)
	}

	binlog := binlogs[0]
	if binlog.GetCommitSeq() != commitSeq {
		return nil, xerror.Errorf(xerror.Normal, "binlog commit seq is not equal to %d, specified commit seq %d, commit seq: %d",
			commitSeq, commitSeq, binlog.GetCommitSeq())
	}

	if j.Extra.CachedBinlogs == nil {
		j.Extra.CachedBinlogs = make(map[int64]*festruct.TBinlog)
	}
	j.Extra.CachedBinlogs[commitSeq] = binlog
	return binlog, nil
}

func (j *Job) lockBinlog(lockCommitSeq int64) error {
	if lockCommitSeq != LockLastBinlogCommitSeq && j.progress.LockedCommitSeq+256 > lockCommitSeq {
		// Avoid frequent lock binlog
		return nil
	}

	var tableId int64
	switch j.SyncType {
	case TableSync:
		tableId = j.Src.TableId
	case DBSync:
		tableId = -1
	default:
		return xerror.Errorf(xerror.Normal, "unknown table sync type: %v", j.SyncType)
	}

	src := &j.Src
	srcRpc, err := j.factory.NewFeRpc(src)
	if err != nil {
		log.Errorf("new fe rpc failed, src: %v, err: %+v", src, err)
		return err
	}

	jobUniqueId := fmt.Sprintf("%s_%s", j.Name, j.Id)
	resp, err := srcRpc.LockBinlog(src, jobUniqueId, tableId, lockCommitSeq)
	if err != nil && rpc.IsUnknownMethod(err) {
		// Keep compatibility with old version
		log.Debugf("lock binlog failed, because of unknown method, src: %v", src)
		j.progress.LockedCommitSeq = lockCommitSeq
		return nil
	} else if err != nil {
		log.Errorf("lock binlog failed, src: %v, err: %+v", src, err)
		return err
	} else if status := resp.GetStatus().GetStatusCode(); status != tstatus.TStatusCode_OK &&
		status != tstatus.TStatusCode_BINLOG_NOT_FOUND_DB &&
		status != tstatus.TStatusCode_BINLOG_NOT_FOUND_TABLE {
		log.Errorf("lock binlog failed, src: %v, status: %+v", src, resp.GetStatus())
		return xerror.Errorf(xerror.RPC, "lock binlog failed, status: %+v", resp.GetStatus())
	} else if status != tstatus.TStatusCode_OK {
		log.Warnf("lock binlog failed, src: %v, status: %+v", src, resp.GetStatus())
		return nil
	}

	lockedCommitSeq := resp.GetLockedCommitSeq()
	log.Debugf("lock binlog success, commit seq: %d, locked commit seq: %d", lockCommitSeq, lockedCommitSeq)

	j.progress.LockedCommitSeq = lockedCommitSeq
	return nil
}

func (j *Job) IsSourceTableExists(tableId int64, tableName string) (bool, error) {
	if tableId == 0 || tableName == "" {
		return false, xerror.Errorf(xerror.Normal, "table id or table name is empty")
	}

	if j.SyncType == TableSync {
		return j.Src.TableId == tableId && j.Src.Table == tableName, nil
	}

	table, err := j.srcMeta.UpdateTable("", tableId)
	if err != nil && xerror.IsCategory(err, xerror.Meta) { // table not found
		return false, nil
	} else if err != nil {
		return false, err
	} else {
		return table.Name == tableName, nil
	}
}

func (j *Job) GetJobProgress() *JobProgress {
	return j.progress
}

func (j *Job) GetSrcMeta() Metaer {
	return j.srcMeta
}

func (j *Job) GetDestMeta() Metaer {
	return j.destMeta
}

func (j *Job) raiseInterruptSignal() func() {
	atomic.AddInt32(&j.Extra.InterruptSignal, 1)
	return func() {
		j.Extra.InterruptCh <- struct{}{}
	}
}

func (j *Job) hasInterruptSignal() bool {
	return atomic.LoadInt32(&j.Extra.InterruptSignal) > 0
}

func (j *Job) consumeInterruptSignals() {
	for j.hasInterruptSignal() {
		<-j.Extra.InterruptCh
		atomic.AddInt32(&j.Extra.InterruptSignal, -1)
	}
}

func isTxnCommitted(status *tstatus.TStatus) bool {
	return isStatusContainsAny(status, "is already COMMITTED")
}

func isTxnNotFound(status *tstatus.TStatus) bool {
	errMessages := status.GetErrorMsgs()
	for _, errMessage := range errMessages {
		// detailMessage = transaction not found
		// or detailMessage = transaction [12356] not found
		if strings.Contains(errMessage, "transaction not found") || regexp.MustCompile(`transaction \[\d+\] not found`).MatchString(errMessage) {
			return true
		}
	}
	return false
}

func isTxnAborted(status *tstatus.TStatus) bool {
	return isStatusContainsAny(status, "is already aborted")
}

func isTableNotFound(status *tstatus.TStatus) bool {
	// 1. FE FrontendServiceImpl.beginTxnImpl
	// 2. FE FrontendServiceImpl.commitTxnImpl
	// 3. FE Table.tryWriteLockOrMetaException
	return isStatusContainsAny(status, "can't find table id:", "table not found", "unknown table")
}

func isStatusContainsAny(status *tstatus.TStatus, patterns ...string) bool {
	errMessages := status.GetErrorMsgs()
	for _, errMessage := range errMessages {
		for _, substr := range patterns {
			if strings.Contains(errMessage, substr) {
				return true
			}
		}
	}
	return false
}

func IsSessionVariableRequired(msg string) bool {
	re := regexp.MustCompile(`set enable_.+=.+|Incorrect column name .* Column regex is`)
	return re.MatchString(msg)
}

func FilterStorageMediumFromCreateTableSql(createSql string) string {
	pattern := `"storage_medium"\s*=\s*"[^"]*"(,\s*)?`
	createSql = regexp.MustCompile(pattern).ReplaceAllString(createSql, "")
	return FilterTailingCommaFromCreateTableSql(createSql)
}

func FilterDynamicPartitionStoragePolicyFromCreateTableSql(createSql string) string {
	// Two patterns:
	// - "dynamic_partition.storage_policy"="storage_policy",
	// - , "dynamic_partition.storage_policy"="storage_policy"
	pattern := `"dynamic_partition.storage_policy"\s*=\s*"[^"]*"(,\s*)?`
	createSql = regexp.MustCompile(pattern).ReplaceAllString(createSql, "")
	return FilterTailingCommaFromCreateTableSql(createSql)
}

func FilterTailingCommaFromCreateTableSql(createSql string) string {
	pattern := `,(\s*)?\)\s*$`
	return regexp.MustCompile(pattern).ReplaceAllString(createSql, ")")
}

func getJobId(name string, src base.Spec, dest base.Spec) string {
	h := md5.New()
	io.WriteString(h, name)
	io.WriteString(h, src.String())
	io.WriteString(h, dest.String())
	return fmt.Sprintf("%x", h.Sum(nil))
}
