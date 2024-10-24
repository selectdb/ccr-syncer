package ccr

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
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
	SYNC_DURATION = time.Second * 3
)

var (
	featureSchemaChangePartialSync        bool
	featureCleanTableAndPartitions        bool
	featureAtomicRestore                  bool
	featureCreateViewDropExists           bool
	featureReplaceNotMatchedWithAlias     bool
	featureFilterShadowIndexesUpsert      bool
	featureCancelConflictBackupRestoreJob bool
)

func init() {
	flag.BoolVar(&featureSchemaChangePartialSync, "feature_schema_change_partial_sync", true,
		"use partial sync when working with schema change")

	// The default value is false, since clean tables will erase views unexpectedly.
	flag.BoolVar(&featureCleanTableAndPartitions, "feature_clean_table_and_partitions", false,
		"clean non restored tables and partitions during fullsync")
	flag.BoolVar(&featureAtomicRestore, "feature_atomic_restore", true,
		"replace tables in atomic during fullsync (otherwise the dest table will not be able to read).")
	flag.BoolVar(&featureCreateViewDropExists, "feature_create_view_drop_exists", true,
		"drop the exists view if exists, when sync the creating view binlog")
	flag.BoolVar(&featureReplaceNotMatchedWithAlias, "feature_replace_not_matched_with_alias", true,
		"replace signature not matched tables with table alias during the full sync")
	flag.BoolVar(&featureFilterShadowIndexesUpsert, "feature_filter_shadow_indexes_upsert", true,
		"filter the upsert to the shadow indexes")
	flag.BoolVar(&featureCancelConflictBackupRestoreJob, "feature_cancel_conflict_backup_restore_job", false,
		"cancel the conflict backup/restore job before issue new backup/restore job")
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

type Job struct {
	SyncType  SyncType    `json:"sync_type"`
	Name      string      `json:"name"`
	Src       base.Spec   `json:"src"`
	ISrc      base.Specer `json:"-"`
	srcMeta   Metaer      `json:"-"`
	Dest      base.Spec   `json:"dest"`
	IDest     base.Specer `json:"-"`
	destMeta  Metaer      `json:"-"`
	SkipError bool        `json:"skip_error"`
	State     JobState    `json:"state"`

	factory *Factory `json:"-"`

	allowTableExists bool `json:"-"` // Only for FirstRun(), don't need to persist.
	forceFullsync    bool `json:"-"` // Force job step fullsync, for test only.

	progress   *JobProgress `json:"-"`
	db         storage.DB   `json:"-"`
	jobFactory *JobFactory  `json:"-"`
	rawStatus  RawJobStatus `json:"-"`

	stop      chan struct{} `json:"-"`
	isDeleted atomic.Bool   `json:"-"`

	concurrencyManager *rpc.ConcurrencyManager `json:"-"`

	lock sync.Mutex `json:"-"`
}

type jobContext struct {
	context.Context
	src              base.Spec
	dest             base.Spec
	db               storage.DB
	skipError        bool
	allowTableExists bool
	factory          *Factory
}

func NewJobContext(src, dest base.Spec, skipError bool, allowTableExists bool, db storage.DB, factory *Factory) *jobContext {
	return &jobContext{
		Context:          context.Background(),
		src:              src,
		dest:             dest,
		skipError:        skipError,
		allowTableExists: allowTableExists,
		db:               db,
		factory:          factory,
	}
}

// new job
func NewJobFromService(name string, ctx context.Context) (*Job, error) {
	jobContext, ok := ctx.(*jobContext)
	if !ok {
		return nil, xerror.Errorf(xerror.Normal, "invalid context type: %T", ctx)
	}

	factory := jobContext.factory
	src := jobContext.src
	dest := jobContext.dest
	job := &Job{
		Name:      name,
		Src:       src,
		ISrc:      factory.NewSpecer(&src),
		srcMeta:   factory.NewMeta(&jobContext.src),
		Dest:      dest,
		IDest:     factory.NewSpecer(&dest),
		destMeta:  factory.NewMeta(&jobContext.dest),
		SkipError: jobContext.skipError,
		State:     JobRunning,

		allowTableExists: jobContext.allowTableExists,
		factory:          factory,
		forceFullsync:    false,

		progress: nil,
		db:       jobContext.db,
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

func (j *Job) RecoverDatabaseSync() error {
	return nil
}

// database old data sync
func (j *Job) DatabaseOldDataSync() error {
	// Step 1: drop all tables
	err := j.IDest.ClearDB()
	if err != nil {
		return err
	}

	// Step 2: make snapshot

	return nil
}

// database sync
func (j *Job) DatabaseSync() error {
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

	log.Debugf("found backends: %v", backends)

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

func (j *Job) isTableSyncWithAlias() bool {
	return j.SyncType == TableSync && j.Src.Table != j.Dest.Table
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
	log.Debugf("extraInfo: %v", extraInfo)
	jobInfoMap["extra_info"] = extraInfo

	jobInfoBytes, err := json.Marshal(jobInfoMap)
	if err != nil {
		return nil, xerror.Errorf(xerror.Normal, "marshal jobInfo failed, jobInfo: %v", jobInfoMap)
	}

	return jobInfoBytes, nil
}

// Like fullSync, but only backup and restore partial of the partitions of a table.
func (j *Job) partialSync() error {
	type inMemoryData struct {
		SnapshotName      string                        `json:"snapshot_name"`
		SnapshotResp      *festruct.TGetSnapshotResult_ `json:"snapshot_resp"`
		TableCommitSeqMap map[int64]int64               `json:"table_commit_seq_map"`
	}

	if j.progress.PartialSyncData == nil {
		return xerror.Errorf(xerror.Normal, "run partial sync but data is nil")
	}

	table := j.progress.PartialSyncData.Table
	partitions := j.progress.PartialSyncData.Partitions
	switch j.progress.SubSyncState {
	case Done:
		log.Infof("partial sync status: done")
		withAlias := len(j.progress.TableAliases) > 0
		if err := j.newPartialSnapshot(table, partitions, withAlias); err != nil {
			return err
		}

	case BeginCreateSnapshot:
		// Step 1: Create snapshot
		log.Infof("partial sync status: create snapshot")
		if featureCancelConflictBackupRestoreJob {
			if err := j.ISrc.CancelBackupIfExists(); err != nil {
				return err
			}
		}

		snapshotName, err := j.ISrc.CreatePartialSnapshotAndWaitForDone(table, partitions)
		if err != nil {
			return err
		}

		j.progress.NextSubCheckpoint(GetSnapshotInfo, snapshotName)

	case GetSnapshotInfo:
		// Step 2: Get snapshot info
		log.Infof("partial sync status: get snapshot info")

		snapshotName := j.progress.PersistData
		src := &j.Src
		srcRpc, err := j.factory.NewFeRpc(src)
		if err != nil {
			return err
		}

		log.Debugf("partial sync begin get snapshot %s", snapshotName)
		snapshotResp, err := srcRpc.GetSnapshot(src, snapshotName)
		if err != nil {
			return err
		}

		if snapshotResp.Status.GetStatusCode() != tstatus.TStatusCode_OK {
			err = xerror.Errorf(xerror.FE, "get snapshot failed, status: %v", snapshotResp.Status)
			return err
		}

		if !snapshotResp.IsSetJobInfo() {
			return xerror.New(xerror.Normal, "jobInfo is not set")
		}

		log.Tracef("job: %.128s", snapshotResp.GetJobInfo())
		if !snapshotResp.IsSetJobInfo() {
			return xerror.New(xerror.Normal, "jobInfo is not set")
		}

		tableCommitSeqMap, err := ExtractTableCommitSeqMap(snapshotResp.GetJobInfo())
		if err != nil {
			return err
		}

		log.Debugf("table commit seq map: %v", tableCommitSeqMap)
		if j.SyncType == TableSync {
			if _, ok := tableCommitSeqMap[j.Src.TableId]; !ok {
				return xerror.Errorf(xerror.Normal, "table id %d, commit seq not found", j.Src.TableId)
			}
		}

		inMemoryData := &inMemoryData{
			SnapshotName:      snapshotName,
			SnapshotResp:      snapshotResp,
			TableCommitSeqMap: tableCommitSeqMap,
		}
		j.progress.NextSubVolatile(AddExtraInfo, inMemoryData)

	case AddExtraInfo:
		// Step 3: Add extra info
		log.Infof("partial sync status: add extra info")

		inMemoryData := j.progress.InMemoryData.(*inMemoryData)
		snapshotResp := inMemoryData.SnapshotResp
		jobInfo := snapshotResp.GetJobInfo()

		log.Infof("partial sync snapshot response meta size: %d, job info size: %d",
			len(snapshotResp.Meta), len(snapshotResp.JobInfo))

		jobInfoBytes, err := j.addExtraInfo(jobInfo)
		if err != nil {
			return err
		}

		log.Debugf("partial sync job info size: %d, bytes: %.128s", len(jobInfoBytes), string(jobInfoBytes))
		snapshotResp.SetJobInfo(jobInfoBytes)

		// save the entire commit seq map, this value will be used in PersistRestoreInfo.
		if len(j.progress.TableCommitSeqMap) == 0 {
			j.progress.TableCommitSeqMap = make(map[int64]int64)
		}
		for tableId, commitSeq := range inMemoryData.TableCommitSeqMap {
			j.progress.TableCommitSeqMap[tableId] = commitSeq
		}
		j.progress.NextSubCheckpoint(RestoreSnapshot, inMemoryData)

	case RestoreSnapshot:
		// Step 4: Restore snapshot
		log.Infof("partial sync status: restore snapshot")

		if j.progress.InMemoryData == nil {
			persistData := j.progress.PersistData
			inMemoryData := &inMemoryData{}
			if err := json.Unmarshal([]byte(persistData), inMemoryData); err != nil {
				return xerror.Errorf(xerror.Normal, "unmarshal persistData failed, persistData: %s", persistData)
			}
			j.progress.InMemoryData = inMemoryData
		}

		// Step 4.1: cancel the running restore job which submitted by former progress, if exists
		if err := j.IDest.CancelRestoreIfExists(j.Src.Database); err != nil {
			return err
		}

		// Step 4.2: start a new fullsync && persist
		inMemoryData := j.progress.InMemoryData.(*inMemoryData)
		snapshotName := inMemoryData.SnapshotName
		restoreSnapshotName := restoreSnapshotName(snapshotName)
		snapshotResp := inMemoryData.SnapshotResp

		// Step 4.3: restore snapshot to dest
		dest := &j.Dest
		destRpc, err := j.factory.NewFeRpc(dest)
		if err != nil {
			return err
		}
		log.Debugf("partial sync begin restore snapshot %s to %s", snapshotName, restoreSnapshotName)

		var tableRefs []*festruct.TTableRef

		// ATTN: The table name of the alias is from the source cluster.
		if aliasName, ok := j.progress.TableAliases[table]; ok {
			log.Infof("partial sync with table alias, table: %s, alias: %s", table, aliasName)
			tableRefs = make([]*festruct.TTableRef, 0)
			tableRef := &festruct.TTableRef{
				Table:     &table,
				AliasName: &aliasName,
			}
			tableRefs = append(tableRefs, tableRef)
		} else if j.isTableSyncWithAlias() {
			log.Infof("table sync snapshot not same name, table: %s, dest table: %s", j.Src.Table, j.Dest.Table)
			tableRefs = make([]*festruct.TTableRef, 0)
			tableRef := &festruct.TTableRef{
				Table:     &j.Src.Table,
				AliasName: &j.Dest.Table,
			}
			tableRefs = append(tableRefs, tableRef)
		}

		restoreReq := rpc.RestoreSnapshotRequest{
			TableRefs:      tableRefs,
			SnapshotName:   restoreSnapshotName,
			SnapshotResult: snapshotResp,

			// DO NOT drop exists tables and partitions
			CleanPartitions: false,
			CleanTables:     false,
			AtomicRestore:   false,
		}
		restoreResp, err := destRpc.RestoreSnapshot(dest, &restoreReq)
		if err != nil {
			return err
		}
		if restoreResp.Status.GetStatusCode() != tstatus.TStatusCode_OK {
			return xerror.Errorf(xerror.Normal, "restore snapshot failed, status: %v", restoreResp.Status)
		}
		log.Infof("partial sync restore snapshot resp: %v", restoreResp)

		for {
			restoreFinished, err := j.IDest.CheckRestoreFinished(restoreSnapshotName)
			if err != nil {
				return err
			}

			if restoreFinished {
				j.progress.NextSubCheckpoint(PersistRestoreInfo, restoreSnapshotName)
				break
			}
			// retry for  MAX_CHECK_RETRY_TIMES, timeout, continue
		}

	case PersistRestoreInfo:
		// Step 5: Update job progress && dest table id
		// update job info, only for dest table id
		var targetName = table
		if alias, ok := j.progress.TableAliases[table]; ok {
			if j.isTableSyncWithAlias() {
				targetName = j.Dest.Table
			}

			// check table exists to ensure the idempotent
			if exist, err := j.IDest.CheckTableExistsByName(alias); err != nil {
				return err
			} else if exist {
				log.Infof("partial sync swap table with alias, table: %s, alias: %s", targetName, alias)
				swap := false // drop the old table
				if err := j.IDest.ReplaceTable(alias, targetName, swap); err != nil {
					return err
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
			srcTableId, err := j.srcMeta.GetTableId(table)
			if err != nil {
				return err
			}
			j.progress.TableMapping[srcTableId] = destTable.Id
			j.progress.NextWithPersist(j.progress.CommitSeq, DBTablesIncrementalSync, Done, "")
		case TableSync:
			commitSeq, ok := j.progress.TableCommitSeqMap[j.Src.TableId]
			if !ok {
				return xerror.Errorf(xerror.Normal, "table id %d, commit seq not found", j.Src.TableId)
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
	}

	switch j.progress.SubSyncState {
	case Done:
		log.Infof("fullsync status: done")
		if err := j.newSnapshot(j.progress.CommitSeq); err != nil {
			return err
		}

	case BeginCreateSnapshot:
		// Step 1: Create snapshot
		log.Infof("fullsync status: create snapshot")

		backupTableList := make([]string, 0)
		switch j.SyncType {
		case DBSync:
			tables, err := j.srcMeta.GetTables()
			if err != nil {
				return err
			}
			for _, table := range tables {
				backupTableList = append(backupTableList, table.Name)
			}
		case TableSync:
			backupTableList = append(backupTableList, j.Src.Table)
		default:
			return xerror.Errorf(xerror.Normal, "invalid sync type %s", j.SyncType)
		}

		if featureCancelConflictBackupRestoreJob {
			if err := j.ISrc.CancelBackupIfExists(); err != nil {
				return err
			}
		}

		snapshotName, err := j.ISrc.CreateSnapshotAndWaitForDone(backupTableList)
		if err != nil {
			return err
		}

		j.progress.NextSubCheckpoint(GetSnapshotInfo, snapshotName)

	case GetSnapshotInfo:
		// Step 2: Get snapshot info
		log.Infof("fullsync status: get snapshot info")

		snapshotName := j.progress.PersistData
		src := &j.Src
		srcRpc, err := j.factory.NewFeRpc(src)
		if err != nil {
			return err
		}

		log.Debugf("fullsync begin get snapshot %s", snapshotName)
		snapshotResp, err := srcRpc.GetSnapshot(src, snapshotName)
		if err != nil {
			return err
		}

		if snapshotResp.Status.GetStatusCode() != tstatus.TStatusCode_OK {
			err = xerror.Errorf(xerror.FE, "get snapshot failed, status: %v", snapshotResp.Status)
			return err
		}

		log.Tracef("fullsync snapshot job: %.128s", snapshotResp.GetJobInfo())
		if !snapshotResp.IsSetJobInfo() {
			return xerror.New(xerror.Normal, "jobInfo is not set")
		}

		tableCommitSeqMap, err := ExtractTableCommitSeqMap(snapshotResp.GetJobInfo())
		if err != nil {
			return err
		}

		if j.SyncType == TableSync {
			if _, ok := tableCommitSeqMap[j.Src.TableId]; !ok {
				return xerror.Errorf(xerror.Normal, "table id %d, commit seq not found", j.Src.TableId)
			}
		}

		inMemoryData := &inMemoryData{
			SnapshotName:      snapshotName,
			SnapshotResp:      snapshotResp,
			TableCommitSeqMap: tableCommitSeqMap,
		}
		j.progress.NextSubVolatile(AddExtraInfo, inMemoryData)

	case AddExtraInfo:
		// Step 3: Add extra info
		log.Infof("fullsync status: add extra info")

		inMemoryData := j.progress.InMemoryData.(*inMemoryData)
		snapshotResp := inMemoryData.SnapshotResp
		jobInfo := snapshotResp.GetJobInfo()
		tableCommitSeqMap := inMemoryData.TableCommitSeqMap

		log.Infof("snapshot response meta size: %d, job info size: %d",
			len(snapshotResp.Meta), len(snapshotResp.JobInfo))

		jobInfoBytes, err := j.addExtraInfo(jobInfo)
		if err != nil {
			return err
		}
		log.Debugf("job info size: %d, bytes: %.128s", len(jobInfoBytes), string(jobInfoBytes))
		snapshotResp.SetJobInfo(jobInfoBytes)

		var commitSeq int64 = math.MaxInt64
		switch j.SyncType {
		case DBSync:
			for _, seq := range tableCommitSeqMap {
				commitSeq = utils.Min(commitSeq, seq)
			}
			j.progress.TableCommitSeqMap = tableCommitSeqMap // persist in CommitNext
		case TableSync:
			commitSeq = tableCommitSeqMap[j.Src.TableId]
		}
		j.progress.CommitNextSubWithPersist(commitSeq, RestoreSnapshot, inMemoryData)

	case RestoreSnapshot:
		// Step 4: Restore snapshot
		log.Infof("fullsync status: restore snapshot")

		if j.progress.InMemoryData == nil {
			persistData := j.progress.PersistData
			inMemoryData := &inMemoryData{}
			if err := json.Unmarshal([]byte(persistData), inMemoryData); err != nil {
				return xerror.Errorf(xerror.Normal, "unmarshal persistData failed, persistData: %s", persistData)
			}
			j.progress.InMemoryData = inMemoryData
		}

		// Step 4.1: cancel the running restore job which by the former process, if exists
		if err := j.IDest.CancelRestoreIfExists(j.Src.Database); err != nil {
			return err
		}

		// Step 4.2: start a new fullsync && persist
		inMemoryData := j.progress.InMemoryData.(*inMemoryData)
		snapshotName := inMemoryData.SnapshotName
		restoreSnapshotName := restoreSnapshotName(snapshotName)
		snapshotResp := inMemoryData.SnapshotResp

		// Step 4.3: restore snapshot to dest
		dest := &j.Dest
		destRpc, err := j.factory.NewFeRpc(dest)
		if err != nil {
			return err
		}
		log.Debugf("begin restore snapshot %s to %s", snapshotName, restoreSnapshotName)

		var tableRefs []*festruct.TTableRef
		if j.isTableSyncWithAlias() {
			log.Debugf("table sync snapshot not same name, table: %s, dest table: %s", j.Src.Table, j.Dest.Table)
			tableRefs = make([]*festruct.TTableRef, 0)
			tableRef := &festruct.TTableRef{
				Table:     &j.Src.Table,
				AliasName: &j.Dest.Table,
			}
			tableRefs = append(tableRefs, tableRef)
		}
		if len(j.progress.TableAliases) > 0 {
			tableRefs = make([]*festruct.TTableRef, 0)
			for table, alias := range j.progress.TableAliases {
				log.Debugf("fullsync alias table from %s to %s", table, alias)
				tableRef := &festruct.TTableRef{
					Table:     &table,
					AliasName: &alias,
				}
				tableRefs = append(tableRefs, tableRef)
			}
		}

		restoreReq := rpc.RestoreSnapshotRequest{
			TableRefs:       tableRefs,
			SnapshotName:    restoreSnapshotName,
			SnapshotResult:  snapshotResp,
			CleanPartitions: false,
			CleanTables:     false,
			AtomicRestore:   false,
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
		restoreResp, err := destRpc.RestoreSnapshot(dest, &restoreReq)
		if err != nil {
			return err
		}
		if restoreResp.Status.GetStatusCode() != tstatus.TStatusCode_OK {
			return xerror.Errorf(xerror.Normal, "restore snapshot failed, status: %v", restoreResp.Status)
		}
		log.Infof("resp: %v", restoreResp)

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
					j.progress.TableAliases[tableName] = tableAlias(tableName)
					j.progress.CommitNextSubWithPersist(j.progress.CommitSeq, RestoreSnapshot, inMemoryData)
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
				break
			} else if err != nil {
				return err
			}

			if restoreFinished {
				j.progress.NextSubCheckpoint(PersistRestoreInfo, restoreSnapshotName)
				break
			}
			// retry for  MAX_CHECK_RETRY_TIMES, timeout, continue
		}

	case PersistRestoreInfo:
		// Step 5: Update job progress && dest table id
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
				if j.isTableSyncWithAlias() {
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
				srcTableName, err := j.srcMeta.GetTableNameById(srcTableId)
				if err != nil {
					return err
				}

				// If srcTableName is empty, it may be deleted.
				// No need to map it to dest table
				if srcTableName == "" {
					log.Warnf("the name of source table id: %d is empty, no need to map it to dest table", srcTableId)
					continue
				}

				destTableId, err := j.destMeta.GetTableId(srcTableName)
				if err != nil {
					return err
				}

				tableMapping[srcTableId] = destTableId
			}

			j.progress.TableMapping = tableMapping
			j.progress.ShadowIndexes = nil
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

// only called by DBSync, TableSync tableId is in Src/Dest Spec
func (j *Job) getDestTableIdBySrc(srcTableId int64) (int64, error) {
	if j.progress.TableMapping != nil {
		if destTableId, ok := j.progress.TableMapping[srcTableId]; ok {
			return destTableId, nil
		}
		log.Warnf("table mapping not found, srcTableId: %d", srcTableId)
	} else {
		log.Warnf("table mapping not found, srcTableId: %d", srcTableId)
		j.progress.TableMapping = make(map[int64]int64)
	}

	srcTableName, err := j.srcMeta.GetTableNameById(srcTableId)
	if err != nil {
		return 0, err
	}

	if destTableId, err := j.destMeta.GetTableId(srcTableName); err != nil {
		return 0, err
	} else {
		j.progress.TableMapping[srcTableId] = destTableId
		return destTableId, nil
	}
}

func (j *Job) isBinlogCommitted(tableId int64, binlogCommitSeq int64) bool {
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

func (j *Job) getDbSyncTableRecords(upsert *record.Upsert) ([]*record.TableRecord, error) {
	commitSeq := upsert.CommitSeq
	tableCommitSeqMap := j.progress.TableCommitSeqMap
	tableRecords := make([]*record.TableRecord, 0, len(upsert.TableRecords))

	for tableId, tableRecord := range upsert.TableRecords {
		// DBIncrementalSync
		if tableCommitSeqMap == nil {
			tableRecords = append(tableRecords, tableRecord)
			continue
		}

		if tableCommitSeq, ok := tableCommitSeqMap[tableId]; ok {
			if commitSeq > tableCommitSeq {
				tableRecords = append(tableRecords, tableRecord)
			}
		} else {
			// for db partial sync
			tableRecords = append(tableRecords, tableRecord)
		}
	}

	return tableRecords, nil
}

func (j *Job) getReleatedTableRecords(upsert *record.Upsert) ([]*record.TableRecord, error) {
	var tableRecords []*record.TableRecord //, 0, len(upsert.TableRecords))

	switch j.SyncType {
	case DBSync:
		records, err := j.getDbSyncTableRecords(upsert)
		if err != nil {
			return nil, err
		}

		if len(records) == 0 {
			return nil, nil
		}
		tableRecords = records
	case TableSync:
		tableRecord, ok := upsert.TableRecords[j.Src.TableId]
		if !ok {
			return nil, xerror.Errorf(xerror.Normal, "table record not found, table: %s", j.Src.Table)
		}

		tableRecords = make([]*record.TableRecord, 0, 1)
		tableRecords = append(tableRecords, tableRecord)
	default:
		return nil, xerror.Errorf(xerror.Normal, "invalid sync type: %s", j.SyncType)
	}

	return tableRecords, nil
}

// Table ingestBinlog
func (j *Job) ingestBinlog(txnId int64, tableRecords []*record.TableRecord) ([]*ttypes.TTabletCommitInfo, error) {
	log.Infof("ingestBinlog, txnId: %d", txnId)

	job, err := j.jobFactory.CreateJob(NewIngestContext(txnId, tableRecords, j.progress.TableMapping), j, "IngestBinlog")
	if err != nil {
		return nil, err
	}

	ingestBinlogJob, ok := job.(*IngestBinlogJob)
	if !ok {
		return nil, xerror.Errorf(xerror.Normal, "invalid job type, job: %+v", job)
	}

	job.Run()
	if err := job.Error(); err != nil {
		return nil, err
	}
	return ingestBinlogJob.CommitInfos(), nil
}

func (j *Job) handleUpsert(binlog *festruct.TBinlog) error {
	log.Infof("handle upsert binlog, sub sync state: %s, prevCommitSeq: %d, commitSeq: %d",
		j.progress.SubSyncState, j.progress.PrevCommitSeq, j.progress.CommitSeq)

	// inMemory will be update in state machine, but progress keep any, so progress.inMemory is also latest, well call NextSubCheckpoint don't need to upate inMemory in progress
	type inMemoryData struct {
		CommitSeq    int64                       `json:"commit_seq"`
		TxnId        int64                       `json:"txn_id"`
		DestTableIds []int64                     `json:"dest_table_ids"`
		TableRecords []*record.TableRecord       `json:"table_records"`
		CommitInfos  []*ttypes.TTabletCommitInfo `json:"commit_infos"`
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
		log.Errorf("need rollback, err: %+v", err)
		j.progress.NextSubCheckpoint(RollbackTransaction, inMemoryData)
	}

	committed := func() {
		log.Infof("txn committed, commitSeq: %d, cleanup", j.progress.CommitSeq)

		inMemoryData := j.progress.InMemoryData.(*inMemoryData)
		commitSeq := j.progress.CommitSeq
		destTableIds := inMemoryData.DestTableIds
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
		log.Debugf("upsert: %v", upsert)

		// Step 1: get related tableRecords
		tableRecords, err := j.getReleatedTableRecords(upsert)
		if err != nil {
			log.Errorf("get related table records failed, err: %+v", err)
		}
		if len(tableRecords) == 0 {
			log.Debug("no related table records")
			return nil
		}

		log.Debugf("tableRecords: %v", tableRecords)
		destTableIds := make([]int64, 0, len(tableRecords))
		if j.SyncType == DBSync {
			for _, tableRecord := range tableRecords {
				if destTableId, err := j.getDestTableIdBySrc(tableRecord.Id); err != nil {
					return err
				} else {
					destTableIds = append(destTableIds, destTableId)
				}
			}
		} else {
			destTableIds = append(destTableIds, j.Dest.TableId)
		}
		inMemoryData := &inMemoryData{
			CommitSeq:    upsert.CommitSeq,
			DestTableIds: destTableIds,
			TableRecords: tableRecords,
		}
		j.progress.NextSubVolatile(BeginTransaction, inMemoryData)

	case BeginTransaction:
		// Step 2: begin txn
		inMemoryData := j.progress.InMemoryData.(*inMemoryData)
		commitSeq := inMemoryData.CommitSeq
		log.Debugf("begin txn, dest: %v, commitSeq: %d", dest, commitSeq)

		destRpc, err := j.factory.NewFeRpc(dest)
		if err != nil {
			return err
		}

		label := j.newLabel(commitSeq)

		beginTxnResp, err := destRpc.BeginTransaction(dest, label, inMemoryData.DestTableIds)
		if err != nil {
			return err
		}
		log.Debugf("resp: %v", beginTxnResp)
		if beginTxnResp.GetStatus().GetStatusCode() != tstatus.TStatusCode_OK {
			if isTableNotFound(beginTxnResp.GetStatus()) && j.SyncType == DBSync {
				// It might caused by the staled TableMapping entries.
				// In order to rebuild the dest table ids, this progress should be rollback.
				j.progress.Rollback(j.SkipError)
				for _, tableRecord := range inMemoryData.TableRecords {
					delete(j.progress.TableMapping, tableRecord.Id)
				}
			}
			return xerror.Errorf(xerror.Normal, "begin txn failed, status: %v", beginTxnResp.GetStatus())
		}
		txnId := beginTxnResp.GetTxnId()
		log.Debugf("TxnId: %d, DbId: %d", txnId, beginTxnResp.GetDbId())

		inMemoryData.TxnId = txnId
		j.progress.NextSubCheckpoint(IngestBinlog, inMemoryData)

	case IngestBinlog:
		log.Debug("ingest binlog")
		if err := updateInMemory(); err != nil {
			return err
		}
		inMemoryData := j.progress.InMemoryData.(*inMemoryData)
		tableRecords := inMemoryData.TableRecords
		txnId := inMemoryData.TxnId

		// Step 3: ingest binlog
		var commitInfos []*ttypes.TTabletCommitInfo
		commitInfos, err := j.ingestBinlog(txnId, tableRecords)
		if err != nil {
			rollback(err, inMemoryData)
			return err
		} else {
			log.Debugf("commitInfos: %v", commitInfos)
			inMemoryData.CommitInfos = commitInfos
			j.progress.NextSubCheckpoint(CommitTransaction, inMemoryData)
		}

	case CommitTransaction:
		// Step 4: commit txn
		log.Debug("commit txn")
		if err := updateInMemory(); err != nil {
			return err
		}
		inMemoryData := j.progress.InMemoryData.(*inMemoryData)
		txnId := inMemoryData.TxnId
		commitInfos := inMemoryData.CommitInfos

		destRpc, err := j.factory.NewFeRpc(dest)
		if err != nil {
			rollback(err, inMemoryData)
			break
		}

		resp, err := destRpc.CommitTransaction(dest, txnId, commitInfos)
		if err != nil {
			rollback(err, inMemoryData)
			break
		}

		if statusCode := resp.Status.GetStatusCode(); statusCode == tstatus.TStatusCode_PUBLISH_TIMEOUT {
			dest.WaitTransactionDone(txnId)
		} else if statusCode != tstatus.TStatusCode_OK {
			err := xerror.Errorf(xerror.Normal, "commit txn failed, status: %v", resp.Status)
			rollback(err, inMemoryData)
			break
		}

		log.Infof("TxnId: %d committed, resp: %v", txnId, resp)
		committed()

		return nil

	case RollbackTransaction:
		log.Debugf("Rollback txn")
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

		log.Infof("rollback TxnId: %d resp: %v", txnId, resp)
		j.progress.Rollback(j.SkipError)
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

	if j.isBinlogCommitted(addPartition.TableId, binlog.GetCommitSeq()) {
		return nil
	}

	if addPartition.IsTemp {
		log.Infof("skip add temporary partition because backup/restore table with temporary partitions is not supported yet")
		return nil
	}

	var destTableName string
	if j.SyncType == TableSync {
		destTableName = j.Dest.Table
	} else if j.SyncType == DBSync {
		destTableId, err := j.getDestTableIdBySrc(addPartition.TableId)
		if err != nil {
			return err
		}

		if destTableName, err = j.destMeta.GetTableNameById(destTableId); err != nil {
			return err
		} else if destTableName == "" {
			return xerror.Errorf(xerror.Normal, "tableId %d not found in destMeta", destTableId)
		}
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

	if j.isBinlogCommitted(dropPartition.TableId, binlog.GetCommitSeq()) {
		return nil
	}

	var destTableName string
	if j.SyncType == TableSync {
		destTableName = j.Dest.Table
	} else if j.SyncType == DBSync {
		destTableId, err := j.getDestTableIdBySrc(dropPartition.TableId)
		if err != nil {
			return err
		}

		if destTableName, err = j.destMeta.GetTableNameById(destTableId); err != nil {
			return err
		} else if destTableName == "" {
			return xerror.Errorf(xerror.Normal, "tableId %d not found in destMeta", destTableId)
		}
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

	if featureCreateViewDropExists {
		viewRegex := regexp.MustCompile(`(?i)^CREATE(\s+)VIEW`)
		isCreateView := viewRegex.MatchString(createTable.Sql)
		tableName := strings.TrimSpace(createTable.TableName)
		if isCreateView && len(tableName) > 0 {
			// drop view if exists
			log.Infof("feature_create_view_drop_exists is enabled, try drop view %s before creating", tableName)
			if err = j.IDest.DropView(tableName); err != nil {
				return xerror.Wrapf(err, xerror.Normal, "drop view before create view %s, table id=%d",
					tableName, createTable.TableId)
			}
		}
	}

	if err = j.IDest.CreateTableOrView(createTable, j.Src.Database); err != nil {
		return xerror.Wrapf(err, xerror.Normal, "create table %d", createTable.TableId)
	}

	j.srcMeta.ClearTablesCache()
	j.destMeta.ClearTablesCache()

	var srcTableName string
	srcTableName, err = j.srcMeta.GetTableNameById(createTable.TableId)
	if err != nil {
		return err
	}

	if len(srcTableName) == 0 {
		// The table is not found in upstream, try read it from the binlog record,
		// but it might failed because the `tableName` field is added after doris 2.0.3.
		srcTableName = strings.TrimSpace(createTable.TableName)
		if len(srcTableName) == 0 {
			return xerror.Errorf(xerror.Normal, "the table with id %d is not found in the upstream cluster, create table: %s",
				createTable.TableId, createTable.String())
		}
		log.Infof("the table id %d is not found in the upstream, use the name %s from the binlog record",
			createTable.TableId, srcTableName)
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
	j.progress.Done()
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

	tableName := dropTable.TableName
	// deprecated
	if tableName == "" {
		dirtySrcTables := j.srcMeta.DirtyGetTables()
		srcTable, ok := dirtySrcTables[dropTable.TableId]
		if !ok {
			return xerror.Errorf(xerror.Normal, "table not found, tableId: %d", dropTable.TableId)
		}

		tableName = srcTable.Name
	}

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

	j.srcMeta.ClearTablesCache()
	j.destMeta.ClearTablesCache()
	if j.progress.TableMapping != nil {
		delete(j.progress.TableMapping, dropTable.TableId)
		j.progress.Done()
	}
	return nil
}

func (j *Job) handleDummy(binlog *festruct.TBinlog) error {
	dummyCommitSeq := binlog.GetCommitSeq()

	log.Infof("handle dummy binlog, need full sync. SyncType: %v, seq: %v", j.SyncType, dummyCommitSeq)

	return j.newSnapshot(dummyCommitSeq)
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

	if !alterJob.IsFinished() {
		switch alterJob.JobState {
		case record.ALTER_JOB_STATE_PENDING:
			// Once the schema change step to WAITING_TXN, the upsert to the shadow indexes is allowed,
			// but the dest indexes of the downstream cluster hasn't been created.
			//
			// To filter the upsert to the shadow indexes, save the shadow index ids here.
			if j.progress.ShadowIndexes == nil {
				j.progress.ShadowIndexes = make(map[int64]int64)
			}
			for shadowIndexId, originIndexId := range alterJob.ShadowIndexes {
				j.progress.ShadowIndexes[shadowIndexId] = originIndexId
			}
		case record.ALTER_JOB_STATE_CANCELLED:
			// clear the shadow indexes
			for shadowIndexId := range alterJob.ShadowIndexes {
				delete(j.progress.ShadowIndexes, shadowIndexId)
			}
		}
		return nil
	}

	// drop table dropTableSql
	var destTableName string
	if j.SyncType == TableSync {
		destTableName = j.Dest.Table
	} else {
		destTableName = alterJob.TableName
	}

	if featureSchemaChangePartialSync && alterJob.Type == record.ALTER_JOB_SCHEMA_CHANGE {
		// Once partial snapshot finished, the shadow indexes will be convert to normal indexes.
		for shadowIndexId := range alterJob.ShadowIndexes {
			delete(j.progress.ShadowIndexes, shadowIndexId)
		}

		replaceTable := true
		return j.newPartialSnapshot(alterJob.TableName, nil, replaceTable)
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

	return j.newSnapshot(j.progress.CommitSeq)
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

	if j.isBinlogCommitted(lightningSchemaChange.TableId, binlog.GetCommitSeq()) {
		return nil
	}

	tableAlias := ""
	if j.isTableSyncWithAlias() {
		tableAlias = j.Dest.Table
	}
	return j.IDest.LightningSchemaChange(j.Src.Database, tableAlias, lightningSchemaChange)
}

// handle rename column
func (j *Job) handleRenameColumn(binlog *festruct.TBinlog) error {
	log.Infof("handle rename column binlog")

	data := binlog.GetData()
	renameColumn, err := record.NewRenameColumnFromJson(data)
	if err != nil {
		return err
	}

	if j.isBinlogCommitted(renameColumn.TableId, binlog.GetCommitSeq()) {
		return nil
	}

	destTableId, err := j.getDestTableIdBySrc(renameColumn.TableId)
	if err != nil {
		return err
	}

	destTableName, err := j.destMeta.GetTableNameById(destTableId)
	if err != nil {
		return err
	} else if destTableName == "" {
		return xerror.Errorf(xerror.Normal, "tableId %d not found in destMeta", destTableId)
	}

	err = j.IDest.RenameColumn(destTableName, renameColumn)
	return err
}

// handle modify comment
func (j *Job) handleModifyComment(binlog *festruct.TBinlog) error {
	log.Infof("handle modify comment binlog")

	data := binlog.GetData()
	modifyComment, err := record.NewModifyCommentFromJson(data)
	if err != nil {
		return err
	}

	destTableId, err := j.getDestTableIdBySrc(modifyComment.TblId)
	if err != nil {
		return err
	}

	destTableName, err := j.destMeta.GetTableNameById(destTableId)
	if err != nil {
		return err
	} else if destTableName == "" {
		return xerror.Errorf(xerror.Normal, "tableId %d not found in destMeta", destTableId)
	}

	err = j.IDest.ModifyComment(destTableName, modifyComment)
	return err
}

func (j *Job) handleTruncateTable(binlog *festruct.TBinlog) error {
	log.Infof("handle truncate table binlog, prevCommitSeq: %d, commitSeq: %d",
		j.progress.PrevCommitSeq, j.progress.CommitSeq)

	data := binlog.GetData()
	truncateTable, err := record.NewTruncateTableFromJson(data)
	if err != nil {
		return err
	}

	if j.isBinlogCommitted(truncateTable.TableId, binlog.GetCommitSeq()) {
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
		if srcTableName, err := j.srcMeta.GetTableNameById(truncateTable.TableId); err == nil {
			// if err != nil, maybe truncate table had been dropped
			j.srcMeta.ClearTable(j.Src.Database, srcTableName)
		}
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

	if j.isBinlogCommitted(replacePartition.TableId, binlog.GetCommitSeq()) {
		return nil
	}

	if !replacePartition.StrictRange {
		log.Warnf("replacing partitions with non strict range is not supported yet, replace partition record: %s", string(data))
		return j.newSnapshot(j.progress.CommitSeq)
	}

	if replacePartition.UseTempName {
		log.Warnf("replacing partitions with use tmp name is not supported yet, replace partition record: %s", string(data))
		return j.newSnapshot(j.progress.CommitSeq)
	}

	oldPartitions := strings.Join(replacePartition.Partitions, ",")
	newPartitions := strings.Join(replacePartition.TempPartitions, ",")
	log.Infof("table %s replace partitions %s with temp partitions %s",
		replacePartition.TableName, oldPartitions, newPartitions)

	partitions := replacePartition.Partitions
	if replacePartition.UseTempName {
		partitions = replacePartition.TempPartitions
	}

	return j.newPartialSnapshot(replacePartition.TableName, partitions, false)
}

// handle rename table
func (j *Job) handleRenameTable(binlog *festruct.TBinlog) error {
	log.Infof("handle rename table binlog")

	data := binlog.GetData()
	renameTable, err := record.NewRenameTableFromJson(data)
	if err != nil {
		return err
	}

	j.srcMeta.GetTables()

	// don't support rename table when table sync
	var destTableName string
	err = nil
	if j.SyncType == TableSync {
		log.Warnf("rename table is not supported when table sync")
		return xerror.Errorf(xerror.Normal, "rename table is not supported when table sync")
	} else if j.SyncType == DBSync {
		destTableId, err := j.getDestTableIdBySrc(renameTable.TableId)
		if err != nil {
			return err
		}

		if destTableName, err = j.destMeta.GetTableNameById(destTableId); err != nil {
			return err
		} else if destTableName == "" {
			return xerror.Errorf(xerror.Normal, "tableId %d not found in destMeta", destTableId)
		}
		err = j.IDest.RenameTable(destTableName, renameTable)

		if err == nil {
			j.destMeta.GetTables()
		}
	}

	return err
}

// return: error && bool backToRunLoop
func (j *Job) handleBinlogs(binlogs []*festruct.TBinlog) (error, bool) {
	log.Infof("handle binlogs, binlogs size: %d", len(binlogs))

	for _, binlog := range binlogs {
		// Step 1: dispatch handle binlog
		if err := j.handleBinlog(binlog); err != nil {
			log.Errorf("handle binlog failed, prevCommitSeq: %d, commitSeq: %d, binlog type: %s, binlog data: %s",
				j.progress.PrevCommitSeq, j.progress.CommitSeq, binlog.GetType(), binlog.GetData())
			return err, false
		}

		// Step 2: check job state, if not incrementalSync, such as DBPartialSync, break
		if !j.isIncrementalSync() {
			log.Debugf("job state is not incremental sync, back to run loop, job state: %s", j.progress.SyncState)
			return nil, true
		}

		// Step 3: update progress
		commitSeq := binlog.GetCommitSeq()
		if j.SyncType == DBSync && j.progress.TableCommitSeqMap != nil {
			// when all table commit seq > commitSeq, it's true
			reachSwitchToDBIncrementalSync := true
			for _, tableCommitSeq := range j.progress.TableCommitSeqMap {
				if tableCommitSeq > commitSeq {
					reachSwitchToDBIncrementalSync = false
					break
				}
			}

			if reachSwitchToDBIncrementalSync {
				j.progress.TableCommitSeqMap = nil
				j.progress.NextWithPersist(j.progress.CommitSeq, DBIncrementalSync, Done, "")
			}
		}

		// Step 4: update progress to db
		if !j.progress.IsDone() {
			j.progress.Done()
		}
	}
	return nil, false
}

func (j *Job) handleBinlog(binlog *festruct.TBinlog) error {
	if binlog == nil || !binlog.IsSetCommitSeq() {
		return xerror.Errorf(xerror.Normal, "invalid binlog: %v", binlog)
	}

	log.Debugf("binlog type: %s, binlog data: %s", binlog.GetType(), binlog.GetData())

	// Step 2: update job progress
	j.progress.StartHandle(binlog.GetCommitSeq())
	xmetrics.HandlingBinlog(j.Name, binlog.GetCommitSeq())

	switch binlog.GetType() {
	case festruct.TBinlogType_UPSERT:
		return j.handleUpsert(binlog)
	case festruct.TBinlogType_ADD_PARTITION:
		return j.handleAddPartition(binlog)
	case festruct.TBinlogType_CREATE_TABLE:
		return j.handleCreateTable(binlog)
	case festruct.TBinlogType_DROP_PARTITION:
		return j.handleDropPartition(binlog)
	case festruct.TBinlogType_DROP_TABLE:
		return j.handleDropTable(binlog)
	case festruct.TBinlogType_ALTER_JOB:
		return j.handleAlterJob(binlog)
	case festruct.TBinlogType_MODIFY_TABLE_ADD_OR_DROP_COLUMNS:
		return j.handleLightningSchemaChange(binlog)
	case festruct.TBinlogType_RENAME_COLUMN:
		return j.handleRenameColumn(binlog)
	case festruct.TBinlogType_MODIFY_COMMENT:
		return j.handleModifyComment(binlog)
	case festruct.TBinlogType_DUMMY:
		return j.handleDummy(binlog)
	case festruct.TBinlogType_ALTER_DATABASE_PROPERTY:
		log.Info("handle alter database property binlog, ignore it")
	case festruct.TBinlogType_MODIFY_TABLE_PROPERTY:
		log.Info("handle alter table property binlog, ignore it")
	case festruct.TBinlogType_BARRIER:
		log.Info("handle barrier binlog, ignore it")
	case festruct.TBinlogType_TRUNCATE_TABLE:
		return j.handleTruncateTable(binlog)
	case festruct.TBinlogType_RENAME_TABLE:
		return j.handleRenameTable(binlog)
	case festruct.TBinlogType_REPLACE_PARTITIONS:
		return j.handleReplacePartitions(binlog)
	default:
		return xerror.Errorf(xerror.Normal, "unknown binlog type: %v", binlog.GetType())
	}

	return nil
}

func (j *Job) recoverIncrementalSync() error {
	switch j.progress.SubSyncState.BinlogType {
	case BinlogUpsert:
		return j.handleUpsert(nil)
	default:
		j.progress.Rollback(j.SkipError)
	}

	return nil
}

func (j *Job) incrementalSync() error {
	if !j.progress.IsDone() {
		log.Infof("job progress is not done, need recover. state: %s, prevCommitSeq: %d, commitSeq: %d",
			j.progress.SubSyncState, j.progress.PrevCommitSeq, j.progress.CommitSeq)

		return j.recoverIncrementalSync()
	}

	// Step 1: get binlog
	log.Debug("start incremental sync")
	src := &j.Src
	srcRpc, err := j.factory.NewFeRpc(src)
	if err != nil {
		log.Errorf("new fe rpc failed, src: %v, err: %+v", src, err)
		return err
	}

	// Step 2: handle all binlog
	for {
		if j.forceFullsync {
			log.Warnf("job is forced to step fullsync by user")
			j.forceFullsync = false
			_ = j.newSnapshot(j.progress.CommitSeq)
			return nil
		}

		// The CommitSeq is equals to PrevCommitSeq in here.
		commitSeq := j.progress.CommitSeq
		log.Debugf("src: %s, commitSeq: %v", src, commitSeq)

		getBinlogResp, err := srcRpc.GetBinlog(src, commitSeq)
		if err != nil {
			return err
		}
		log.Debugf("resp: %v", getBinlogResp)

		// Step 2.1: check binlog status
		status := getBinlogResp.GetStatus()
		switch status.StatusCode {
		case tstatus.TStatusCode_OK:
		case tstatus.TStatusCode_BINLOG_TOO_OLD_COMMIT_SEQ:
		case tstatus.TStatusCode_BINLOG_TOO_NEW_COMMIT_SEQ:
			return nil
		case tstatus.TStatusCode_BINLOG_DISABLE:
			return xerror.Errorf(xerror.Normal, "binlog is disabled")
		case tstatus.TStatusCode_BINLOG_NOT_FOUND_DB:
			return xerror.Errorf(xerror.Normal, "can't found db")
		case tstatus.TStatusCode_BINLOG_NOT_FOUND_TABLE:
			return xerror.Errorf(xerror.Normal, "can't found table")
		default:
			return xerror.Errorf(xerror.Normal, "invalid binlog status type: %v", status.StatusCode)
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
		log.Debug("table full sync")
		return j.fullSync()
	case TableIncrementalSync:
		log.Debug("table incremental sync")
		return j.incrementalSync()
	case TablePartialSync:
		log.Debug("table partial sync")
		return j.partialSync()
	default:
		return xerror.Errorf(xerror.Normal, "unknown sync state: %v", j.progress.SyncState)
	}
}

func (j *Job) dbTablesIncrementalSync() error {
	log.Debug("db tables incremental sync")

	return j.incrementalSync()
}

func (j *Job) dbSpecificTableFullSync() error {
	log.Debug("db specific table full sync")

	return nil
}

func (j *Job) dbSync() error {
	switch j.progress.SyncState {
	case DBFullSync:
		log.Debug("db full sync")
		return j.fullSync()
	case DBTablesIncrementalSync:
		return j.dbTablesIncrementalSync()
	case DBSpecificTableFullSync:
		return j.dbSpecificTableFullSync()
	case DBIncrementalSync:
		log.Debug("db incremental sync")
		return j.incrementalSync()
	case DBPartialSync:
		log.Debug("db partial sync")
		return j.partialSync()
	default:
		return xerror.Errorf(xerror.Normal, "unknown db sync state: %v", j.progress.SyncState)
	}
}

func (j *Job) sync() error {
	j.lock.Lock()
	defer j.lock.Unlock()

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
func (j *Job) handleError(err error) error {
	var xerr *xerror.XError
	if !errors.As(err, &xerr) {
		log.Errorf("convert error to xerror failed, err: %+v", err)
		return nil
	}

	xmetrics.AddError(xerr)
	if xerr.IsPanic() {
		log.Errorf("job panic, job: %s, err: %+v", j.Name, err)
		return err
	}

	if xerr.Category() == xerror.Meta {
		log.Warnf("receive meta category error, make new snapshot, job: %s, err: %v", j.Name, err)
		_ = j.newSnapshot(j.progress.CommitSeq)
	}
	return nil
}

func (j *Job) run() {
	ticker := time.NewTicker(SYNC_DURATION)
	defer ticker.Stop()

	var panicError error

	for {
		j.updateJobStatus()

		// do maybeDeleted first to avoid mark job deleted after job stopped & before job run & close stop chan gap in Delete, so job will not run
		if j.maybeDeleted() {
			return
		}

		select {
		case <-j.stop:
			gls.DeleteGls(gls.GoID())
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
			panicError = j.handleError(err)
		}
	}
}

func (j *Job) newSnapshot(commitSeq int64) error {
	log.Infof("new snapshot, commitSeq: %d", commitSeq)

	j.progress.PartialSyncData = nil
	j.progress.TableAliases = nil
	switch j.SyncType {
	case TableSync:
		j.progress.NextWithPersist(commitSeq, TableFullSync, BeginCreateSnapshot, "")
		return nil
	case DBSync:
		j.progress.NextWithPersist(commitSeq, DBFullSync, BeginCreateSnapshot, "")
		return nil
	default:
		err := xerror.Panicf(xerror.Normal, "unknown table sync type: %v", j.SyncType)
		log.Fatalf("run %+v", err)
		return err
	}
}

// New partial snapshot, with the source cluster table name and the partitions to sync.
// A empty partitions means to sync the whole table.
//
// If the replace is true, the restore task will load data into a new table and replaces the old
// one when restore finished. So replace requires whole table partial sync.
func (j *Job) newPartialSnapshot(table string, partitions []string, replace bool) error {
	if j.SyncType == TableSync && table != j.Src.Table {
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
		Table:      table,
		Partitions: partitions,
	}
	j.progress.PartialSyncData = syncData
	j.progress.TableAliases = nil
	if replace {
		alias := tableAlias(table)
		j.progress.TableAliases = make(map[string]string)
		j.progress.TableAliases[table] = alias
		log.Infof("new partial snapshot, commitSeq: %d, table: %s, alias: %s", commitSeq, table, alias)
	} else {
		log.Infof("new partial snapshot, commitSeq: %d, table: %s, partitions: %v", commitSeq, table, partitions)
	}

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
	gls.ResetGls(gls.GoID(), map[interface{}]interface{}{})
	gls.Set("job", j.Name)

	// retry 3 times to check IsProgressExist
	var isProgressExist bool
	var err error
	for i := 0; i < 3; i++ {
		isProgressExist, err = j.db.IsProgressExist(j.Name)
		if err != nil {
			log.Errorf("check progress exist failed, error: %+v", err)
			continue
		}
		break
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
		if err := j.newSnapshot(0); err != nil {
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

func (j *Job) Desync() error {
	if j.SyncType == DBSync {
		return j.desyncDB()
	} else {
		return j.desyncTable()
	}
}

func (j *Job) UpdateSkipError(skipError bool) error {
	j.lock.Lock()
	defer j.lock.Unlock()

	originSkipError := j.SkipError
	if originSkipError == skipError {
		return nil
	}

	j.SkipError = skipError
	if err := j.persistJob(); err != nil {
		j.SkipError = originSkipError
		return err
	} else {
		return nil
	}
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
	log.Infof("first run check job, src: %s, dest: %s", &j.Src, &j.Dest)

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

		if enable, err := j.ISrc.IsTableEnableBinlog(); err != nil {
			return err
		} else if !enable {
			return xerror.Errorf(xerror.Normal, "src table %s.%s not enable binlog", j.Src.Database, j.Src.Table)
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
	if j.SyncType == TableSync && !j.allowTableExists {
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

func (j *Job) GetLag() (int64, error) {
	j.lock.Lock()
	defer j.lock.Unlock()

	srcSpec := &j.Src
	rpc, err := j.factory.NewFeRpc(srcSpec)
	if err != nil {
		return 0, err
	}

	commitSeq := j.progress.CommitSeq
	resp, err := rpc.GetBinlogLag(srcSpec, commitSeq)
	if err != nil {
		return 0, err
	}

	log.Debugf("resp: %v, lag: %d", resp, resp.GetLag())
	return resp.GetLag(), nil
}

func (j *Job) getJobState() JobState {
	j.lock.Lock()
	defer j.lock.Unlock()

	return j.State
}

func (j *Job) changeJobState(state JobState) error {
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

func (j *Job) ForceFullsync() {
	log.Infof("force job %s step full sync", j.Name)

	j.lock.Lock()
	defer j.lock.Unlock()
	j.forceFullsync = true
}

type RawJobStatus struct {
	state         int32
	progressState int32
}

func (j *Job) updateJobStatus() {
	atomic.StoreInt32(&j.rawStatus.state, int32(j.State))
	if j.progress != nil {
		atomic.StoreInt32(&j.rawStatus.progressState, int32(j.progress.SyncState))
	}
}

type JobStatus struct {
	Name          string `json:"name"`
	State         string `json:"state"`
	ProgressState string `json:"progress_state"`
}

func (j *Job) Status() *JobStatus {
	state := JobState(atomic.LoadInt32(&j.rawStatus.state)).String()
	progressState := SyncState(atomic.LoadInt32(&j.rawStatus.progressState)).String()

	return &JobStatus{
		Name:          j.Name,
		State:         state,
		ProgressState: progressState,
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

func restoreSnapshotName(snapshotName string) string {
	if snapshotName == "" {
		return ""
	}

	// use current seconds
	return fmt.Sprintf("%s_r_%d", snapshotName, time.Now().Unix())
}

func tableAlias(tableName string) string {
	return fmt.Sprintf("__ccr_%s_%d", tableName, time.Now().Unix())
}
