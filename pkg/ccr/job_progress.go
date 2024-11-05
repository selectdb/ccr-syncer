package ccr

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/selectdb/ccr_syncer/pkg/storage"
	"github.com/selectdb/ccr_syncer/pkg/xerror"
	"github.com/selectdb/ccr_syncer/pkg/xmetrics"
	log "github.com/sirupsen/logrus"
)

// TODO: rewrite all progress by two level state machine
// first one is sync state, second one is job state

const (
	UPDATE_JOB_PROGRESS_DURATION = time.Second * 3
)

type SyncState int

const (
	// Database sync state machine states
	DBFullSync              SyncState = 0
	DBTablesIncrementalSync SyncState = 1
	DBSpecificTableFullSync SyncState = 2
	DBIncrementalSync       SyncState = 3
	DBPartialSync           SyncState = 4 // sync partitions

	// Table sync state machine states
	TableFullSync        SyncState = 500
	TableIncrementalSync SyncState = 501
	TablePartialSync     SyncState = 502

	// TODO: add timeout state for restart full sync
)

// SyncState Stringer
func (s SyncState) String() string {
	switch s {
	case DBFullSync:
		return "DBFullSync"
	case DBTablesIncrementalSync:
		return "DBTablesIncrementalSync"
	case DBSpecificTableFullSync:
		return "DBSpecificTableFullSync"
	case DBIncrementalSync:
		return "DBIncrementalSync"
	case DBPartialSync:
		return "DBPartialSync"
	case TableFullSync:
		return "TableFullSync"
	case TableIncrementalSync:
		return "TableIncrementalSync"
	case TablePartialSync:
		return "TablePartialSync"
	default:
		return fmt.Sprintf("Unknown SyncState: %d", s)
	}
}

type BinlogType int

const (
	// Binlog type
	BinlogNone                        BinlogType = -1
	BinlogUpsert                      BinlogType = 0
	BinlogAddPartition                BinlogType = 1
	BinlogCreateTable                 BinlogType = 2
	BinlogDropPartition               BinlogType = 3
	BinlogDropTable                   BinlogType = 4
	BinlogAlterJob                    BinlogType = 5
	BinlogModifyTableAddOrDropColumns BinlogType = 6
	BinlogDummy                       BinlogType = 7
	BinlogAlterDatabaseProperty       BinlogType = 8
	BinlogModifyTableProperty         BinlogType = 9
	BinlogBarrier                     BinlogType = 10
	BinlogModifyPartitions            BinlogType = 11
	BinlogReplacePartitions           BinlogType = 12
)

type SubSyncState struct {
	State      int        `json:"state"`
	BinlogType BinlogType `json:"binlog_type"`
}

var (
	/// Sub Sync States
	Done SubSyncState = SubSyncState{State: -1, BinlogType: BinlogNone}

	// DB/Table FullSync state machine states
	BeginCreateSnapshot SubSyncState = SubSyncState{State: 0, BinlogType: BinlogNone}
	GetSnapshotInfo     SubSyncState = SubSyncState{State: 1, BinlogType: BinlogNone}
	AddExtraInfo        SubSyncState = SubSyncState{State: 2, BinlogType: BinlogNone}
	RestoreSnapshot     SubSyncState = SubSyncState{State: 3, BinlogType: BinlogNone}
	PersistRestoreInfo  SubSyncState = SubSyncState{State: 4, BinlogType: BinlogNone}
	WaitBackupDone      SubSyncState = SubSyncState{State: 5, BinlogType: BinlogNone}
	WaitRestoreDone     SubSyncState = SubSyncState{State: 6, BinlogType: BinlogNone}

	BeginTransaction    SubSyncState = SubSyncState{State: 11, BinlogType: BinlogUpsert}
	IngestBinlog        SubSyncState = SubSyncState{State: 12, BinlogType: BinlogUpsert}
	CommitTransaction   SubSyncState = SubSyncState{State: 13, BinlogType: BinlogUpsert}
	RollbackTransaction SubSyncState = SubSyncState{State: 14, BinlogType: BinlogUpsert}

	// IncrementalSync state machine states
	DB_1 SubSyncState = SubSyncState{State: 100, BinlogType: BinlogNone}
)

// SubSyncState Stringer
func (s SubSyncState) String() string {
	switch s {
	case Done:
		return "Done"
	case BeginCreateSnapshot:
		return "BeginCreateSnapshot"
	case GetSnapshotInfo:
		return "GetSnapshotInfo"
	case AddExtraInfo:
		return "AddExtraInfo"
	case RestoreSnapshot:
		return "RestoreSnapshot"
	case PersistRestoreInfo:
		return "PersistRestoreInfo"
	case BeginTransaction:
		return "BeginTransaction"
	case IngestBinlog:
		return "IngestBinlog"
	case CommitTransaction:
		return "CommitTransaction"
	case RollbackTransaction:
		return "RollbackTransaction"
	default:
		return fmt.Sprintf("Unknown sub sync state: %d, binlog type: %d", s.State, s.BinlogType)
	}
}

type JobPartialSyncData struct {
	TableId      int64    `json:"table_id"`
	Table        string   `json:"table"`
	PartitionIds []int64  `json:"partition_ids"`
	Partitions   []string `json:"partitions"`
}

type JobProgress struct {
	JobName string     `json:"job_name"`
	db      storage.DB `json:"-"`

	// Table/DB big sync state machine states
	SyncState SyncState `json:"sync_state"`
	// Sub sync state machine states
	SubSyncState SubSyncState `json:"sub_sync_state"`

	// The sync id of full/partial snapshot
	SyncId int64 `json:"job_sync_id"`
	// The commit seq where the target cluster has synced.
	PrevCommitSeq int64           `json:"prev_commit_seq"`
	CommitSeq     int64           `json:"commit_seq"`
	TableMapping  map[int64]int64 `json:"table_mapping"`
	// the upstream table id to name mapping, build during the fullsync,
	// keep snapshot to avoid rename. it might be staled.
	TableNameMapping  map[int64]string    `json:"table_name_mapping"`
	TableCommitSeqMap map[int64]int64     `json:"table_commit_seq_map"` // only for DBTablesIncrementalSync
	InMemoryData      any                 `json:"-"`
	PersistData       string              `json:"data"` // this often for binlog or snapshot info
	PartialSyncData   *JobPartialSyncData `json:"partial_sync_data,omitempty"`

	// The tables need to be replaced rather than dropped during sync.
	TableAliases map[string]string `json:"table_aliases,omitempty"`

	// The shadow indexes of the pending schema changes
	ShadowIndexes map[int64]int64 `json:"shadow_index_map,omitempty"`

	// Some fields to save the unix epoch time of the key timepoint.
	CreatedAt              int64 `json:"created_at,omitempty"`
	FullSyncStartAt        int64 `json:"full_sync_start_at,omitempty"`
	IncrementalSyncStartAt int64 `json:"incremental_sync_start_at,omitempty"`
	IngestBinlogAt         int64 `json:"ingest_binlog_at,omitempty"`
}

func (j *JobProgress) String() string {
	// const maxStringLength = 64
	return fmt.Sprintf("JobProgress{JobName: %s, SyncState: %s, SubSyncState: %s, CommitSeq: %d, TableCommitSeqMap: %v, InMemoryData: %.64v, PersistData: %.64s}", j.JobName, j.SyncState, j.SubSyncState, j.CommitSeq, j.TableCommitSeqMap, j.InMemoryData, j.PersistData)
}

func NewJobProgress(jobName string, syncType SyncType, db storage.DB) *JobProgress {
	var syncState SyncState
	if syncType == DBSync {
		syncState = DBFullSync
	} else {
		syncState = TableFullSync
	}
	return &JobProgress{
		JobName: jobName,
		db:      db,

		SyncState:    syncState,
		SubSyncState: BeginCreateSnapshot,
		CommitSeq:    0,
		TableMapping: nil,

		TableCommitSeqMap: nil,
		InMemoryData:      nil,
		PersistData:       "",
		PartialSyncData:   nil,
		TableAliases:      nil,
		ShadowIndexes:     nil,

		CreatedAt:              time.Now().Unix(),
		FullSyncStartAt:        0,
		IncrementalSyncStartAt: 0,
		IngestBinlogAt:         0,
	}
}

// create JobProgress from json data
func NewJobProgressFromJson(jobName string, db storage.DB) (*JobProgress, error) {
	// get progress from db, retry 3 times
	var err error
	var jsonData string
	for i := 0; i < 3; i++ {
		jsonData, err = db.GetProgress(jobName)
		if err != nil {
			log.Errorf("get job progress failed, error: %+v", err)
			continue
		}
		break
	}
	if err != nil {
		return nil, err
	}

	var jobProgress JobProgress
	if err := json.Unmarshal([]byte(jsonData), &jobProgress); err != nil {
		return nil, err
	} else {
		jobProgress.InMemoryData = nil
		jobProgress.db = db
		return &jobProgress, nil
	}
}

func (j *JobProgress) StartHandle(commitSeq int64) {
	j.CommitSeq = commitSeq

	j.Persist()
}

// This is in memory, not persist, only for job internal use
// need all job to be restartable
func (j *JobProgress) NextSubVolatile(subSyncState SubSyncState, inMemoryData any) {
	j.SubSyncState = subSyncState
	j.InMemoryData = inMemoryData
}

func _convertToPersistData(persistData any) string {
	if persistData == nil {
		return ""
	}

	// persistData is already json string
	if _, ok := persistData.(string); ok {
		return persistData.(string)
	}

	if persistDataJson, err := json.Marshal(persistData); err != nil {
		log.Panicf("marshal persist data failed: %+v", xerror.WithStack(err))
		return ""
	} else {
		return string(persistDataJson)
	}
}

// Persist is checkpint, next state only get it from persistData
func (j *JobProgress) NextSubCheckpoint(subSyncState SubSyncState, persistData any) {
	if subSyncState == IngestBinlog {
		j.IngestBinlogAt = time.Now().Unix()
	}

	j.SubSyncState = subSyncState

	j.PersistData = _convertToPersistData(persistData)

	// TODO: check
	j.Persist()
}

func (j *JobProgress) CommitNextSubWithPersist(commitSeq int64, subSyncState SubSyncState, persistData any) {
	j.CommitSeq = commitSeq
	j.SubSyncState = subSyncState

	j.PersistData = _convertToPersistData(persistData)

	// TODO: check
	j.Persist()
}

// Switch to new sync state.
//
// The PrevCommitSeq is set to commitSeq, if the sub sync state is done.
func (j *JobProgress) NextWithPersist(commitSeq int64, syncState SyncState, subSyncState SubSyncState, persistData string) {
	if subSyncState == BeginCreateSnapshot && (syncState == TableFullSync || syncState == DBFullSync) {
		j.FullSyncStartAt = time.Now().Unix()
		j.IncrementalSyncStartAt = 0
		j.IngestBinlogAt = 0
	} else if subSyncState == Done && (syncState == TableIncrementalSync || syncState == DBIncrementalSync) {
		j.IncrementalSyncStartAt = time.Now().Unix()
		j.IngestBinlogAt = 0
	}

	j.CommitSeq = commitSeq
	if subSyncState == Done {
		j.PrevCommitSeq = commitSeq
	}

	j.SyncState = syncState
	j.SubSyncState = subSyncState
	j.PersistData = persistData
	j.InMemoryData = nil

	j.Persist()
}

func (j *JobProgress) IsDone() bool { return j.SubSyncState == Done && j.PrevCommitSeq == j.CommitSeq }

// TODO(Drogon): check reset some fields
func (j *JobProgress) Done() {
	log.Debugf("job %s step next", j.JobName)

	j.SubSyncState = Done
	j.PrevCommitSeq = j.CommitSeq

	xmetrics.ConsumeBinlog(j.JobName, j.PrevCommitSeq)

	j.Persist()
}

func (j *JobProgress) Rollback(skipError bool) {
	log.Debugf("job %s step rollback", j.JobName)

	j.SubSyncState = Done
	// if rollback, then prev commit seq is the last commit seq
	// but if skip error, we can consume the binlog then prev commit seq is the last commit seq
	if !skipError {
		j.CommitSeq = j.PrevCommitSeq
	}

	xmetrics.Rollback(j.JobName, j.PrevCommitSeq)
	j.Persist()
}

// write progress to db, busy loop until success
// TODO: add timeout check
func (j *JobProgress) Persist() {
	log.Trace("update job progress")

	for {
		// Step 1: to json
		// TODO: fix to json error
		jsonBytes, err := json.Marshal(j)
		if err != nil {
			log.Errorf("parse job progress failed, error: %+v", err)
			time.Sleep(UPDATE_JOB_PROGRESS_DURATION)
			continue
		}

		// Step 2: write to db
		err = j.db.UpdateProgress(j.JobName, string(jsonBytes))
		if err != nil {
			log.Errorf("update job progress failed, error: %+v", err)
			time.Sleep(UPDATE_JOB_PROGRESS_DURATION)
			continue
		}

		break
	}

	log.Tracef("update job progress done, state: %s, subState: %s, commitSeq: %d, prevCommitSeq: %d",
		j.SyncState, j.SubSyncState, j.CommitSeq, j.PrevCommitSeq)
}
