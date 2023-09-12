package ccr

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/selectdb/ccr_syncer/storage"
	log "github.com/sirupsen/logrus"
	"go.uber.org/zap"
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

	// Table sync state machine states
	TableFullSync        SyncState = 500
	TableIncrementalSync SyncState = 501

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
	case TableFullSync:
		return "TableFullSync"
	case TableIncrementalSync:
		return "TableIncrementalSync"
	default:
		return fmt.Sprintf("Unknown SyncState: %d", s)
	}
}

type SubSyncState int

const (
	/// Sub Sync States
	Done SubSyncState = -1

	// DB/Table FullSync state machine states
	BeginCreateSnapshot SubSyncState = 0
	GetSnapshotInfo     SubSyncState = 1
	AddExtraInfo        SubSyncState = 2
	RestoreSnapshot     SubSyncState = 3
	PersistRestoreInfo  SubSyncState = 4

	// IncrementalSync state machine states

	DB_1 SubSyncState = 10
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
	default:
		return fmt.Sprintf("Unknown SubSyncState: %d", s)
	}
}

type JobProgress struct {
	JobName string     `json:"job_name"`
	db      storage.DB `json:"-"`

	// Table/DB big sync state machine states
	SyncState SyncState `json:"sync_state"`
	// Sub sync state machine states
	SubSyncState SubSyncState `json:"sub_sync_state"`

	CommitSeq         int64           `json:"commit_seq"`
	TransactionId     int64           `json:"transaction_id"`
	TableCommitSeqMap map[int64]int64 `json:"table_commit_seq_map"` // only for DBTablesIncrementalSync
	InMemoryData      any             `json:"-"`
	PersistData       string          `json:"data"` // this often for binlog or snapshot info
}

func (j *JobProgress) String() string {
	return fmt.Sprintf("JobProgress{JobName: %s, SyncState: %s, SubSyncState: %s, CommitSeq: %d, TransactionId: %d, TableCommitSeqMap: %v, InMemoryData: %v, PersistData: %s}", j.JobName, j.SyncState, j.SubSyncState, j.CommitSeq, j.TransactionId, j.TableCommitSeqMap, j.InMemoryData, j.PersistData)
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

		SyncState:     syncState,
		SubSyncState:  BeginCreateSnapshot,
		CommitSeq:     0,
		TransactionId: 0,

		TableCommitSeqMap: nil,
		InMemoryData:      nil,
		PersistData:       "",
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
			log.Error("get job progress failed", zap.String("job", jobName), zap.Error(err))
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

// ToJson
func (j *JobProgress) ToJson() (string, error) {
	if jsonData, err := json.Marshal(j); err != nil {
		return "", err
	} else {
		return string(jsonData), nil
	}
}

// TODO: Add api, begin/commit/abort

// func (j *JobProgress) BeginCreateSnapshot() {
// 	j.CommitSeq = 0
// 	j.JobState = BeginCreateSnapshot

// 	j.persist()
// }

func (j *JobProgress) DoneCreateSnapshot(snapshotName string) {
	j.PersistData = snapshotName

	j.Persist()
}

func (j *JobProgress) StartHandle(commitSeq int64) {
	j.CommitSeq = commitSeq
	j.TransactionId = 0

	j.Persist()
}

func (j *JobProgress) BeginTransaction(txnId int64) {
	j.TransactionId = txnId

	j.Persist()
}

// write progress to db, busy loop until success
// TODO: add timeout check
func (j *JobProgress) Persist() {
	log.Debugf("update job progress: %s", j)

	for {
		// Step 1: to json
		// TODO: fix to json error
		progressJson, err := j.ToJson()
		if err != nil {
			log.Error("parse job progress failed", zap.String("job", j.JobName), zap.Error(err))
			time.Sleep(UPDATE_JOB_PROGRESS_DURATION)
			continue
		}

		// Step 2: write to db
		err = j.db.UpdateProgress(j.JobName, progressJson)
		if err != nil {
			log.Error("update job progress failed", zap.String("job", j.JobName), zap.Error(err))
			time.Sleep(UPDATE_JOB_PROGRESS_DURATION)
			continue
		}

		break
	}

	log.Debugf("update job progress done: %s", j)
}

// func (j *JobProgress) Done() {
// 	switch j.SyncState {
// 	case DBFullSync:
// 		switch j.SubSyncState {
// 		case DBFullSync_BeginCreateSnapshot:
// 		case DBFullSync_DoneCreateSnapshot:
// 		}
// 	case DBTablesIncrementalSync:
// 	case DBSpecificTableFullSync:
// 	case DBIncrementalSync:

// 	case TableFullSync:
// 	case TableIncrementalSync:
// 	}
// }

func (j *JobProgress) Commit() {
	j.Done()
	// j.persist()
}

func (j *JobProgress) NextSub(subSyncState SubSyncState, inMemoryData any) {
	j.SubSyncState = subSyncState
	j.InMemoryData = inMemoryData
}

// Persist is checkpint, next state only get it from persistData
func (j *JobProgress) NextSubWithPersist(subSyncState SubSyncState, persistData string) {
	j.SubSyncState = subSyncState
	j.PersistData = persistData

	// TODO: check
	j.Persist()
}

func (j *JobProgress) CommitNextSubWithPersist(commitSeq int64, subSyncState SubSyncState, persistData any) {
	j.CommitSeq = commitSeq
	j.SubSyncState = subSyncState

	persistDataJson, err := json.Marshal(persistData)
	if err != nil {
		log.Panicf("marshal persist data failed: %v", err)
	}

	j.PersistData = string(persistDataJson)

	// TODO: check
	j.Persist()
}

func (j *JobProgress) NextWithPersist(commitSeq int64, syncState SyncState, subSyncState SubSyncState, persistData string) {
	j.CommitSeq = commitSeq
	j.SyncState = syncState
	j.SubSyncState = subSyncState
	j.PersistData = persistData
	j.InMemoryData = nil

	j.Persist()
}

// FIXME
func (j *JobProgress) Done() {
	// j.JobState = JobStateDone

	j.Persist()
}
