package ccr

import (
	"encoding/json"
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

type JobState int

const (
	JobStatePrepare JobState = 0
	JobStateCommit  JobState = 1
	JobStateAbort   JobState = 2

	JobStateFullSync_DoneCreateSnapshot JobState = 31
	JobStateFullSync_BeginRestore       JobState = 32
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

type SubSyncState int

const (
	/// Sub Sync States
	// DB/Table FullSync state machine states
	BeginCreateSnapshot SubSyncState = 20
	GetSnapshotInfo     SubSyncState = 21
	AddExtraInfo        SubSyncState = 22
	RestoreSnapshot     SubSyncState = 23
	PersistRestoreInfo  SubSyncState = 24
)

type JobProgress struct {
	JobName string     `json:"job_name"`
	db      storage.DB `json:"-"`

	JobState      JobState     `json:"state"`
	SyncState     SyncState    `json:"sync_state"`
	SubSyncState  SubSyncState `json:"sub_sync_state"`
	CommitSeq     int64        `json:"commit_seq"`
	TransactionId int64        `json:"transaction_id"`
	InMemoryData  any          `json:"-"`
	PersistData   string       `json:"data"` // this often for binlog or snapshot info
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

		JobState:      JobStateCommit,
		SyncState:     syncState,
		SubSyncState:  BeginCreateSnapshot,
		CommitSeq:     0,
		TransactionId: 0,

		InMemoryData: nil,
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
// 	j.SyncState = DBFullSync
// 	j.JobState = JobStateFullSync_BeginCreateSnapshot
// 	j.CommitSeq = 0

// 	j.persist()
// }

func (j *JobProgress) DoneCreateSnapshot(snapshotName string) {
	j.JobState = JobStateFullSync_DoneCreateSnapshot
	j.PersistData = snapshotName

	j.Persist()
}

func (j *JobProgress) NewIncrementalSync() {
	j.SyncState = TableIncrementalSync

	j.Persist()
}

func (j *JobProgress) StartHandle(commitSeq int64) {
	j.JobState = JobStatePrepare
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
	log.Tracef("update job progress: %v", j)
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
	log.Tracef("update job progress done: %v", j)
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

func (j *JobProgress) NextWithPersist(syncState SyncState, subSyncState SubSyncState, persistData string) {
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
