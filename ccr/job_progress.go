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
	JobStateDoing JobState = 0
	JobStateDone  JobState = 1

	JobStateFullSync_BeginCreateSnapshot JobState = 30
	JobStateFullSync_DoneCreateSnapshot  JobState = 31
	JobStateFullSync_BeginRestore        JobState = 32
)

type SyncState int

const (
	FullSync        SyncState = 0
	IncrementalSync SyncState = 1
)

type JobProgress struct {
	JobName       string     `json:"job_name"`
	SyncState     SyncState  `json:"sync_state"`
	JobState      JobState   `json:"state"`
	CommitSeq     int64      `json:"commit_seq"`
	TransactionId int64      `json:"transaction_id"`
	Data          string     `json:"data"` // this often for binlog or snapshot info
	db            storage.DB `json:"-"`
}

func NewJobProgress(jobName string, db storage.DB) *JobProgress {
	return &JobProgress{
		JobName:       jobName,
		SyncState:     FullSync,
		JobState:      JobStateDone,
		CommitSeq:     0,
		TransactionId: 0,
		db:            db,
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

func (j *JobProgress) BeginCreateSnapshot() {
	j.SyncState = FullSync
	j.JobState = JobStateFullSync_BeginCreateSnapshot
	j.CommitSeq = 0

	j.persist()
}

func (j *JobProgress) DoneCreateSnapshot(snapshotName string) {
	j.JobState = JobStateFullSync_DoneCreateSnapshot
	j.Data = snapshotName

	j.persist()
}

func (j *JobProgress) BeginRestore(commitSeq int64) {
	j.JobState = JobStateFullSync_BeginRestore
	j.CommitSeq = commitSeq

	j.persist()
}

func (j *JobProgress) NewIncrementalSync() {
	j.SyncState = IncrementalSync

	j.persist()
}

func (j *JobProgress) StartDeal(commitSeq int64) {
	j.JobState = JobStateDoing
	j.CommitSeq = commitSeq
	j.TransactionId = 0

	j.persist()
}

func (j *JobProgress) Done() {
	j.JobState = JobStateDone

	j.persist()
}

func (j *JobProgress) BeginTransaction(txnId int64) {
	j.TransactionId = txnId

	j.persist()
}

// write progress to db, busy loop until success
// TODO: add timeout check
func (j *JobProgress) persist() {
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
