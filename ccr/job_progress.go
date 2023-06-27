package ccr

import "encoding/json"

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
	SyncState     SyncState `json:"sync_state"`
	JobState      JobState  `json:"state"`
	CommitSeq     int64     `json:"commit_seq"`
	TransactionId int64     `json:"transaction_id"`
	Data          string    `json:"data"` // this often for binlog or snapshot info
}

func NewJobProgress() *JobProgress {
	return &JobProgress{
		SyncState:     FullSync,
		JobState:      JobStateDone,
		CommitSeq:     0,
		TransactionId: 0,
	}
}

// create JobProgress from json data
func NewJobProgressFromJson(jsonData string) (*JobProgress, error) {
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
}

func (j *JobProgress) DoneCreateSnapshot(snapshotName string) {
	j.JobState = JobStateFullSync_DoneCreateSnapshot
	j.Data = snapshotName
}

func (j *JobProgress) BeginRestore(commitSeq int64) {
	j.JobState = JobStateFullSync_BeginRestore
	j.CommitSeq = commitSeq
}

func (j *JobProgress) NewIncrementalSync() {
	j.SyncState = IncrementalSync
}

func (j *JobProgress) Done() {
	j.JobState = JobStateDone
}
