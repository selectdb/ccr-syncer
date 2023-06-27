package ccr

import "encoding/json"

type JobState int

const (
	JobStateDoing JobState = 0
	JobStateDone  JobState = 1
)

type SyncState int

const (
	FullSync        SyncState = 0
	IncrementalSync SyncState = 1
)

type JobProgress struct {
	JobState      JobState  `json:"state"`
	SyncState     SyncState `json:"sync_state"`
	CommitSeq     int64     `json:"commit_seq"`
	TransactionId int64     `json:"transaction_id"`
	Data          []byte    `json:"data"` // this often for binlog or snapshot info
}

func NewJobProgress() *JobProgress {
	return &JobProgress{
		JobState:      JobStateDone,
		SyncState:     FullSync,
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

func (j *JobProgress) NewFullSync(commitSeq int64) {
	j.JobState = JobStateDoing
	j.SyncState = FullSync
	j.CommitSeq = commitSeq
}

func (j *JobProgress) Done() {
	j.JobState = JobStateDone
}

func (j *JobProgress) NewIncrementalSync() {
	j.SyncState = IncrementalSync
}
