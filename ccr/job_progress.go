package ccr

import "encoding/json"

type JobState int

const (
	JobStateDoing JobState = 0
	JobStateDone  JobState = 1
)

type JobProgress struct {
	State         JobState `json:"state"`
	CommitSeq     int64    `json:"commit_seq"`
	TransactionId int64    `json:"transaction_id"`
}

func NewJobProgress() *JobProgress {
	return &JobProgress{
		State:         JobStateDone,
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
