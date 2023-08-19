package record

import (
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"
)

type AlterJobV2 struct {
	Type      string `json:"type"`
	DbId      int64  `json:"dbId"`
	TableId   int64  `json:"tableId"`
	TableName string `json:"tableName"`
	JobId     int64  `json:"jobId"`
	JobState  string `json:"jobState"`
	RawSql    string `json:"rawSql"`
}

func NewAlterJobV2FromJson(data string) (*AlterJobV2, error) {
	var alterJob AlterJobV2
	err := json.Unmarshal([]byte(data), &alterJob)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshal alter job error")
	}

	// rollup not contain RawSql
	// if alterJob.RawSql == "" {
	// 	// TODO: fallback to create sql from other fields
	// 	return nil, errors.Errorf("alter job raw sql is empty")
	// }

	if alterJob.TableId == 0 {
		return nil, errors.Errorf("table id not found")
	}

	return &alterJob, nil
}

func (a *AlterJobV2) IsFinished() bool {
	return a.JobState == "FINISHED"
}

// String
func (a *AlterJobV2) String() string {
	return fmt.Sprintf("AlterJobV2: DbId: %d, TableId: %d, TableName: %s, JobId: %d, JobState: %s, RawSql: %s",
		a.DbId, a.TableId, a.TableName, a.JobId, a.JobState, a.RawSql)
}
