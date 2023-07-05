package record

import (
	"encoding/json"
	"fmt"
)

type AlterJobV2 struct {
	Type      string `json:"type"`
	DbId      int64  `json:"dbId"`
	TableId   int64  `json:"tableId"`
	TableName string `json:"tableName"`
	State     string `json:"state"`
	JobId     int64  `json:"jobId"`
	JobState  string `json:"jobState"`
	RawSql    string `json:"rawSql"`
}

func NewAlterJobV2FromJson(data string) (*AlterJobV2, error) {
	var alterJob AlterJobV2
	err := json.Unmarshal([]byte(data), &alterJob)
	if err != nil {
		return nil, err
	}

	if alterJob.RawSql == "" {
		// TODO: fallback to create sql from other fields
		return nil, fmt.Errorf("alter job raw sql is empty")
	}

	if alterJob.TableId == 0 {
		return nil, fmt.Errorf("table id not found")
	}

	return &alterJob, nil
}

// String
func (c *AlterJobV2) String() string {
	return fmt.Sprintf("AlterJobV2: DbId: %d, TableId: %d, TableName: %s, State: %s, JobId: %d, JobState: %s, RawSql: %s",
		c.DbId, c.TableId, c.TableName, c.State, c.JobId, c.JobState, c.RawSql)
}
