// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License
package record

import (
	"encoding/json"
	"fmt"

	"github.com/selectdb/ccr_syncer/pkg/xerror"
)

const (
	ALTER_JOB_SCHEMA_CHANGE = "SCHEMA_CHANGE"
	ALTER_JOB_ROLLUP        = "ROLLUP"

	ALTER_JOB_STATE_PENDING     = "PENDING"
	ALTER_JOB_STATE_WAITING_TXN = "WAITING_TXN"
	ALTER_JOB_STATE_RUNNING     = "RUNNING"
	ALTER_JOB_STATE_FINISHED    = "FINISHED"
	ALTER_JOB_STATE_CANCELLED   = "CANCELLED"
)

type AlterJobV2 struct {
	Type          string          `json:"type"`
	DbId          int64           `json:"dbId"`
	TableId       int64           `json:"tableId"`
	TableName     string          `json:"tableName"`
	JobId         int64           `json:"jobId"`
	JobState      string          `json:"jobState"`
	RawSql        string          `json:"rawSql"`
	ShadowIndexes map[int64]int64 `json:"iim"`

	// for rollup
	RollupIndexId   int64  `json:"rollupIndexId"`
	RollupIndexName string `json:"rollupIndexName"`
	BaseIndexId     int64  `json:"baseIndexId"`
	BaseIndexName   string `json:"baseIndexName"`
}

func NewAlterJobV2FromJson(data string) (*AlterJobV2, error) {
	var alterJob AlterJobV2
	err := json.Unmarshal([]byte(data), &alterJob)
	if err != nil {
		return nil, xerror.Wrap(err, xerror.Normal, "unmarshal alter job error")
	}

	// rollup not contain RawSql
	// if alterJob.RawSql == "" {
	// 	// TODO: fallback to create sql from other fields
	// 	return nil, xerror.Errorf(xerror.Normal, "alter job raw sql is empty")
	// }

	if alterJob.TableId == 0 {
		return nil, xerror.Errorf(xerror.Normal, "invalid alter job, table id not found")
	}

	if alterJob.TableName == "" {
		return nil, xerror.Errorf(xerror.Normal, "invalid alter job, tableName is empty")
	}

	return &alterJob, nil
}

func (a *AlterJobV2) IsFinished() bool {
	return a.JobState == ALTER_JOB_STATE_FINISHED
}

// Stringer
func (a *AlterJobV2) String() string {
	return fmt.Sprintf("AlterJobV2: DbId: %d, TableId: %d, TableName: %s, JobId: %d, JobState: %s, RawSql: %s",
		a.DbId, a.TableId, a.TableName, a.JobId, a.JobState, a.RawSql)
}
