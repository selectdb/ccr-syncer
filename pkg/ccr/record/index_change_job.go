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

	"github.com/selectdb/ccr_syncer/pkg/xerror"
)

const (
	INDEX_CHANGE_JOB_STATE_RUNNING     = "RUNNING"
	INDEX_CHANGE_JOB_STATE_FINISHED    = "FINISHED"
	INDEX_CHANGE_JOB_STATE_CANCELLED   = "CANCELLED"
	INDEX_CHANGE_JOB_STATE_WAITING_TXN = "WATING_TXN"
)

type IndexChangeJob struct {
	DbId           int64   `json:"dbId"`
	TableId        int64   `json:"tableId"`
	TableName      string  `json:"tableName"`
	PartitionId    int64   `json:"partitionId"`
	PartitionName  string  `json:"partitionName"`
	JobState       string  `json:"jobState"`
	ErrMsg         string  `json:"errMsg"`
	CreateTimeMs   int64   `json:"createTimeMs"`
	FinishedTimeMs int64   `json:"finishedTimeMs"`
	IsDropOp       bool    `json:"isDropOp"`
	OriginIndexId  int64   `json:"originIndexId"`
	TimeoutMs      int64   `json:"timeoutMs"`
	Indexes        []Index `json:"alterInvertedIndexes"`
}

func NewIndexChangeJobFromJson(data string) (*IndexChangeJob, error) {
	m := &IndexChangeJob{}
	if err := json.Unmarshal([]byte(data), m); err != nil {
		return nil, xerror.Wrap(err, xerror.Normal, "unmarshal index change job error")
	}

	if m.TableId == 0 {
		return nil, xerror.Errorf(xerror.Normal, "index change job table id not found")
	}

	if m.PartitionId == 0 {
		return nil, xerror.Errorf(xerror.Normal, "index change job partition id not found")
	}

	if m.JobState == "" {
		return nil, xerror.Errorf(xerror.Normal, "index change job state not found")
	}

	if len(m.Indexes) == 0 {
		return nil, xerror.Errorf(xerror.Normal, "index change job alter inverted indexes is empty")
	}

	if !m.IsDropOp && len(m.Indexes) != 1 {
		return nil, xerror.Errorf(xerror.Normal, "index change job alter inverted indexes length is not 1")
	}

	return m, nil
}
