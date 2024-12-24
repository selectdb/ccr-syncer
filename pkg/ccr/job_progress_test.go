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
package ccr

import (
	"encoding/json"
	"io"
	"reflect"
	"testing"

	"github.com/selectdb/ccr_syncer/pkg/storage"
	log "github.com/sirupsen/logrus"
)

func init() {
	log.SetOutput(io.Discard)
}

func deepEqual(got, expect string) bool {
	var v1, v2 interface{}
	err := json.Unmarshal([]byte(got), &v1)
	if err != nil {
		return false
	}

	err = json.Unmarshal([]byte(expect), &v2)
	if err != nil {
		return false
	}
	return reflect.DeepEqual(v1, v2)
}

func TestJobProgress_MarshalJSON(t *testing.T) {
	type fields struct {
		JobName           string
		db                storage.DB
		SyncState         SyncState
		SubSyncState      SubSyncState
		PrevCommitSeq     int64
		CommitSeq         int64
		TableMapping      map[int64]int64
		TransactionId     int64
		TableCommitSeqMap map[int64]int64
		InMemoryData      any
		PersistData       string
		TableAliases      map[string]string
	}
	tests := []struct {
		name    string
		fields  fields
		want    string
		wantErr bool
	}{
		{
			name: "marshal job progress",
			fields: fields{
				JobName:           "test-job",
				db:                nil,
				SyncState:         TableFullSync,
				SubSyncState:      BeginCreateSnapshot,
				PrevCommitSeq:     0,
				CommitSeq:         1,
				TableCommitSeqMap: map[int64]int64{1: 2},
				InMemoryData:      nil,
				PersistData:       "test-data",
				TableAliases:      map[string]string{"table": "alias"},
			},
			want: `{
  "job_name": "test-job",
  "sync_state": 500,
  "sub_sync_state": {
    "state": 0,
    "binlog_type": -1
  },
  "job_sync_id":0,
  "prev_commit_seq": 0,
  "commit_seq": 1,
  "table_mapping": null,
  "table_commit_seq_map": {
    "1": 2
  },
  "data": "test-data",
  "table_aliases": {
    "table": "alias"
  }
}`,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			jp := &JobProgress{
				JobName:           tt.fields.JobName,
				db:                tt.fields.db,
				SyncState:         tt.fields.SyncState,
				SubSyncState:      tt.fields.SubSyncState,
				PrevCommitSeq:     tt.fields.PrevCommitSeq,
				CommitSeq:         tt.fields.CommitSeq,
				TableCommitSeqMap: tt.fields.TableCommitSeqMap,
				InMemoryData:      tt.fields.InMemoryData,
				PersistData:       tt.fields.PersistData,
				TableAliases:      tt.fields.TableAliases,
			}
			got, err := json.Marshal(jp)
			if (err != nil) != tt.wantErr {
				t.Errorf("JobProgress.MarshalJSON() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !deepEqual(string(got), tt.want) {
				t.Errorf("JobProgress.MarshalJSON() = %v, want %v", string(got), string(tt.want))
			}
		})
	}
}

func TestJobProgress_UnmarshalJSON(t *testing.T) {
	type fields struct {
		JobName           string
		db                storage.DB
		SyncState         SyncState
		SubSyncState      SubSyncState
		CommitSeq         int64
		TransactionId     int64
		TableCommitSeqMap map[int64]int64
		InMemoryData      any
		PersistData       string
	}
	type args struct {
		data []byte
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "unmarshal job progress",
			fields: fields{
				JobName:           "test-job",
				db:                nil,
				SyncState:         TableFullSync,
				SubSyncState:      BeginCreateSnapshot,
				CommitSeq:         1,
				TransactionId:     2,
				TableCommitSeqMap: map[int64]int64{1: 2},
				InMemoryData:      nil,
				PersistData:       "test-data",
			},
			args: args{
				data: []byte(`{"job_name":"test-job","sync_state":500,"sub_sync_state":{"state":0,"binlog_type":-1},"commit_seq":1,"transaction_id":2,"table_commit_seq_map":{"1":2},"data":"test-data"}`),
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			jp := &JobProgress{
				JobName:           tt.fields.JobName,
				db:                tt.fields.db,
				SyncState:         tt.fields.SyncState,
				SubSyncState:      tt.fields.SubSyncState,
				CommitSeq:         tt.fields.CommitSeq,
				TableCommitSeqMap: tt.fields.TableCommitSeqMap,
				InMemoryData:      tt.fields.InMemoryData,
				PersistData:       tt.fields.PersistData,
			}
			if err := json.Unmarshal(tt.args.data, jp); (err != nil) != tt.wantErr {
				t.Errorf("JobProgress.UnmarshalJSON() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
