package ccr

import (
	"encoding/json"
	"io"
	"reflect"
	"testing"

	"github.com/selectdb/ccr_syncer/storage"

	log "github.com/sirupsen/logrus"
)

func init() {
	log.SetOutput(io.Discard)
}

func TestJobProgress_MarshalJSON(t *testing.T) {
	type fields struct {
		JobName           string
		db                storage.DB
		SyncState         SyncState
		SubSyncState      SubSyncState
		PrevCommitSeq     int64
		CommitSeq         int64
		TransactionId     int64
		TableCommitSeqMap map[int64]int64
		InMemoryData      any
		PersistData       string
	}
	tests := []struct {
		name    string
		fields  fields
		want    []byte
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
			},
			want:    []byte(`{"job_name":"test-job","sync_state":500,"sub_sync_state":{"state":0,"binlog_type":-1},"prev_commit_seq":0,"commit_seq":1,"table_commit_seq_map":{"1":2},"data":"test-data"}`),
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
			}
			got, err := json.Marshal(jp)
			if (err != nil) != tt.wantErr {
				t.Errorf("JobProgress.MarshalJSON() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
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
