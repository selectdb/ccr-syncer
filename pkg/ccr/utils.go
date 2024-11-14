package ccr

import (
	"encoding/json"

	"github.com/selectdb/ccr_syncer/pkg/xerror"
)

type BackupViewInfo struct {
	Id   int64  `json:"id"`
	Name string `json:"name"`
}

type BackupOlapTableInfo struct {
	Id int64 `json:"id"`
}

type NewBackupObject struct {
	Views []BackupViewInfo `json:"views"`
}

type BackupJobInfo struct {
	TableCommitSeqMap map[int64]int64                `json:"table_commit_seq_map"`
	BackupObjects     map[string]BackupOlapTableInfo `json:"backup_objects"`
	NewBackupObjects  *NewBackupObject               `json:"new_backup_objects"`
}

func NewBackupJobInfoFromJson(data []byte) (*BackupJobInfo, error) {
	jobInfo := &BackupJobInfo{}
	if err := json.Unmarshal(data, &jobInfo); err != nil {
		return nil, xerror.Wrapf(err, xerror.Normal, "unmarshal job info error: %v", err)
	}
	return jobInfo, nil
}

func (i *BackupJobInfo) TableNameMapping() map[int64]string {
	tableMapping := make(map[int64]string)
	for tableName, tableInfo := range i.BackupObjects {
		tableMapping[tableInfo.Id] = tableName
	}
	return tableMapping
}

// Get the table id by table name, return -1 if not found
func (i *BackupJobInfo) TableId(name string) int64 {
	if tableInfo, ok := i.BackupObjects[name]; ok {
		return tableInfo.Id
	}

	return -1
}

func (i *BackupJobInfo) Views() []string {
	if i.NewBackupObjects == nil {
		return []string{}
	}

	views := make([]string, 0)
	for _, viewInfo := range i.NewBackupObjects.Views {
		views = append(views, viewInfo.Name)
	}
	return views
}
