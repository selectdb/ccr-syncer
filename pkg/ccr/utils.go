package ccr

import (
	"encoding/json"

	"github.com/selectdb/ccr_syncer/pkg/xerror"
)

func ExtractTableCommitSeqMap(data []byte) (map[int64]int64, error) {
	type JobInfo struct {
		TableCommitSeqMap map[int64]int64 `json:"table_commit_seq_map"`
	}
	var jobInfo JobInfo

	if err := json.Unmarshal(data, &jobInfo); err != nil {
		return nil, xerror.Wrapf(err, xerror.Normal, "unmarshal job info for extracting table commit seq map error: %v", err)
	}
	return jobInfo.TableCommitSeqMap, nil
}

func ExtractTableMapping(data []byte) (map[int64]string, error) {
	type BackupOlapTableInfo struct {
		Id int64 `json:"id"`
	}
	type JobInfo struct {
		BackupObjects map[string]BackupOlapTableInfo `json:"backup_objects"`
	}

	var jobInfo JobInfo
	if err := json.Unmarshal(data, &jobInfo); err != nil {
		return nil, xerror.Wrapf(err, xerror.Normal, "unmarshal job info for extracting table mapping error: %v", err)
	}

	tableMapping := make(map[int64]string)
	for tableName, tableInfo := range jobInfo.BackupObjects {
		tableMapping[tableInfo.Id] = tableName
	}
	return tableMapping, nil
}