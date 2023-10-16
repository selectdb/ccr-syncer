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
		return nil, xerror.Wrapf(err, xerror.Normal, "unmarshal job info error: %v", err)
	}
	return jobInfo.TableCommitSeqMap, nil
}
