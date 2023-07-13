package ccr

import (
	"encoding/json"

	"github.com/pkg/errors"
)

func ExtractTableCommitSeqMap(data []byte) (map[int64]int64, error) {
	type JobInfo struct {
		TableCommitSeqMap map[int64]int64 `json:"table_commit_seq_map"`
	}
	var jobInfo JobInfo

	if err := json.Unmarshal(data, &jobInfo); err != nil {
		return nil, errors.Wrapf(err, "unmarshal job info error: %v", err)
	}
	return jobInfo.TableCommitSeqMap, nil
}
