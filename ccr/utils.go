package ccr

import "encoding/json"

func ExtractTableCommitSeqMap(data []byte) (map[int64]int64, error) {
	type JobInfo struct {
		TableCommitSeqMap map[int64]int64 `json:"table_commit_seq_map"`
	}
	var jobInfo JobInfo
	err := json.Unmarshal(data, &jobInfo)
	if err != nil {
		return nil, err
	}
	return jobInfo.TableCommitSeqMap, nil
}
