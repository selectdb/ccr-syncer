package record

import (
	"encoding/json"
	"fmt"
)

type PartitionRecord struct {
	PartitionID int64 `json:"partitionId"`
	Version     int64 `json:"version"`
}

func (p PartitionRecord) String() string {
	return fmt.Sprintf("PartitionRecord{PartitionID: %d, Version: %d}", p.PartitionID, p.Version)
}

type TableRecord struct {
	PartitionRecords []PartitionRecord `json:"partitionRecords"`
}

func (t TableRecord) String() string {
	return fmt.Sprintf("TableRecord{PartitionRecords: %v}", t.PartitionRecords)
}

type Upsert struct {
	CommitSeq    int64                 `json:"commitSeq"`
	TxnID        int64                 `json:"txnId"`
	TimeStamp    int64                 `json:"timeStamp"`
	Label        string                `json:"label"`
	DbID         int64                 `json:"dbId"`
	TableRecords map[int64]TableRecord `json:"tableRecords"`
}

// Stringer
func (u Upsert) String() string {
	return fmt.Sprintf("Upsert{CommitSeq: %d, TxnID: %d, TimeStamp: %d, Label: %s, DbID: %d, TableRecords: %v}", u.CommitSeq, u.TxnID, u.TimeStamp, u.Label, u.DbID, u.TableRecords)
}

//	{
//	  "commitSeq": 949780,
//	  "txnId": 18019,
//	  "timeStamp": 1687676101779,
//	  "label": "insert_334a873c523741cd_a1d6f371e6bc4514",
//	  "dbId": 10116,
//	  "tableRecords": {
//	    "21012": {
//	      "partitionRecords": [
//	        {
//	          "partitionId": 21011,
//	          "version": 9
//	        }
//	      ]
//	    }
//	  }
//	}
func NewUpsertFromJson(data string) (*Upsert, error) {
	var up Upsert
	err := json.Unmarshal([]byte(data), &up)
	if err != nil {
		return nil, err
	}
	return &up, nil
}
