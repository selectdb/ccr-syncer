package record

import (
	"encoding/json"
	"fmt"

	"github.com/selectdb/ccr_syncer/pkg/xerror"
)

type PartitionRecord struct {
	Id      int64  `json:"partitionId"`
	Range   string `json:"range"`
	Version int64  `json:"version"`
	IsTemp  bool   `json:"isTempPartition"`
	Stid    int64  `json:stid`
}

func (p PartitionRecord) String() string {
	return fmt.Sprintf("PartitionRecord{Id: %d, Range: %s, Version: %d}", p.Id, p.Range, p.Version)
}

type TableRecord struct {
	Id               int64             `json:"_"`
	PartitionRecords []PartitionRecord `json:"partitionRecords"`
	IndexIds         []int64           `json:"indexIds"`
}

func (t TableRecord) String() string {
	return fmt.Sprintf("TableRecord{Id: %d, PartitionRecords: %v, IndexIds: %v}", t.Id, t.PartitionRecords, t.IndexIds)
}

type Upsert struct {
	CommitSeq    int64                  `json:"commitSeq"`
	TxnID        int64                  `json:"txnId"`
	TimeStamp    int64                  `json:"timeStamp"`
	Label        string                 `json:"label"`
	DbID         int64                  `json:"dbId"`
	TableRecords map[int64]*TableRecord `json:"tableRecords"`
	Stids        []int64                `json:"stids"`
}

// Stringer
func (u Upsert) String() string {
	return fmt.Sprintf("Upsert{CommitSeq: %d, TxnID: %d, TimeStamp: %d, Label: %s, DbID: %d, TableRecords: %v, Stids: %v}", u.CommitSeq, u.TxnID, u.TimeStamp, u.Label, u.DbID, u.TableRecords, u.Stids)
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
		return nil, xerror.Wrap(err, xerror.Normal, "unmarshal upsert error")
	}

	for tableId, tableRecord := range up.TableRecords {
		tableRecord.Id = tableId
	}

	return &up, nil
}
