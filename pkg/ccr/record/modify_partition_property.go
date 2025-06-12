package record

import (
	"encoding/json"
	"fmt"

	"github.com/selectdb/ccr_syncer/pkg/xerror"
)

type DataProperty struct {
	StorageMedium          string `json:"storageMedium"`
	CooldownTimeMs         int64  `json:"cooldownTimeMs"`
	StoragePolicy          string `json:"storagePolicy"`
	IsMutable              bool   `json:"isMutable"`
	StorageMediumSpecified bool   `json:"storageMediumSpecified,omitempty"`
}

type Tag struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

type ReplicaAllocation struct {
	AllocMap map[string]int16 `json:"allocMap"`
}

// ModifyPartitionInfo represents single partition modification info
type ModifyPartitionInfo struct {
	DbId            int64              `json:"dbId"`
	TableId         int64              `json:"tableId"`
	PartitionId     int64              `json:"partitionId"`
	DataProperty    *DataProperty      `json:"dataProperty"`
	ReplicationNum  int16              `json:"replicationNum"`
	IsInMemory      bool               `json:"isInMemory"`
	ReplicaAlloc    *ReplicaAllocation `json:"replicaAlloc"`
	StoragePolicy   string             `json:"storagePolicy"`
	TblProperties   map[string]string  `json:"tableProperties"`
	PartitionName   string             `json:"partitionName,omitempty"`
	IsTempPartition bool               `json:"isTempPartition,omitempty"`
}

type BatchModifyPartitionsInfo struct {
	Infos []*ModifyPartitionInfo `json:"infos"`
}

func (batchModifyPartitionsInfo *BatchModifyPartitionsInfo) Deserialize(data string) error {
	err := json.Unmarshal([]byte(data), &batchModifyPartitionsInfo)
	if err != nil {
		return xerror.Wrap(err, xerror.Normal, "unmarshal batch modify partitions info error")
	}

	if len(batchModifyPartitionsInfo.Infos) == 0 {
		return xerror.Errorf(xerror.Normal, "modify partition infos is empty")
	}

	return nil
}

func NewBatchModifyPartitionsInfoFromJson(data string) (*BatchModifyPartitionsInfo, error) {
	var batchModifyPartitionsInfo BatchModifyPartitionsInfo
	if err := batchModifyPartitionsInfo.Deserialize(data); err != nil {
		return nil, err
	}
	return &batchModifyPartitionsInfo, nil
}

func (batchModifyPartitionsInfo *BatchModifyPartitionsInfo) String() string {
	return fmt.Sprintf("BatchModifyPartitionsInfo: Infos count: %d", len(batchModifyPartitionsInfo.Infos))
}

// GetTableId implements Record interface by returning the first table ID in the batch
func (batchModifyPartitionsInfo *BatchModifyPartitionsInfo) GetTableId() int64 {
	if len(batchModifyPartitionsInfo.Infos) == 0 {
		return -1
	}
	return batchModifyPartitionsInfo.Infos[0].TableId
}
