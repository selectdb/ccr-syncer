package record

import (
	"encoding/json"
	"fmt"

	"github.com/selectdb/ccr_syncer/pkg/xerror"
)

type RenamePartition struct {
	DbId             int64  `json:"db"`
	TableId          int64  `json:"tb"`
	PartitionId      int64  `json:"p"`
	NewPartitionName string `json:"nP"`
	OldPartitionName string `json:"oP"`
}

func NewRenamePartitionFromJson(data string) (*RenamePartition, error) {
	var rename RenamePartition
	err := json.Unmarshal([]byte(data), &rename)
	if err != nil {
		return nil, xerror.Wrap(err, xerror.Normal, "unmarshal rename partition record error")
	}

	if rename.TableId == 0 {
		return nil, xerror.Errorf(xerror.Normal, "rename partition record table id not found")
	}

	if rename.PartitionId == 0 {
		return nil, xerror.Errorf(xerror.Normal, "rename partition record partition id not found")
	}

	if rename.NewPartitionName == "" {
		return nil, xerror.Errorf(xerror.Normal, "rename partition record new partition name not found")
	}

	return &rename, nil
}

// Stringer
func (r *RenamePartition) String() string {
	return fmt.Sprintf("RenamePartition: DbId: %d, TableId: %d, PartitionId: %d, NewPartitionName: %s, OldPartitionName: %s",
		r.DbId, r.TableId, r.PartitionId, r.NewPartitionName, r.OldPartitionName)
}
