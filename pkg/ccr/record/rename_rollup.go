package record

import (
	"encoding/json"
	"fmt"

	"github.com/selectdb/ccr_syncer/pkg/xerror"
)

type RenameRollup struct {
	DbId          int64  `json:"db"`
	TableId       int64  `json:"tb"`
	IndexId       int64  `json:"ind"`
	NewRollupName string `json:"nR"`
	OldRollupName string `json:"oR"`
}

func NewRenameRollupFromJson(data string) (*RenameRollup, error) {
	var record RenameRollup
	err := json.Unmarshal([]byte(data), &record)
	if err != nil {
		return nil, xerror.Wrap(err, xerror.Normal, "unmarshal rename rollup record error")
	}

	if record.TableId == 0 {
		return nil, xerror.Errorf(xerror.Normal, "rename rollup record table id not found")
	}

	if record.NewRollupName == "" {
		return nil, xerror.Errorf(xerror.Normal, "rename rollup record old rollup name not found")
	}

	return &record, nil
}

// Stringer
func (r *RenameRollup) String() string {
	return fmt.Sprintf("RenameRollup: DbId: %d, TableId: %d, IndexId: %d, NewRollupName: %s, OldRollupName: %s",
		r.DbId, r.TableId, r.IndexId, r.NewRollupName, r.OldRollupName)
}
