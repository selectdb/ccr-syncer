package record

import (
	"encoding/json"
	"fmt"

	"github.com/selectdb/ccr_syncer/pkg/xerror"
)

type ModifyComment struct {
	Type         string            `json:"type"`
	DbId         int64             `json:"dbId"`
	TblId        int64             `json:"tblId"`
	ColToComment map[string]string `json:"colToComment"`
	TblComment   string            `json:"tblComment"`
}

func NewModifyCommentFromJson(data string) (*ModifyComment, error) {
	var modifyComment ModifyComment
	err := json.Unmarshal([]byte(data), &modifyComment)
	if err != nil {
		return nil, xerror.Wrap(err, xerror.Normal, "unmarshal modify comment error")
	}

	if modifyComment.TblId == 0 {
		return nil, xerror.Errorf(xerror.Normal, "table id not found")
	}

	return &modifyComment, nil
}

// Stringer
func (r *ModifyComment) String() string {
	return fmt.Sprintf("ModifyComment: Type: %s, DbId: %d, TblId: %d, ColToComment: %v, TblComment: %s", r.Type, r.DbId, r.TblId, r.ColToComment, r.TblComment)
}
