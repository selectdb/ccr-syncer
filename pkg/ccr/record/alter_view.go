package record

import (
	"encoding/json"
	"fmt"
)

type AlterView struct {
	DbId          int64  `json:"dbId"`
	TableId       int64  `json:"tableId"`
	InlineViewDef string `json:"inlineViewDef"`
	SqlMode       int64  `json:"sqlMode"`
}

func NewAlterViewFromJson(data string) (*AlterView, error) {
	var alterView AlterView
	err := json.Unmarshal([]byte(data), &alterView)
	if err != nil {
		return nil, fmt.Errorf("unmarshal alter view error: %v", err)
	}

	if alterView.TableId == 0 {
		return nil, fmt.Errorf("table id not found")
	}

	return &alterView, nil
}

func (a *AlterView) String() string {
	return fmt.Sprintf("AlterView: DbId: %d, TableId: %d, InlineViewDef: %s, SqlMode: %d", a.DbId, a.TableId, a.InlineViewDef, a.SqlMode)
}
