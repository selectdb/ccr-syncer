package record

import (
	"encoding/json"
	"strings"

	"github.com/selectdb/ccr_syncer/pkg/xerror"
)

type ModifyTableAddOrDropInvertedIndices struct {
	DbId                int64   `json:"dbId"`
	TableId             int64   `json:"tableId"`
	IsDropInvertedIndex bool    `json:"isDropInvertedIndex"`
	RawSql              string  `json:"rawSql"`
	Indexes             []Index `json:"indexes"`
	AlternativeIndexes  []Index `json:"alterInvertedIndexes"`
}

func NewModifyTableAddOrDropInvertedIndicesFromJson(data string) (*ModifyTableAddOrDropInvertedIndices, error) {
	m := &ModifyTableAddOrDropInvertedIndices{}
	if err := json.Unmarshal([]byte(data), m); err != nil {
		return nil, xerror.Wrap(err, xerror.Normal, "unmarshal modify table add or drop inverted indices error")
	}

	if m.RawSql == "" {
		// TODO: fallback to create sql from other fields
		return nil, xerror.Errorf(xerror.Normal, "modify table add or drop inverted indices sql is empty")
	}

	if m.TableId == 0 {
		return nil, xerror.Errorf(xerror.Normal, "modify table add or drop inverted indices table id not found")
	}

	return m, nil
}

func (m *ModifyTableAddOrDropInvertedIndices) GetRawSql() string {
	if strings.Contains(m.RawSql, "ALTER TABLE") && strings.Contains(m.RawSql, "INDEX") &&
		!strings.Contains(m.RawSql, "DROP INDEX") && !strings.Contains(m.RawSql, "ADD INDEX") {
		// fix the syntax error
		// See apache/doris#44392 for details
		return strings.ReplaceAll(m.RawSql, "INDEX", "ADD INDEX")
	}
	return m.RawSql
}
