package record

import (
	"encoding/json"

	"github.com/selectdb/ccr_syncer/pkg/xerror"
)

type RestoreInfo struct {
	DbId      int64            `json:"dbId"`
	DbName    string           `json:"dbName"`
	TableInfo map[int64]string `json:"tableInfo"`
}

func NewRestoreInfoFromJson(data string) (*RestoreInfo, error) {
	var restoreInfo RestoreInfo
	err := json.Unmarshal([]byte(data), &restoreInfo)
	if err != nil {
		return nil, xerror.Wrap(err, xerror.Normal, "unmarshal create table error")
	}

	if restoreInfo.DbId == 0 {
		return nil, xerror.Errorf(xerror.Normal, "db id not found")
	}
	return &restoreInfo, nil
}
