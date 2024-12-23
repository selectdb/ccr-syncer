// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License
package record

import (
	"encoding/json"
	"fmt"

	"github.com/selectdb/ccr_syncer/pkg/xerror"
)

type RenameColumn struct {
	DbId                   int64           `json:"dbId"`
	TableId                int64           `json:"tableId"`
	ColName                string          `json:"colName"`
	NewColName             string          `json:"newColName"`
	IndexIdToSchemaVersion map[int64]int32 `json:"indexIdToSchemaVersion"`
}

func NewRenameColumnFromJson(data string) (*RenameColumn, error) {
	var renameColumn RenameColumn
	err := json.Unmarshal([]byte(data), &renameColumn)
	if err != nil {
		return nil, xerror.Wrap(err, xerror.Normal, "unmarshal rename column error")
	}

	if renameColumn.TableId == 0 {
		return nil, xerror.Errorf(xerror.Normal, "table id not found")
	}

	return &renameColumn, nil
}

// Stringer
func (r *RenameColumn) String() string {
	return fmt.Sprintf("RenameColumn: DbId: %d, TableId: %d, ColName: %s, NewColName: %s, IndexIdToSchemaVersion: %v", r.DbId, r.TableId, r.ColName, r.NewColName, r.IndexIdToSchemaVersion)
}
