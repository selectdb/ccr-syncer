// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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

type RenameTable struct {
	DbId             int64  `json:"db"`
	TableId          int64  `json:"tb"`
	IndexId          int64  `json:"ind"`
	PartitionId      int64  `json:"p"`
	NewTableName     string `json:"nT"`
	OldTableName     string `json:"oT"`
	NewRollupName    string `json:"nR"`
	OldRollupName    string `json:"oR"`
	NewPartitionName string `json:"nP"`
	OldPartitionName string `json:"oP"`
}

func NewRenameTableFromJson(data string) (*RenameTable, error) {
	var renameTable RenameTable
	err := json.Unmarshal([]byte(data), &renameTable)
	if err != nil {
		return nil, xerror.Wrap(err, xerror.Normal, "unmarshal rename table error")
	}

	if renameTable.TableId == 0 {
		return nil, xerror.Errorf(xerror.Normal, "table id not found")
	}

	return &renameTable, nil
}

// Stringer
func (r *RenameTable) String() string {
	return fmt.Sprintf("RenameTable: DbId: %d, TableId: %d, PartitionId: %d, IndexId: %d, NewTableName: %s, OldTableName: %s, NewRollupName: %s, OldRollupName: %s, NewPartitionName: %s, OldPartitionName: %s", r.DbId, r.TableId, r.PartitionId, r.IndexId, r.NewTableName, r.OldTableName, r.NewRollupName, r.OldRollupName, r.NewPartitionName, r.OldPartitionName)
}
