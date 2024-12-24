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

type ReplaceTableRecord struct {
	DbId            int64  `json:"dbId"`
	OriginTableId   int64  `json:"origTblId"`
	OriginTableName string `json:"origTblName"`
	NewTableId      int64  `json:"newTblName"`
	NewTableName    string `json:"actualNewTblName"`
	SwapTable       bool   `json:"swapTable"`
	IsForce         bool   `json:"isForce"`
}

func NewReplaceTableRecordFromJson(data string) (*ReplaceTableRecord, error) {
	record := &ReplaceTableRecord{}
	err := json.Unmarshal([]byte(data), record)
	if err != nil {
		return nil, xerror.Wrap(err, xerror.Normal, "unmarshal replace table record error")
	}

	if record.OriginTableId == 0 {
		return nil, xerror.Errorf(xerror.Normal, "table id of replace table record not found")
	}

	if record.OriginTableName == "" {
		return nil, xerror.Errorf(xerror.Normal, "table name of replace table record not found")
	}

	if record.NewTableId == 0 {
		return nil, xerror.Errorf(xerror.Normal, "new table id of replace table record not found")
	}

	if record.NewTableName == "" {
		return nil, xerror.Errorf(xerror.Normal, "new table name of replace table record not found")
	}

	return record, nil
}

// Stringer
func (r *ReplaceTableRecord) String() string {
	return fmt.Sprintf("ReplaceTableRecord: DbId: %d, OriginTableId: %d, OriginTableName: %s, NewTableId: %d, NewTableName: %s, SwapTable: %v, IsForce: %v",
		r.DbId, r.OriginTableId, r.OriginTableName, r.NewTableId, r.NewTableName, r.SwapTable, r.IsForce)
}
