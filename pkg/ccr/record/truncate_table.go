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

// {
//   "dbId": 10079,
//   "db": "default_cluster:ccr", # "default_cluster:" prefix will be removed in Doris v2.1
//   "tblId": 77395,
//   "table": "src_1_alias",
//   "isEntireTable": false,
//   "rawSql": "PARTITIONS (src_1_alias)"
// }

type TruncateTable struct {
	DbId          int64  `json:"dbId"`
	DbName        string `json:"db"`
	TableId       int64  `json:"tblId"`
	TableName     string `json:"table"`
	IsEntireTable bool   `json:"isEntireTable"`
	RawSql        string `json:"rawSql"`
}

func NewTruncateTableFromJson(data string) (*TruncateTable, error) {
	var truncateTable TruncateTable
	err := json.Unmarshal([]byte(data), &truncateTable)
	if err != nil {
		return nil, xerror.Wrap(err, xerror.Normal, "unmarshal truncate table error")
	}

	if truncateTable.TableId == 0 {
		return nil, xerror.Errorf(xerror.Normal, "table id not found")
	}

	return &truncateTable, nil
}

// Stringer
func (t *TruncateTable) String() string {
	return fmt.Sprintf("TruncateTable: DbId: %d, Db: %s, TableId: %d, Table: %s, IsEntireTable: %v, RawSql: %s", t.DbId, t.DbName, t.TableId, t.TableName, t.IsEntireTable, t.RawSql)
}
