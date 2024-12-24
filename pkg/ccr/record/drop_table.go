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

type DropTable struct {
	DbId      int64  `json:"dbId"`
	TableId   int64  `json:"tableId"`
	TableName string `json:"tableName"`
	IsView    bool   `json:"isView"`
	RawSql    string `json:"rawSql"`
}

func NewDropTableFromJson(data string) (*DropTable, error) {
	var dropTable DropTable
	err := json.Unmarshal([]byte(data), &dropTable)
	if err != nil {
		return nil, xerror.Wrap(err, xerror.Normal, "unmarshal drop table error")
	}

	if dropTable.TableId == 0 {
		return nil, xerror.Errorf(xerror.Normal, "table id not found")
	}

	return &dropTable, nil
}

// Stringer, all fields
func (c *DropTable) String() string {
	return fmt.Sprintf("DropTable: DbId: %d, TableId: %d, TableName: %s, IsView: %t, RawSql: %s", c.DbId, c.TableId, c.TableName, c.IsView, c.RawSql)
}
