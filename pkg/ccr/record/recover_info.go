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

type RecoverInfo struct {
	DbId             int64  `json:"dbId"`
	NewDbName        string `json:"newDbName"`
	TableId          int64  `json:"tableId"`
	TableName        string `json:"tableName"`
	NewTableName     string `json:"newTableName"`
	PartitionId      int64  `json:"partitionId"`
	PartitionName    string `json:"partitionName"`
	NewPartitionName string `json:"newPartitionName"`
}

func NewRecoverInfoFromJson(data string) (*RecoverInfo, error) {
	var recoverInfo RecoverInfo
	err := json.Unmarshal([]byte(data), &recoverInfo)
	if err != nil {
		return nil, xerror.Wrap(err, xerror.Normal, "unmarshal create table error")
	}

	if recoverInfo.TableId == 0 {
		return nil, xerror.Errorf(xerror.Normal, "table id not found")
	}

	// table name must exist. partition name not checked since optional.
	if recoverInfo.TableName == "" {
		return nil, xerror.Errorf(xerror.Normal, "Table Name can not be null")
	}
	return &recoverInfo, nil
}

func (c *RecoverInfo) IsRecoverTable() bool {
	if c.PartitionName == "" || c.PartitionId == -1 {
		return true
	}
	return false
}

// String
func (c *RecoverInfo) String() string {
	return fmt.Sprintf("RecoverInfo: DbId: %d, NewDbName: %s, TableId: %d, TableName: %s, NewTableName: %s, PartitionId: %d, PartitionName: %s, NewPartitionName: %s",
		c.DbId, c.NewDbName, c.TableId, c.TableName, c.NewTableName, c.PartitionId, c.PartitionName, c.NewPartitionName)
}
