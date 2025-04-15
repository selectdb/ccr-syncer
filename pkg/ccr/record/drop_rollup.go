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

type DropRollup struct {
	DbId      int64  `json:"dbId"`
	TableId   int64  `json:"tableId"`
	TableName string `json:"tableName"`
	IndexId   int64  `json:"indexId"`
	IndexName string `json:"indexName"`
}

func NewDropRollupFromJson(data string) (*DropRollup, error) {
	var dropRollup DropRollup
	if err := dropRollup.Deserialize(data); err != nil {
		return nil, err
	}
	return &dropRollup, nil
}

func (dropRollup *DropRollup) Deserialize(data string) error {
	err := json.Unmarshal([]byte(data), &dropRollup)
	if err != nil {
		return xerror.Wrap(err, xerror.Normal, "unmarshal drop rollup error")
	}

	if dropRollup.TableId == 0 {
		return xerror.Errorf(xerror.Normal, "invalid drop rollup, table id not found")
	}

	if dropRollup.TableName == "" {
		return xerror.Errorf(xerror.Normal, "invalid drop rollup, tableName is empty")
	}

	if dropRollup.IndexName == "" {
		return xerror.Errorf(xerror.Normal, "invalid drop rollup, indexName is empty")
	}

	return nil
}

func (dropRollup *DropRollup) GetTableId() int64 {
	return dropRollup.TableId
}

func (d *DropRollup) String() string {
	return fmt.Sprintf("DropRollup{DbId: %d, TableId: %d, TableName: %s, IndexId: %d, IndexName: %s}",
		d.DbId, d.TableId, d.TableName, d.IndexId, d.IndexName)
}
