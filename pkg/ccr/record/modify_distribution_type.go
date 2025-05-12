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

type ModifyDistributionType struct {
	DbId    int64 `json:"db"`
	TableId int64 `json:"tb"`
}

func (r *ModifyDistributionType) Deserialize(data string) error {
	err := json.Unmarshal([]byte(data), &r)
	if err != nil {
		return xerror.Wrap(err, xerror.Normal, "unmarshal rename table error")
	}

	if r.TableId == 0 {
		return xerror.Errorf(xerror.Normal, "table id not found")
	}

	return nil
}

func NewModifyDistributionTypeFromJson(data string) (*ModifyDistributionType, error) {
	var modifyDistributionType ModifyDistributionType
	if err := modifyDistributionType.Deserialize(data); err != nil {
		return nil, err
	}
	return &modifyDistributionType, nil
}

// Stringer
func (r *ModifyDistributionType) String() string {
	return fmt.Sprintf("RenameTable: DbId: %d, TableId: %d", r.DbId, r.TableId)
}

func (record *ModifyDistributionType) GetTableId() int64 {
	return record.TableId
}
