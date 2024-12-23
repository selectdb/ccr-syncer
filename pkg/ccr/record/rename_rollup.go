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

type RenameRollup struct {
	DbId          int64  `json:"db"`
	TableId       int64  `json:"tb"`
	IndexId       int64  `json:"ind"`
	NewRollupName string `json:"nR"`
	OldRollupName string `json:"oR"`
}

func NewRenameRollupFromJson(data string) (*RenameRollup, error) {
	var record RenameRollup
	err := json.Unmarshal([]byte(data), &record)
	if err != nil {
		return nil, xerror.Wrap(err, xerror.Normal, "unmarshal rename rollup record error")
	}

	if record.TableId == 0 {
		return nil, xerror.Errorf(xerror.Normal, "rename rollup record table id not found")
	}

	if record.NewRollupName == "" {
		return nil, xerror.Errorf(xerror.Normal, "rename rollup record old rollup name not found")
	}

	return &record, nil
}

// Stringer
func (r *RenameRollup) String() string {
	return fmt.Sprintf("RenameRollup: DbId: %d, TableId: %d, IndexId: %d, NewRollupName: %s, OldRollupName: %s",
		r.DbId, r.TableId, r.IndexId, r.NewRollupName, r.OldRollupName)
}
