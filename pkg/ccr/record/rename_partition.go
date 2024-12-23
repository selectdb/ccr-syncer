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

type RenamePartition struct {
	DbId             int64  `json:"db"`
	TableId          int64  `json:"tb"`
	PartitionId      int64  `json:"p"`
	NewPartitionName string `json:"nP"`
	OldPartitionName string `json:"oP"`
}

func NewRenamePartitionFromJson(data string) (*RenamePartition, error) {
	var rename RenamePartition
	err := json.Unmarshal([]byte(data), &rename)
	if err != nil {
		return nil, xerror.Wrap(err, xerror.Normal, "unmarshal rename partition record error")
	}

	if rename.TableId == 0 {
		return nil, xerror.Errorf(xerror.Normal, "rename partition record table id not found")
	}

	if rename.PartitionId == 0 {
		return nil, xerror.Errorf(xerror.Normal, "rename partition record partition id not found")
	}

	if rename.NewPartitionName == "" {
		return nil, xerror.Errorf(xerror.Normal, "rename partition record new partition name not found")
	}

	return &rename, nil
}

// Stringer
func (r *RenamePartition) String() string {
	return fmt.Sprintf("RenamePartition: DbId: %d, TableId: %d, PartitionId: %d, NewPartitionName: %s, OldPartitionName: %s",
		r.DbId, r.TableId, r.PartitionId, r.NewPartitionName, r.OldPartitionName)
}
