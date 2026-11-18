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

type ModifyDistributionBucketNum struct {
	DbId        int64  `json:"dbId"`
	TableId     int64  `json:"tableId"`
	Type        string `json:"Type"`
	AutoBucket  bool   `json:"autoBucket"`
	BucketNum   int    `json:"bucketNum"`
	ColumnsName string `json:"columnsName"`
}

func (modifyDistributionBucketNum *ModifyDistributionBucketNum) Deserialize(data string) error {
	err := json.Unmarshal([]byte(data), &modifyDistributionBucketNum)
	if err != nil {
		return xerror.Wrap(err, xerror.Normal, "unmarshal modify distribution bucket num error")
	}

	if modifyDistributionBucketNum.Type != "RANDOM" && modifyDistributionBucketNum.Type != "HASH" {
		return xerror.Errorf(xerror.Normal, "distribution type can not recognize")
	}

	if modifyDistributionBucketNum.TableId == 0 {
		return xerror.Errorf(xerror.Normal, "table id not found")
	}

	if modifyDistributionBucketNum.BucketNum <= 0 {
		return xerror.Errorf(xerror.Normal, "bucket num is invalid")
	}
	return nil
}

func NewModifyDistributionBucketNumFromJson(data string) (*ModifyDistributionBucketNum, error) {
	var modifyDistributionBucketNum ModifyDistributionBucketNum
	if err := modifyDistributionBucketNum.Deserialize(data); err != nil {
		return nil, err
	}
	return &modifyDistributionBucketNum, nil
}

// String
func (c *ModifyDistributionBucketNum) String() string {
	return fmt.Sprintf("ModifyDistributionBucketNum: DbId: %d, TableId: %d, Type: %s, AutoBucket: %v, BucketNum: %d, ColumnsName: %s",
		c.DbId, c.TableId, c.Type, c.AutoBucket, c.BucketNum, c.ColumnsName)
}

func (record *ModifyDistributionBucketNum) GetTableId() int64 {
	return record.TableId
}
