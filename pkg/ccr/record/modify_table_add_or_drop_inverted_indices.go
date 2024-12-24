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
	"strings"

	"github.com/selectdb/ccr_syncer/pkg/xerror"
)

type ModifyTableAddOrDropInvertedIndices struct {
	DbId                int64   `json:"dbId"`
	TableId             int64   `json:"tableId"`
	IsDropInvertedIndex bool    `json:"isDropInvertedIndex"`
	RawSql              string  `json:"rawSql"`
	Indexes             []Index `json:"indexes"`
	AlternativeIndexes  []Index `json:"alterInvertedIndexes"`
}

func NewModifyTableAddOrDropInvertedIndicesFromJson(data string) (*ModifyTableAddOrDropInvertedIndices, error) {
	m := &ModifyTableAddOrDropInvertedIndices{}
	if err := json.Unmarshal([]byte(data), m); err != nil {
		return nil, xerror.Wrap(err, xerror.Normal, "unmarshal modify table add or drop inverted indices error")
	}

	if m.RawSql == "" {
		// TODO: fallback to create sql from other fields
		return nil, xerror.Errorf(xerror.Normal, "modify table add or drop inverted indices sql is empty")
	}

	if m.TableId == 0 {
		return nil, xerror.Errorf(xerror.Normal, "modify table add or drop inverted indices table id not found")
	}

	return m, nil
}

func (m *ModifyTableAddOrDropInvertedIndices) GetRawSql() string {
	if strings.Contains(m.RawSql, "ALTER TABLE") && strings.Contains(m.RawSql, "INDEX") &&
		!strings.Contains(m.RawSql, "DROP INDEX") && !strings.Contains(m.RawSql, "ADD INDEX") {
		// fix the syntax error
		// See apache/doris#44392 for details
		return strings.ReplaceAll(m.RawSql, "INDEX", "ADD INDEX")
	}
	return m.RawSql
}
