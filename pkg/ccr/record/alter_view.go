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
)

type AlterView struct {
	DbId          int64  `json:"dbId"`
	TableId       int64  `json:"tableId"`
	InlineViewDef string `json:"inlineViewDef"`
	SqlMode       int64  `json:"sqlMode"`
}

func NewAlterViewFromJson(data string) (*AlterView, error) {
	var alterView AlterView
	err := json.Unmarshal([]byte(data), &alterView)
	if err != nil {
		return nil, fmt.Errorf("unmarshal alter view error: %v", err)
	}

	if alterView.TableId == 0 {
		return nil, fmt.Errorf("table id not found")
	}

	return &alterView, nil
}

func (a *AlterView) String() string {
	return fmt.Sprintf("AlterView: DbId: %d, TableId: %d, InlineViewDef: %s, SqlMode: %d", a.DbId, a.TableId, a.InlineViewDef, a.SqlMode)
}
