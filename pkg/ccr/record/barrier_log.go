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

	"github.com/selectdb/ccr_syncer/pkg/xerror"
)

type BarrierLog struct {
	DbId       int64  `json:"dbId"`
	TableId    int64  `json:"tableId"`
	BinlogType int64  `json:"binlogType"`
	Binlog     string `json:"binlog"`
}

func NewBarrierLogFromJson(data string) (*BarrierLog, error) {
	var log BarrierLog
	err := json.Unmarshal([]byte(data), &log)
	if err != nil {
		return nil, xerror.Wrap(err, xerror.Normal, "unmarshal barrier log error")
	}
	return &log, nil
}
