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

type ModifyComment struct {
	Type         string            `json:"type"`
	DbId         int64             `json:"dbId"`
	TblId        int64             `json:"tblId"`
	ColToComment map[string]string `json:"colToComment"`
	TblComment   string            `json:"tblComment"`
}

func NewModifyCommentFromJson(data string) (*ModifyComment, error) {
	var modifyComment ModifyComment
	err := json.Unmarshal([]byte(data), &modifyComment)
	if err != nil {
		return nil, xerror.Wrap(err, xerror.Normal, "unmarshal modify comment error")
	}

	if modifyComment.TblId == 0 {
		return nil, xerror.Errorf(xerror.Normal, "table id not found")
	}

	return &modifyComment, nil
}

// Stringer
func (r *ModifyComment) String() string {
	return fmt.Sprintf("ModifyComment: Type: %s, DbId: %d, TblId: %d, ColToComment: %v, TblComment: %s", r.Type, r.DbId, r.TblId, r.ColToComment, r.TblComment)
}
