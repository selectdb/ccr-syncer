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
package ccr

import (
	"fmt"
	"time"
)

// snapshot name format "ccrs_${ccr_name}_${sync_id}"
func NewSnapshotLabelPrefix(ccrName string, syncId int64) string {
	return fmt.Sprintf("ccrs_%s_%d", ccrName, syncId)
}

// snapshot name format "ccrp_${ccr_name}_${sync_id}"
func NewPartialSnapshotLabelPrefix(ccrName string, syncId int64) string {
	return fmt.Sprintf("ccrp_%s_%d", ccrName, syncId)
}

func NewLabelWithTs(prefix string) string {
	return fmt.Sprintf("%s_%d", prefix, time.Now().Unix())
}

func NewRestoreLabel(snapshotName string) string {
	if snapshotName == "" {
		return ""
	}

	// use current seconds
	return fmt.Sprintf("%s_r_%d", snapshotName, time.Now().Unix())
}

func TableAlias(tableName string) string {
	return fmt.Sprintf("__ccr_%s_%d", tableName, time.Now().Unix())
}
