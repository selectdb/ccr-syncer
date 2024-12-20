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
package ccr_test

import (
	"testing"

	"github.com/selectdb/ccr_syncer/pkg/ccr"
)

func TestIsSessionVariableRequired(t *testing.T) {
	tests := []string{
		"If you want to specify column names, please `set enable_nereids_planner=true`",
		"set enable_variant_access_in_original_planner = true in session variable",
		"Please enable the session variable 'enable_projection' through `set enable_projection = true",
		"agg state not enable, need set enable_agg_state=true",
		"which is greater than 38 is disabled by default. set enable_decimal256 = true to enable it",
		"if we have a column with decimalv3 type and set enable_decimal_conversion = false",
		"Incorrect column name '名称'. Column regex is '^[_a-zA-Z@0-9\\s/][.a-zA-Z0-9_+-/?@#$%^&*\"\\s,:]{0,255}$'",
	}
	for i, test := range tests {
		if !ccr.IsSessionVariableRequired(test) {
			t.Errorf("test %d failed, input: %s", i, test)
		}
	}
}

func TestFilterStorageMediumFromCreateTableSql(t *testing.T) {
	type TestCase struct {
		origin, expect string
	}
	tests := []TestCase{
		{
			origin: "CREATE TABLE `test` ( `id` INT(11) NOT NULL, `name` VARCHAR(255) NOT NULL, `storage_medium` VARCHAR(255) NOT NULL ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 10 PROPERTIES (\"replication_allocation\" = \"tag.location.default: 1\", \"storage_medium\" = \"SSD\", \"is_being_synced\" = \"true\")",
			expect: "CREATE TABLE `test` ( `id` INT(11) NOT NULL, `name` VARCHAR(255) NOT NULL, `storage_medium` VARCHAR(255) NOT NULL ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 10 PROPERTIES (\"replication_allocation\" = \"tag.location.default: 1\", \"is_being_synced\" = \"true\")",
		},
	}
	for i, test := range tests {
		sql := ccr.FilterStorageMediumFromCreateTableSql(test.origin)
		if sql != test.expect {
			t.Errorf("test %d failed, expect %s, but got %s", i, test.expect, sql)
		}
	}
}
