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
package base_test

import (
	"testing"

	"github.com/selectdb/ccr_syncer/pkg/ccr/base"
)

func TestAddDBPrefixToCreateTableOrViewSql(t *testing.T) {
	type TestCase struct {
		origin, expect string
	}

	testCases := []TestCase{
		{"CREATE VIEW `v` AS SELECT * FROM t", "CREATE VIEW `target_db`.`v` AS SELECT * FROM t"},
		{"CREATE VIEW `target_db`.`v` AS SELECT * FROM t", "CREATE VIEW `target_db`.`v` AS SELECT * FROM t"},
		{" CREATE VIEW `v` AS SELECT * FROM t", "CREATE VIEW `target_db`.`v` AS SELECT * FROM t"},
		{"CREATE TABLE `v` (...", "CREATE TABLE `target_db`.`v` (..."},
		{"CREATE VIEW `view_test_746794472` AS SELECT `internal`.`TEST_regression_test_db_sync_mv_basic$.`tbl_duplicate_0_746794472`.`user_id` AS `k1`, `internal`.`TEST_regression_test_db_sync_mv_basic`.`tbl_duplicate_0_746794472`.`name` AS `name`, SUM(`internal`.`TEST_regression_te$t_db_sync_mv_basic`.`tbl_duplicate_0_746794472`.`age`) AS `v1` FROM `internal`.`TEST_regression_test_db_sync_mv_basic`.`tbl_duplicate_0_746794472` GROUP BY k1,`internal`.`TEST_regression_test_db_sync_mv_basic`.`tbl_duplicate_0_746794472`.`name`",
			"CREATE VIEW `target_db`.`view_test_746794472` AS SELECT `internal`.`TEST_regression_test_db_sync_mv_basic$.`tbl_duplicate_0_746794472`.`user_id` AS `k1`, `internal`.`TEST_regression_test_db_sync_mv_basic`.`tbl_duplicate_0_746794472`.`name` AS `name`, SUM(`internal`.`TEST_regression_te$t_db_sync_mv_basic`.`tbl_duplicate_0_746794472`.`age`) AS `v1` FROM `internal`.`TEST_regression_test_db_sync_mv_basic`.`tbl_duplicate_0_746794472` GROUP BY k1,`internal`.`TEST_regression_test_db_sync_mv_basic`.`tbl_duplicate_0_746794472`.`name`"},
	}

	for i, c := range testCases {
		if actual := base.AddDBPrefixToCreateTableOrViewSql("target_db", c.origin); actual != c.expect {
			t.Errorf("case %d failed, expect %s, but got %s", i, c.expect, actual)
		}
	}
}

func TestReplaceAndEscapeComment(t *testing.T) {
	type TestCase struct {
		origin, expect string
	}

	testCases := []TestCase{
		{"CREATE TABLE `t` (\n  `test` int NOT NULL COMMENT '[\"0000-01-01\", \"9999-12-31\"]'", "CREATE TABLE `t` (\n  `test` int NOT NULL COMMENT \"[\\\"0000-01-01\\\", \\\"9999-12-31\\\"]\""},
		{"CREATE TABLE `t` (\n  `test` int NOT NULL COMMENT 'xxx\"test\"'", "CREATE TABLE `t` (\n  `test` int NOT NULL COMMENT \"xxx\\\"test\\\"\""},
		{"CREATE TABLE `t` (\n  `test1` int NOT NULL COMMENT 'xxx\"test1\"', `test2` int NOT NULL COMMENT 'xxx\"test2\"'", "CREATE TABLE `t` (\n  `test1` int NOT NULL COMMENT \"xxx\\\"test1\\\"\", `test2` int NOT NULL COMMENT \"xxx\\\"test2\\\"\""},
	}

	for i, c := range testCases {
		if actual := base.ReplaceAndEscapeComment(c.origin); actual != c.expect {
			t.Errorf("case %d failed, expect %s, but got %s", i, c.expect, actual)
		}
	}
}
