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
// under the License.

suite("test_tsa_alt_prop_skip_bitmap_column") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def dbName = context.dbName
    def tableName = "tbl_" + helper.randomSuffix()
    def aliasTableName = "tbl_alias_" + helper.randomSuffix()
    helper.set_alias(aliasTableName)
    def test_num = 0
    def insert_num = 5

    def exist = { res -> Boolean
        return res.size() != 0
    }

    def notEnableSkip = { res -> Boolean
        return !res[0][1].contains("\"enable_unique_key_skip_bitmap_column\" = \"true\"")
    }

    def enableSkip = { res -> Boolean
        return res[0][1].contains("\"enable_unique_key_skip_bitmap_column\" = \"true\"")
    }

    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
    target_sql "DROP TABLE IF EXISTS TEST_${dbName}.${tableName}"

    helper.enableDbBinlog()

    sql """
        CREATE TABLE ${tableName} 
        (
            `k` int(11) NULL, 
            `v1` BIGINT NULL,
            `v2` BIGINT NULL DEFAULT "9876",
            `v3` BIGINT NOT NULL,
            `v4` BIGINT NOT NULL DEFAULT "1234",
            `v5` BIGINT NULL
        ) 
        UNIQUE KEY(`k`) 
        DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "enable_unique_key_skip_bitmap_column" = "false",
            "light_schema_change" = "true",
            "store_row_column" = "false",
            "binlog.enable" = "true"
        );
    """

    helper.ccrJobDelete(tableName)
    helper.ccrJobCreate(tableName)

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 30))

    logger.info("=== Test 1: check property not exist ===")

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}\"", exist, 60, "sql"))

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${aliasTableName}\"", exist, 60, "target"))

    assertTrue(helper.checkShowTimesOf("SHOW CREATE TABLE ${tableName}", notEnableSkip, 60, "sql"))

    assertTrue(helper.checkShowTimesOf("SHOW CREATE TABLE ${aliasTableName}", notEnableSkip, 60, "target"))

    logger.info("=== Test 2: alter table enable property enable_unique_key_merge_on_write ===")

    sql """
        ALTER TABLE ${tableName} ENABLE FEATURE "UPDATE_FLEXIBLE_COLUMNS";
        """

    logger.info("=== Test 3: check property exist ===")

    assertTrue(helper.checkShowTimesOf("SHOW CREATE TABLE ${tableName}", enableSkip, 60, "sql"))

    assertTrue(helper.checkShowTimesOf("SHOW COLUMNS FROM ${aliasTableName} WHERE Field = \"__DORIS_SKIP_BITMAP_COL__\"", exist, 30, "target"))
}