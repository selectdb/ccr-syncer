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
suite("test_ts_rollup_col_add") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def tableName = "tbl_" + helper.randomSuffix()
    def test_num = 0
    def insert_num = 5

    def exist = { res -> Boolean
        return res.size() != 0
    }

    def has_count = { count ->
        return { res -> Boolean
            res.size() == count
        }
    }

    sql """
        CREATE TABLE if NOT EXISTS ${tableName}
        (
            `id` INT,
            `col1` INT,
            `col2` INT,
            `col3` INT,
            `col4` INT,
        )
        ENGINE=OLAP
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
    """
    sql """
        ALTER TABLE ${tableName}
        ADD ROLLUP rollup_${tableName} (id, col2, col4)
        """

    assertTrue(helper.checkShowTimesOf("""
                                SHOW ALTER TABLE ROLLUP
                                FROM ${context.dbName}
                                WHERE TableName = "${tableName}" AND State = "FINISHED"
                                """,
                                has_count(1), 30))

    helper.ccrJobCreate(tableName)
    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 30))
    assertTrue(helper.check_table_describe_times(tableName, 30))

    def first_job_progress = helper.get_job_progress(tableName)

    logger.info("=== Test 1: add key column ===")
    // {
    //   "type": "SCHEMA_CHANGE",
    //   "dbId": 10273,
    //   "tableId": 10485,
    //   "tableName": "tbl_848588167",
    //   "jobId": 10527,
    //   "jobState": "FINISHED",
    //   "rawSql": "ALTER TABLE `regression_test_table_sync_rollup_col_add`.`tbl_848588167` ADD COLUMN `key` int NULL DEFAULT \"0\" COMMENT \"\" IN `rollup_tbl_848588167`",
    //   "iim": {
    //     "10528": 10486,
    //     "10533": 10492
    //   }
    // }
    sql """
        ALTER TABLE ${tableName}
        ADD COLUMN `key` INT KEY DEFAULT "0"
        TO rollup_${tableName}
        """
    sql "sync"

    assertTrue(helper.checkShowTimesOf("""
                                SHOW ALTER TABLE COLUMN
                                FROM ${context.dbName}
                                WHERE TableName = "${tableName}"
                                    AND IndexName = "rollup_${tableName}"
                                    AND State = "FINISHED"
                                """,
                                has_count(1), 30))

    assertTrue(helper.check_table_describe_times(tableName, 30))

    logger.info("=== Test 2: add value column ===")
    // binlog type: MODIFY_TABLE_ADD_OR_DROP_COLUMNS, binlog data:
    // {
    //   "dbId": 11049,
    //   "tableId": 11058,
    //   "indexSchemaMap": {
    //     "11101": [...]
    //   },
    //   "indexes": [],
    //   "jobId": 11117,
    //   "rawSql":"ALTER TABLE `regression_test_table_sync_rollup_col_add`.`tbl_848588167` ADD COLUMN `first_value` int NULL DEFAULT \"0\" COMMENT \"\" IN `rollup_tbl_848588167`"
    //   }
    sql """
        ALTER TABLE ${tableName}
        ADD COLUMN `first_value` INT DEFAULT "0"
        TO rollup_${tableName}
        """
    sql "sync"

    assertTrue(helper.checkShowTimesOf("""
                                SHOW ALTER TABLE COLUMN
                                FROM ${context.dbName}
                                WHERE TableName = "${tableName}"
                                    AND IndexName = "rollup_${tableName}"
                                    AND State = "FINISHED"
                                """,
                                has_count(2), 30))
    assertTrue(helper.check_table_describe_times(tableName, 30))

    // no full sync triggered.
    def last_job_progress = helper.get_job_progress(tableName)
    assertTrue(last_job_progress.full_sync_start_at == first_job_progress.full_sync_start_at)
}
