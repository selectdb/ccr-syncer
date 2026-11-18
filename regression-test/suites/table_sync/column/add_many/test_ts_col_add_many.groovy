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
suite("test_ts_col_add_many") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def tableName = "tbl_" + helper.randomSuffix()
    def test_num = 0
    def insert_num = 5

    def exist = { res -> Boolean
        return res.size() != 0
    }

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE if NOT EXISTS ${tableName}
        (
            `test` INT,
            `id` INT,
            `value` INT
        )
        ENGINE=OLAP
        UNIQUE KEY(`test`, `id`)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
    """

    def values = [];
    for (int index = 0; index < insert_num; index++) {
        values.add("(${test_num}, ${index}, ${index})")
    }
    sql """
        INSERT INTO ${tableName} VALUES ${values.join(",")}
        """
    sql "sync"

    helper.ccrJobCreate(tableName)
    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 30))

    logger.info("=== Test 1: add column case ===")
    // binlog type: ALTER_JOB, binlog data:
    // {
    //   "type": "SCHEMA_CHANGE",
    //   "dbId": 11049,
    //   "tableId": 11329,
    //   "tableName": "tbl_add_many_column431ed55d264646ba9bd30419a7b8f90d",
    //   "jobId": 11346,
    //   "jobState": "PENDING",
    //   "rawSql": "ALTER TABLE `regression_test_schema_change`.`tbl_add_many_column431ed55d264646ba9bd30419a7b8f90d` ADD COLUMN (`last_key` int NULL DEFAULT \"0\" COMMENT \"\", `last_value` int NULL DEFAULT \"0\" COMMENT \"\")"
    // }
    sql """
        ALTER TABLE ${tableName}
        ADD COLUMN (`last_key` INT KEY DEFAULT "0", `last_value` INT DEFAULT "0")
        """
    sql "sync"

    assertTrue(helper.checkShowTimesOf("""
                                SHOW ALTER TABLE COLUMN
                                FROM ${context.dbName}
                                WHERE TableName = "${tableName}" AND State = "FINISHED"
                                """,
                                exist, 30))

    def has_columns = { res -> Boolean
        // Field == 'first' && 'Key' == 'YES'
        def found_last_key = false
        def found_last_value = false
        for (int i = 0; i < res.size(); i++) {
            if (res[i][0] == 'last_key' && (res[i][3] == 'YES' || res[i][3] == 'true')) {
                found_last_key = true
            }
            if (res[i][0] == 'last_value' && (res[i][3] == 'NO' || res[i][3] == 'false')) {
                found_last_value = true
            }
        }
        return found_last_key && found_last_value
    }

    assertTrue(helper.checkShowTimesOf("SHOW COLUMNS FROM `${tableName}`", has_columns, 60, "target_sql"))
}

