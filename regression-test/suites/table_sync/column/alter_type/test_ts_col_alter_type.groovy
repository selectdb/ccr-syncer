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
suite("test_ts_col_alter_type") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def dbName = context.dbName
    def dbNameTarget = "TEST_" + context.dbName
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

    def value_is_big_int = { res -> Boolean
        // Field == 'value' && 'Type' == 'bigint'
        return res[2][0] == 'value' && res[2][1] == 'bigint'
    }

    def id_is_big_int = { res -> Boolean
        // Field == 'id' && 'Type' == 'bigint'
        return res[1][0] == 'id' && res[1][1] == 'bigint'
    }

    helper.enableDbBinlog()
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
    target_sql "DROP TABLE IF EXISTS ${dbNameTarget}.${tableName}"

    sql """
        CREATE TABLE if NOT EXISTS ${tableName}
        (
            `test` INT,
            `id` INT,
            `value` INT
        )
        ENGINE=OLAP
        UNIQUE KEY(`test`, `id`)
        DISTRIBUTED BY HASH(test) BUCKETS 1
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

    helper.ccrJobDelete(tableName)
    helper.ccrJobCreate(tableName)

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 30))

    logger.info("=== Test 1: add key column type ===")
    // binlog type: ALTER_JOB, binlog data:
    //  {
    //      "type":"SCHEMA_CHANGE",
    //      "dbId":11049,
    //      "tableId":11058,
    //      "tableName":"tbl_add_column6ab3b514b63c4368aa0a0149da0acabd",
    //      "jobId":11076,
    //      "jobState":"FINISHED",
    //      "rawSql":"ALTER TABLE `regression_test_schema_change`.`tbl_add_column6ab3b514b63c4368aa0a0149da0acabd` MODIFY COLUMN `id` bigint NULL DEFAULT \"0\" COMMENT \"\" FIRST"
    //  }
    sql """
        ALTER TABLE ${tableName}
        MODIFY COLUMN `id` BIGINT KEY
        """
    sql "sync"

    assertTrue(helper.checkShowTimesOf("""
                                SHOW ALTER TABLE COLUMN
                                FROM ${dbName}
                                WHERE TableName = "${tableName}" AND State = "FINISHED"
                                """,
                                has_count(1), 30))

    assertTrue(helper.checkShowTimesOf("SHOW COLUMNS FROM `${tableName}`", id_is_big_int, 60, "sql"))

    assertTrue(helper.checkShowTimesOf("SHOW COLUMNS FROM `${tableName}`", id_is_big_int, 60, "target_sql"))

    logger.info("=== Test 2: alter value column type ===")
    // binlog type: ALTER_JOB, binlog data:
    // {
    //   "type": "SCHEMA_CHANGE",
    //   "dbId": 11049,
    //   "tableId": 11058,
    //   "tableName": "tbl_add_column6ab3b514b63c4368aa0a0149da0acabd",
    //   "jobId": 11100,
    //   "jobState": "FINISHED",
    //   "rawSql": "ALTER TABLE `regression_test_schema_change`.`tbl_add_column6ab3b514b63c4368aa0a0149da0acabd` MODIFY COLUMN `value` int NULL DEFAULT \"0\" COMMENT \"\" AFTER `id`"
    // }
    sql """
        ALTER TABLE ${tableName}
        MODIFY COLUMN `value` BIGINT
        """
    sql "sync"

    logger.info("=== Test 2: Check column type ===")

    assertTrue(helper.checkShowTimesOf("""
                                SHOW ALTER TABLE COLUMN
                                FROM ${dbName}
                                WHERE TableName = "${tableName}" AND State = "FINISHED"
                                """,
                                has_count(2), 30))

    assertTrue(helper.checkShowTimesOf("SHOW COLUMNS FROM ${tableName}", value_is_big_int, 60, "sql"))

    assertTrue(helper.checkShowTimesOf("SHOW COLUMNS FROM ${tableName}", value_is_big_int, 60, "target_sql"))
}

