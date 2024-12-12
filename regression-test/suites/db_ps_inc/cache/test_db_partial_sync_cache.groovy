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
suite("test_db_partial_sync_cache") {
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

    def get_ccr_name = { ccr_body_json ->
        def jsonSlurper = new groovy.json.JsonSlurper()
        def object = jsonSlurper.parseText "${ccr_body_json}"
        return object.name
    }

    helper.enableDbBinlog()
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

    helper.ccrJobCreate()

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 30))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName}", insert_num, 60))

    def first_job_progress = helper.get_job_progress()

    logger.info("=== Test 1: add first column case ===")
    // binlog type: ALTER_JOB, binlog data:
    //  {
    //      "type":"SCHEMA_CHANGE",
    //      "dbId":11049,
    //      "tableId":11058,
    //      "tableName":"tbl_add_column6ab3b514b63c4368aa0a0149da0acabd",
    //      "jobId":11076,
    //      "jobState":"FINISHED",
    //      "rawSql":"ALTER TABLE `regression_test_schema_change`.`tbl_add_column6ab3b514b63c4368aa0a0149da0acabd` ADD COLUMN `first` int NULL DEFAULT \"0\" COMMENT \"\" FIRST"
    //  }
    sql """
        ALTER TABLE ${tableName}
        ADD COLUMN `first` INT KEY DEFAULT "0" FIRST
        """
    sql "sync"

    assertTrue(helper.checkShowTimesOf("""
                                SHOW ALTER TABLE COLUMN
                                FROM ${context.dbName}
                                WHERE TableName = "${tableName}" AND State = "FINISHED"
                                """,
                                has_count(1), 30))

    def has_column_first = { res -> Boolean
        // Field == 'first' && 'Key' == 'YES'
        return res[0][0] == 'first' && (res[0][3] == 'YES' || res[0][3] == 'true')
    }

    assertTrue(helper.checkShowTimesOf("SHOW COLUMNS FROM `${tableName}`", has_column_first, 60, "target_sql"))

    sql "INSERT INTO ${tableName} VALUES (123, 123, 123, 123)"

    // cache must be clear and reload.
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName}", insert_num + 1, 60))

    // no full sync triggered.
    def last_job_progress = helper.get_job_progress()
    assertTrue(last_job_progress.full_sync_start_at == first_job_progress.full_sync_start_at)
}

