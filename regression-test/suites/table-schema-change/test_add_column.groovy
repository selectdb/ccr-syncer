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
suite("test_add_column") {
    def tableName = "tbl_add_column" + UUID.randomUUID().toString().replace("-", "")
    def syncerAddress = "127.0.0.1:9190"
    def test_num = 0
    def insert_num = 5
    def sync_gap_time = 5000
    String response

    def checkShowTimesOf = { sqlString, myClosure, times, func = "sql" -> Boolean
        Boolean ret = false
        List<List<Object>> res
        while (times > 0) {
            try {
                if (func == "sql") {
                    res = sql "${sqlString}"
                } else {
                    res = target_sql "${sqlString}"
                }
                if (myClosure.call(res)) {
                    ret = true
                }
            } catch (Exception e) {}

            if (ret) {
                break
            } else if (--times > 0) {
                sleep(sync_gap_time)
            }
        }

        return ret
    }

    def checkSelectRowTimesOf = { sqlString, rowSize, times -> Boolean
        def tmpRes = target_sql "${sqlString}"
        while (tmpRes.size() != rowSize) {
            sleep(sync_gap_time)
            if (--times > 0) {
                tmpRes = target_sql "${sqlString}"
            } else {
                break
            }
        }
        return tmpRes.size() == rowSize
    }

    def checkSelectColTimesOf = { sqlString, colSize, times -> Boolean
        def tmpRes = target_sql "${sqlString}"
        while (tmpRes.size() == 0 || tmpRes[0].size() != colSize) {
            sleep(sync_gap_time)
            if (--times > 0) {
                tmpRes = target_sql "${sqlString}"
            } else {
                break
            }
        }
        return tmpRes.size() > 0 && tmpRes[0].size() == colSize
    }

    def checkData = { data, beginCol, value -> Boolean
        if (data.size() < beginCol + value.size()) {
            return false
        }

        for (int i = 0; i < value.size(); ++i) {
            if ((data[beginCol + i]) as int != value[i]) {
                return false
            }
        }

        return true
    }

    def checkRestoreFinishTimesOf = { checkTable, times -> Boolean
        Boolean ret = false
        while (times > 0) {
            def sqlInfo = target_sql "SHOW RESTORE FROM TEST_${context.dbName}"
            for (List<Object> row : sqlInfo) {
                if ((row[10] as String).contains(checkTable)) {
                    ret = (row[4] as String) == "FINISHED"
                }
            }

            if (ret) {
                break
            } else if (--times > 0) {
                sleep(sync_gap_time)
            }
        }

        return ret
    }

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

    def get_job_progress = { ccr_name ->
        def request_body = """ {"name":"${ccr_name}"} """
        def get_job_progress_uri = { check_func ->
            httpTest {
                uri "/job_progress"
                endpoint syncerAddress
                body request_body
                op "post"
                check check_func
            }
        }

        def result = null
        get_job_progress_uri.call() { code, body ->
            if (!"${code}".toString().equals("200")) {
                throw "request failed, code: ${code}, body: ${body}"
            }
            def jsonSlurper = new groovy.json.JsonSlurper()
            def object = jsonSlurper.parseText "${body}"
            if (!object.success) {
                throw "request failed, error msg: ${object.error_msg}"
            }
            logger.info("job progress: ${object.job_progress}")
            result = jsonSlurper.parseText object.job_progress
        }
        return result
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

    def bodyJson = get_ccr_body "${tableName}"
    ccr_name = get_ccr_name(bodyJson)
    httpTest {
        uri "/create_ccr"
        endpoint syncerAddress
        body "${bodyJson}"
        op "post"
        result response
    }
    logger.info("ccr job name: ${ccr_name}")

    assertTrue(checkRestoreFinishTimesOf("${tableName}", 30))

    first_job_progress = get_job_progress(ccr_name)

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
        ADD COLUMN `first` INT KEY DEFAULT 0 FIRST
        """
    sql "sync"

    assertTrue(checkShowTimesOf("""
                                SHOW ALTER TABLE COLUMN
                                FROM ${context.dbName}
                                WHERE TableName = "${tableName}" AND State = "FINISHED"
                                """,
                                has_count(1), 30))

    def has_column_first = { res -> Boolean
        // Field == 'first' && 'Key' == 'YES'
        return res[0][0] == 'first' && (res[0][3] == 'YES' || res[0][3] == 'true')
    }

    assertTrue(checkShowTimesOf("SHOW COLUMNS FROM `${tableName}`", has_column_first, 60, "target_sql"))

    logger.info("=== Test 2: add column after last key ===")
    // binlog type: ALTER_JOB, binlog data:
    // {
    //   "type": "SCHEMA_CHANGE",
    //   "dbId": 11049,
    //   "tableId": 11058,
    //   "tableName": "tbl_add_column6ab3b514b63c4368aa0a0149da0acabd",
    //   "jobId": 11100,
    //   "jobState": "FINISHED",
    //   "rawSql": "ALTER TABLE `regression_test_schema_change`.`tbl_add_column6ab3b514b63c4368aa0a0149da0acabd` ADD COLUMN `last` int NULL DEFAULT \"0\" COMMENT \"\" AFTER `id`"
    // }
    sql """
        ALTER TABLE ${tableName}
        ADD COLUMN `last` INT KEY DEFAULT 0 AFTER `id`
        """
    sql "sync"

    assertTrue(checkShowTimesOf("""
                                SHOW ALTER TABLE COLUMN
                                FROM ${context.dbName}
                                WHERE TableName = "${tableName}" AND State = "FINISHED"
                                """,
                                has_count(2), 30))

    def has_column_last = { res -> Boolean
        // Field == 'last' && 'Key' == 'YES'
        return res[3][0] == 'last' && (res[3][3] == 'YES' || res[3][3] == 'true')
    }

    assertTrue(checkShowTimesOf("SHOW COLUMNS FROM `${tableName}`", has_column_last, 60, "target_sql"))

    logger.info("=== Test 3: add value column after last key ===")
    // binlog type: MODIFY_TABLE_ADD_OR_DROP_COLUMNS, binlog data:
    // {
    //   "dbId": 11049,
    //   "tableId": 11058,
    //   "indexSchemaMap": {
    //     "11101": [...]
    //   },
    //   "indexes": [],
    //   "jobId": 11117,
    //   "rawSql": "ALTER TABLE `regression_test_schema_change`.`tbl_add_column6ab3b514b63c4368aa0a0149da0acabd` ADD COLUMN `first_value` int NULL DEFAULT \"0\" COMMENT \"\" AFTER `last`"
    //   }
    sql """
        ALTER TABLE ${tableName}
        ADD COLUMN `first_value` INT DEFAULT 0 AFTER `last`
        """
    sql "sync"

    assertTrue(checkShowTimesOf("""
                                SHOW ALTER TABLE COLUMN
                                FROM ${context.dbName}
                                WHERE TableName = "${tableName}" AND State = "FINISHED"
                                """,
                                has_count(3), 30))

    def has_column_first_value = { res -> Boolean
        // Field == 'first_value' && 'Key' == 'NO'
        return res[4][0] == 'first_value' && (res[4][3] == 'NO' || res[4][3] == 'false')
    }

    assertTrue(checkShowTimesOf("SHOW COLUMNS FROM `${tableName}`", has_column_first_value, 60, "target_sql"))

    logger.info("=== Test 4: add value column last ===")
    // binlog type: MODIFY_TABLE_ADD_OR_DROP_COLUMNS, binlog data:
    // {
    //   "dbId": 11049,
    //   "tableId": 11150,
    //   "indexSchemaMap": {
    //     "11180": []
    //   },
    //   "indexes": [],
    //   "jobId": 11197,
    //   "rawSql": "ALTER TABLE `regression_test_schema_change`.`tbl_add_column5f9a63de97fc4b5fb7a001f778dd180d` ADD COLUMN `last_value` int NULL DEFAULT \"0\" COMMENT \"\" AFTER `value`"
    // }
    sql """
        ALTER TABLE ${tableName}
        ADD COLUMN `last_value` INT DEFAULT 0 AFTER `value`
        """
    sql "sync"

    assertTrue(checkShowTimesOf("""
                                SHOW ALTER TABLE COLUMN
                                FROM ${context.dbName}
                                WHERE TableName = "${tableName}" AND State = "FINISHED"
                                """,
                                has_count(4), 30))

    def has_column_last_value = { res -> Boolean
        // Field == 'last_value' && 'Key' == 'NO'
        return res[6][0] == 'last_value' && (res[6][3] == 'NO' || res[6][3] == 'false')
    }

    assertTrue(checkShowTimesOf("SHOW COLUMNS FROM `${tableName}`", has_column_last_value, 60, "target_sql"))

    // no full sync triggered.
    last_job_progress = get_job_progress(ccr_name)
    assertTrue(last_job_progress.full_sync_start_at == first_job_progress.full_sync_start_at)
}
