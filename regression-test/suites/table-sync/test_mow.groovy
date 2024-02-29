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

suite("test_mow") {
    def tableName = "tbl_mow_" + UUID.randomUUID().toString().replace("-", "")
    def syncerAddress = "127.0.0.1:9190"
    def test_num = 0
    def insert_num = 5
    def sync_gap_time = 5000
    String response

    def checkSelectTimesOf = { sqlString, rowSize, times, func = null -> Boolean
        def tmpRes = target_sql "${sqlString}"
        while (tmpRes.size() != rowSize || (func != null && !func(tmpRes))) {
            sleep(sync_gap_time)
            if (--times > 0) {
                tmpRes = target_sql "${sqlString}"
            } else {
                break
            }
        }
        return tmpRes.size() == rowSize && (func == null || func(tmpRes))
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

    sql """
        CREATE TABLE if NOT EXISTS ${tableName}
        (
            `test` INT,
            `id` INT,
            `commit_seq` INT
        )
        ENGINE=OLAP
        UNIQUE KEY(`test`)
        DISTRIBUTED BY HASH(test) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "function_column.sequence_col" = 'commit_seq',
            "enable_unique_key_merge_on_write" = "true"
        )
    """
    for (int index = 0; index < insert_num; ++index) {
        sql """
            INSERT INTO ${tableName} VALUES (${test_num}, ${index}, ${index})
            """
    }
    sql """ALTER TABLE ${tableName} set ("binlog.enable" = "true")"""
    sql "sync"

    logger.info("=== Test 1: full update mow ===")
    httpTest {
        uri "/create_ccr"
        endpoint syncerAddress
        def bodyJson = get_ccr_body "${tableName}"
        body "${bodyJson}"
        op "post"
        result response
    }

    assertTrue(checkRestoreFinishTimesOf("${tableName}", 30))

    // show create table regression_test_p0.tbl_mow_sync;
    def res = target_sql "SHOW CREATE TABLE ${tableName}"
    def enabledMOW = false
    for (List<Object> row : res) {
        if ((row[0] as String) == "${tableName}") {
            enabledMOW = (row[1] as String).contains("\"enable_unique_key_merge_on_write\" = \"true\"")
            break
        }
    }
    assertTrue(enabledMOW)

    logger.info("=== Test 2: incremental value ===")
    test_num = 2
    for (int index = 0; index < insert_num; ++index) {
        sql """
            INSERT INTO ${tableName} VALUES (${test_num}, ${index}, ${index})
            """
    }
    sql "sync"
    def checkSeq1 = { inputRes -> Boolean
        for (List<Object> row : inputRes) {
            if ((row[2] as Integer) != 4) {
                return false
            }
        }
        return true
    }
    assertTrue(checkSelectTimesOf("SELECT * FROM ${tableName} WHERE test=${test_num}",
                                   1, 30, checkSeq1))
    

    logger.info("=== Test 3: sequence value ===")
    test_num = 3
    for (int index = 0; index < insert_num; ++index) {
        sql """
            INSERT INTO ${tableName} VALUES (${test_num}, ${test_num}, 5 - ${index})
            """
    }
    sql "sync"
    def checkSeq2 = { inputRes -> Boolean
        for (List<Object> row : inputRes) {
            if ((row[2] as Integer) != 5) {
                return false
            }
        }
        return true
    }
    assertTrue(checkSelectTimesOf("SELECT * FROM ${tableName} WHERE test=${test_num}",
                                   1, 30, checkSeq2))
}
