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

suite("test_ts_index_add_bitmap") {
    logger.info("test bitmap index will be replaced by inverted index")
    return

    def tableName = "tbl_" + helper.randomSuffix()
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

    def checkRestoreFinishTimesOf = { checkTable, times -> Boolean
        Boolean ret = false
        while (times > 0) {
            def sqlInfo = target_sql "SHOW RESTORE FROM TEST_${context.dbName}"
            for (List<Object> row : sqlInfo) {
                if ((row[10] as String).contains(checkTable)) {
                    ret = row[4] == "FINISHED"
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
            `id` INT
        )
        ENGINE=OLAP
        DUPLICATE KEY(`test`, `id`)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        )
    """
    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${tableName} VALUES (${test_num}, ${index})
            """
    }
    sql """CREATE INDEX idx_test ON ${tableName} (`test`) USING BITMAP"""
    def createIndexFinished = { inputRes -> Boolean
        for (List<Object> row : inputRes) {
            if ((row[9] as String) == "FINISHED") {
                return true
            }
        }
        return false
    }
    sql "sync"
    assertTrue(checkShowTimesOf("""
                                SHOW ALTER TABLE COLUMN
                                WHERE TableName = \"${tableName}\"
                                """,
                                createIndexFinished, 30))
    sql """ALTER TABLE ${tableName} set ("binlog.enable" = "true")"""




    logger.info("=== Test 1: full update bitmap index ===")
    httpTest {
        uri "/create_ccr"
        endpoint syncerAddress
        def bodyJson = get_ccr_body "${tableName}"
        body "${bodyJson}"
        op "post"
        result response
    }

    assertTrue(checkRestoreFinishTimesOf("${tableName}", 30))
    def checkBitmap = { inputRes -> Boolean
        for (List<Object> row : inputRes) {
            if (row[2] == "idx_test" && row[10] == "BITMAP") {
                return true
            }
        }
        return false
    }
    assertTrue(checkShowTimesOf("""
                                SHOW INDEXES FROM TEST_${context.dbName}.${tableName}
                                """, 
                                checkBitmap, 30, "target"))
    


    logger.info("=== Test 2: incremental update bitmap index ===")
    sql """CREATE INDEX idx_id ON ${tableName} (`id`) USING BITMAP"""
    def checkBitmap2 = { inputRes -> Boolean
        for (List<Object> row : inputRes) {
            if (row[2] == "idx_id" && row[10] == "BITMAP") {
                return true
            }
        }
        return false
    }
    assertTrue(checkShowTimesOf("""
                                SHOW INDEXES FROM TEST_${context.dbName}.${tableName}
                                """, 
                                checkBitmap2, 30, "target"))
}
