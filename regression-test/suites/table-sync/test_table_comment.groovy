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

suite("test_table_comment") {

    def tableName = "tbl_comment" + UUID.randomUUID().toString().replace("-", "")
    def syncerAddress = "127.0.0.1:9190"
    def sync_gap_time = 5000
    String response

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

    def checkTableCommentTimesOf = { checkTable, expectedComment, times -> Boolean
        def expected = "COMMENT '${expectedComment}'"
        def res = target_sql "SHOW CREATE TABLE ${checkTable}"
        while (times > 0) {
            if (res.size() > 0 && (res[0][1] as String).contains(expected)) {
                return true
            }
            if (--times > 0) {
                sleep(sync_gap_time)
                res = target_sql "SHOW CREATE TABLE ${checkTable}"
            }
        }
        return false
    }

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName}
        (
            `test` INT,
            `id` INT
        )
        ENGINE=OLAP
        UNIQUE KEY(`test`, `id`)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
        """

    httpTest {
        uri "/create_ccr"
        endpoint syncerAddress
        def bodyJson = get_ccr_body "${tableName}"
        body "${bodyJson}"
        op "post"
        result response
    }

    assertTrue(checkRestoreFinishTimesOf("${tableName}", 30))

    logger.info("=== Test 1: modify table comment case ===")
    sql """
        ALTER TABLE ${tableName}
        MODIFY COMMENT "this is a test table"
        """
    assertTrue(checkTableCommentTimesOf(tableName, "this is a test table", 30))
}
