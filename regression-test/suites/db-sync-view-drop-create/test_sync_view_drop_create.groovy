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

suite("test_sync_view_drop_create") {
    def syncerAddress = "127.0.0.1:9190"
    def sync_gap_time = 5000
    def createDuplicateTable = { tableName ->
       sql """
            CREATE TABLE if NOT EXISTS ${tableName}
            (
                user_id            BIGINT       NOT NULL COMMENT "用户 ID",
                name               VARCHAR(20)           COMMENT "用户姓名",
                age                INT                   COMMENT "用户年龄"
            )
            ENGINE=OLAP
            DUPLICATE KEY(user_id)
            DISTRIBUTED BY HASH(user_id) BUCKETS 10
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "binlog.enable" = "true"
            )
        """
    }

    def checkShowTimesOf = { sqlString, checkFunc, times, func = "sql" -> Boolean
        List<List<Object>> res
        while (times > 0) {
            try {
                if (func == "sql") {
                    res = sql "${sqlString}"
                } else {
                    res = target_sql "${sqlString}"
                }

                if (checkFunc.call(res)) {
                    return true
                }
            } catch (Exception e) {
                logger.warn("Exception: ${e}")
            }

            if (--times > 0) {
                sleep(sync_gap_time)
            }
        }

        return false
    }

    def checkSelectTimesOf = { sqlString, rowSize, times -> Boolean
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

    def checkBackupFinishTimesOf = { checkTable, times -> Boolean
        Boolean ret = false
        while (times > 0) {
            def sqlInfo = sql "SHOW BACKUP FROM ${context.dbName}"
            for (List<Object> row : sqlInfo) {
                if ((row[4] as String).contains(checkTable)) {
                    ret = row[3] == "FINISHED"
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

    def checkRestoreAllFinishTimesOf = { checkTable, times -> Boolean
        Boolean ret = true
        while (times > 0) {
            def sqlInfo = target_sql "SHOW RESTORE FROM TEST_${context.dbName}"
            for (List<Object> row : sqlInfo) {
                if ((row[10] as String).contains(checkTable)) {
                    if ((row[4] as String) != "FINISHED") {
                        ret = false
                    }
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

    def checkRestoreRowsTimesOf = {rowSize, times -> Boolean
        Boolean ret = false
        while (times > 0) {
            def sqlInfo = target_sql "SHOW RESTORE FROM TEST_${context.dbName}"
            if (sqlInfo.size() >= rowSize) {
                ret = true
                break
            } else if (--times > 0 && sqlInfo.size() < rowSize) {
                sleep(sync_gap_time)
            }
        }

        return ret
    }

    def exist = { res -> Boolean
        return res.size() != 0
    }
    def notExist = { res -> Boolean
        return res.size() == 0
    }

    def suffix = UUID.randomUUID().toString().replace("-", "")
    def tableDuplicate0 = "tbl_duplicate_0_${suffix}"
    createDuplicateTable(tableDuplicate0)
    sql """
        INSERT INTO ${tableDuplicate0} VALUES
        (1, "Emily", 25),
        (2, "Benjamin", 35),
        (3, "Olivia", 28),
        (4, "Alexander", 60),
        (5, "Ava", 17),
        (5, "Ava", 18);
        """

    sql "ALTER DATABASE ${context.dbName} SET properties (\"binlog.enable\" = \"true\")"

    logger.info("=== Test1: create view ===")
    sql """
        CREATE VIEW view_test_${suffix} (k1, name,  v1)
        AS
        SELECT user_id as k1, name, SUM(age) FROM ${tableDuplicate0}
        GROUP BY k1,name;
        """

    String response

    httpTest {
        uri "/delete"
        endpoint syncerAddress
        def bodyJson = get_ccr_body ""
        body "${bodyJson}"
        op "post"
    }

    httpTest {
        uri "/create_ccr"
        endpoint syncerAddress
        def bodyJson = get_ccr_body ""
        body "${bodyJson}"
        op "post"
        result response
    }

    assertTrue(checkRestoreFinishTimesOf("${tableDuplicate0}", 30))
    assertTrue(checkSelectTimesOf("SELECT * FROM ${tableDuplicate0}", 6, 30))

    // drop the view, and create it again.
    // Must be incremental sync.
    sql """
        DROP VIEW view_test_${suffix}
    """

    sql """
        CREATE VIEW view_test_${suffix} (k1, name,  v1)
        AS
        SELECT user_id as k1, name,  SUM(age) FROM ${tableDuplicate0}
        GROUP BY k1,name;
    """

    // Since create view is synced to downstream, this insert will be sync too.
    sql """
        INSERT INTO ${tableDuplicate0} VALUES
            (6, "Zhangsan", 31),
            (5, "Ava", 20);
        """
    sql "sync"

    assertTrue(checkSelectTimesOf("SELECT * FROM ${tableDuplicate0}", 8, 50))
    def view_size = target_sql "SHOW VIEW FROM ${tableDuplicate0}"
    assertTrue(view_size.size() == 1);
}

