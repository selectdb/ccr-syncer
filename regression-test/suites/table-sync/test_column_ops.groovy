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
suite("test_column_ops") {

    def tableName = "tbl_column_ops" + UUID.randomUUID().toString().replace("-", "")
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

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE if NOT EXISTS ${tableName}
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

    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${tableName} VALUES (${test_num}, ${index})
            """
    }
    sql "sync"

    httpTest {
        uri "/create_ccr"
        endpoint syncerAddress
        def bodyJson = get_ccr_body "${tableName}"
        body "${bodyJson}"
        op "post"
        result response
    }

    assertTrue(checkRestoreFinishTimesOf("${tableName}", 30))


    logger.info("=== Test 1: add column case ===")
    sql """
        ALTER TABLE ${tableName}
        ADD COLUMN (`cost` VARCHAR(3) DEFAULT "123")
        """
    
    assertTrue(checkShowTimesOf("""
                                SHOW ALTER TABLE COLUMN
                                FROM ${context.dbName}
                                WHERE TableName = "${tableName}" AND State = "FINISHED"
                                """,
                                exist, 30))

    assertTrue(checkSelectColTimesOf("SELECT * FROM ${tableName} WHERE test=${test_num}",
                                      3, 30))


    logger.info("=== Test 2: modify column length case ===")
    test_num = 2
    sql """
        ALTER TABLE ${tableName}
        MODIFY COLUMN `cost` VARCHAR(4) DEFAULT "123"
        """
    sql """
        INSERT INTO ${tableName} VALUES (${test_num}, 0, "8901")
        """
    sql "sync"
    assertTrue(checkSelectRowTimesOf("SELECT * FROM ${tableName} WHERE test=${test_num}",
                                      1, 30))


//     logger.info("=== Test 3: modify column type case ===")
//     test_num = 3
//     sql """
//         ALTER TABLE ${tableName}
//         MODIFY COLUMN `cost` INT DEFAULT "123"
//         """
//     assertTrue(checkRestoreFinishTimesOf("${tableName}", 1, 30))
//
//     sql """
//         INSERT INTO ${tableName} VALUES (${test_num}, 0, 23456)
//         """
//     assertTrue(checkSelectRowTimesOf("SELECT * FROM ${tableName} WHERE test=${test_num}",
//                                       1, 30))


    logger.info("=== Test 4: rename column case ===")
    test_num = 4
    sql """
        ALTER TABLE ${tableName}
        RENAME COLUMN `cost` `_cost`
        """
    sql """
        INSERT INTO ${tableName} VALUES (${test_num}, 0, "666")
        """
    sql "sync"
    assertTrue(checkSelectRowTimesOf("SELECT * FROM ${tableName} WHERE test=${test_num}",
        1, 30))
    assertTrue(checkSelectRowTimesOf("SELECT * FROM ${tableName} WHERE test=${test_num} AND _cost='666'",
        1, 1))


    logger.info("=== Test 5: drop column case ===")
    sql """
        ALTER TABLE ${tableName}
        DROP COLUMN `_cost`
        """
    assertTrue(checkSelectColTimesOf("SELECT * FROM ${tableName} WHERE test=${test_num}",
                                      2, 30))
}
