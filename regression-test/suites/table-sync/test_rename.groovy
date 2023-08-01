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

suite("test_rename") {
    def tableName = "tbl_rename"
    def syncerAddress = "127.0.0.1:9190"
    def test_num = 0
    def insert_num = 5
    def sync_gap_time = 5000
    String respone

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

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE if NOT EXISTS ${tableName}
        (
            `test` INT,
            `id` INT
        )
        ENGINE=OLAP
        UNIQUE KEY(`test`, `id`)
        PARTITION BY RANGE(`test`)
        (
            PARTITION `less100` VALUES LESS THAN ("100")
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
    """
    sql """ALTER TABLE ${tableName} set ("binlog.enable" = "true")"""

    httpTest {
        uri "/create_ccr"
        endpoint syncerAddress
        def bodyJson = get_ccr_body "${tableName}"
        body "${bodyJson}"
        op "post"
        result respone
    }

    def exist = { res -> Boolean
        return res.size() != 0
    }
    assertTrue(checkShowTimesOf("SHOW CREATE TABLE TEST_${context.dbName}.${tableName}",
                                exist, 30))


    logger.info("=== Test 0: Common insert case ===")
    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${tableName} VALUES (${test_num}, ${index})
            """
    }
    assertTrue(checkSelectTimesOf("SELECT * FROM ${tableName} WHERE test=${test_num}",
                                  insert_num, 30))



    logger.info("=== Test 1: Rename table case ===")
    test_num = 1
    def newTableName = "NEW_${tableName}"
    sql "ALTER TABLE ${tableName} RENAME ${newTableName}"

    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${newTableName} VALUES (${test_num}, ${index})
            """
    }
    assertTrue(checkSelectTimesOf("SELECT * FROM ${tableName} WHERE test=${test_num}",
                                  insert_num, 30))


    // logger.info("=== Test 2: Rename partition case ===")
    // def tmpPartition = "tmp"
    // sql """
    //     ALTER TABLE ${newTableName}
    //     ADD PARTITION ${tmpPartition}
    //     VALUES [('100'), ('10000'))
    // """
    // assertTrue(checkShowTimesOf("""
    //                             SHOW PARTITIONS
    //                             FROM TEST_${context.dbName}.${tableName}
    //                             WHERE PartitionName = \"${tmpPartition}\"
    //                             """,
    //                             exist, 30, "target"))

    // def test_big_num = 100
    // for (int index = 0; index < insert_num; index++) {
    //     sql """
    //         INSERT INTO ${newTableName} VALUES (${test_big_num}, ${index})
    //         """
    // }
    // assertTrue(checkSelectTimesOf("SELECT * FROM ${tableName} WHERE test=${test_big_num}",
    //                               insert_num, 30))

    // def newPartitionName = "new_tmp"
    // sql """ALTER TABLE ${newTableName}
    //        RENAME PARTITION ${tmpPartition} ${newPartitionName}"""

    // test_big_num = 1000
    // for (int index = 0; index < insert_num; index++) {
    //     sql """
    //         INSERT INTO ${newTableName} VALUES (${test_big_num}, ${index})
    //         """
    // }
    // assertTrue(checkSelectTimesOf("SELECT * FROM ${tableName} WHERE test=${test_big_num}",
    //                               insert_num, 30))

    // sql """
    //     ALTER TABLE ${newTableName}
    //     DROP PARTITION IF EXISTS ${newPartitionName}
    // """
    // def notExist = { res -> Boolean
    //     return res.size() == 0
    // }
    // assertTrue(checkShowTimesOf("""
    //                             SHOW PARTITIONS
    //                             FROM TEST_${context.dbName}.${tableName}
    //                             WHERE PartitionName = \"${tmpPartition}\"
    //                             """,
    //                             notExist, 30, "target"))
    // def resSql = target_sql "SELECT * FROM ${tableName} WHERE test=99"
    // assertTrue(resSql.size() == 0)
    // resSql = target_sql "SELECT * FROM ${tableName} WHERE test=100"
    // assertTrue(resSql.size() == 0)
}