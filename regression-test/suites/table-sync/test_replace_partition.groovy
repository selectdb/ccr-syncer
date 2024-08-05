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

suite("test_replace_partition") {

    def baseTableName = "test_replace_partition_" + UUID.randomUUID().toString().replace("-", "")
    def syncerAddress = "127.0.0.1:9190"
    def test_num = 0
    def insert_num = 5
    def sync_gap_time = 5000
    def opPartitonName = "less0"
    String response

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
    def notExist = { res -> Boolean
        return res.size() == 0
    }

    logger.info("=== Create table ===")
    tableName = "${baseTableName}"
    sql """
        CREATE TABLE if NOT EXISTS ${tableName}
        (
            `test` INT,
            `id` INT
        )
        ENGINE=OLAP
        UNIQUE KEY(`test`, `id`)
        PARTITION BY RANGE(`id`)
        (
            PARTITION `p1` VALUES LESS THAN ("0"),
            PARTITION `p2` VALUES LESS THAN ("100"),
            PARTITION `p3` VALUES LESS THAN ("200"),
            PARTITION `p4` VALUES LESS THAN ("300")
        )
        DISTRIBUTED BY HASH(id) BUCKETS AUTO
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

    assertTrue(checkRestoreFinishTimesOf("${tableName}", 60))

    logger.info("=== Add temp partition p5 ===")

    sql """
        ALTER TABLE ${tableName} ADD TEMPORARY PARTITION p5 VALUES [("0"), ("100"))
        """

    assertTrue(checkShowTimesOf("""
                                SHOW TEMPORARY PARTITIONS
                                FROM ${tableName}
                                WHERE PartitionName = "p5"
                                """,
                                exist, 60, "sql"))

    sql "INSERT INTO ${tableName} TEMPORARY PARTITION (p5) VALUES (1, 50)"

    assertTrue(checkShowTimesOf("""
                                SELECT *
                                FROM ${tableName}
                                TEMPORARY PARTITION (p5)
                                WHERE id = 50
                                """,
                                exist, 60, "sql"))

    logger.info("=== Replace partition p2 by p5 ===")

    assertTrue(checkShowTimesOf("""
                                SELECT *
                                FROM ${tableName}
                                WHERE id = 50
                                """,
                                notExist, 60, "target"))

    sql "ALTER TABLE ${tableName} REPLACE PARTITION (p2) WITH TEMPORARY PARTITION (p5)"

    assertTrue(checkShowTimesOf("""
                                SELECT *
                                FROM ${tableName}
                                WHERE id = 50
                                """,
                                exist, 60, "target"))

    // We don't support replace partition with non-strict range and use temp name.

    // logger.info("=== Add temp partition p6 ===")

    // sql """
    //     ALTER TABLE ${tableName} ADD TEMPORARY PARTITION p6 VALUES [("100"), ("200"))
    //     """

    // assertTrue(checkShowTimesOf("""
    //                             SHOW TEMPORARY PARTITIONS
    //                             FROM ${tableName}
    //                             WHERE PartitionName = "p6"
    //                             """,
    //                             exist, 60, "sql"))

    // sql "INSERT INTO ${tableName} TEMPORARY PARTITION (p6) VALUES (2, 150)"

    // assertTrue(checkShowTimesOf("""
    //                             SELECT *
    //                             FROM ${tableName}
    //                             TEMPORARY PARTITION (p6)
    //                             WHERE id = 150
    //                             """,
    //                             exist, 60, "sql"))

    // logger.info("=== Replace partition p3 by p6, with tmp partition name ===")

    // assertTrue(checkShowTimesOf("""
    //                             SELECT *
    //                             FROM ${tableName}
    //                             WHERE id = 150
    //                             """,
    //                             notExist, 60, "target"))

    // sql """ALTER TABLE ${tableName} REPLACE PARTITION (p3) WITH TEMPORARY PARTITION (p6)
    //     PROPERTIES (
    //         "use_temp_partition_name" = "true"
    //     )
    // """

    // assertTrue(checkShowTimesOf("""
    //                             SELECT *
    //                             FROM ${tableName}
    //                             WHERE id = 150
    //                             """,
    //                             exist, 60, "target"))

// // for non strict range
//     logger.info("=== Add temp partition p7 ===")

//     sql """
//         ALTER TABLE ${tableName} ADD TEMPORARY PARTITION p7 VALUES [("0"), ("200"))
//         """

//     assertTrue(checkShowTimesOf("""
//                                 SHOW TEMPORARY PARTITIONS
//                                 FROM ${tableName}
//                                 WHERE PartitionName = "p7"
//                                 """,
//                                 exist, 60, "sql"))

//     sql "INSERT INTO ${tableName} TEMPORARY PARTITION (p7) VALUES (1, 60), (2, 160)"

//     assertTrue(checkShowTimesOf("""
//                                 SELECT *
//                                 FROM ${tableName}
//                                 TEMPORARY PARTITION (p7)
//                                 WHERE id = 60
//                                 """,
//                                 exist, 60, "sql"))

//     logger.info("=== Replace partition p2,p6 by p7 ===")

//     assertTrue(checkShowTimesOf("""
//                                 SELECT *
//                                 FROM ${tableName}
//                                 WHERE id = 60
//                                 """,
//                                 notExist, 60, "target"))

//     sql """ALTER TABLE ${tableName} REPLACE PARTITION (p2,p6) WITH TEMPORARY PARTITION (p7)
//         PROPERTIES(
//             "strict_range" = "false"
//         )
//     """

//     assertTrue(checkShowTimesOf("""
//                                 SELECT *
//                                 FROM ${tableName}
//                                 WHERE id = 60
//                                 """,
//                                 exist, 60, "target"))

}

