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
suite("test_truncate") {

    def tableName = "tbl_truncate_" + UUID.randomUUID().toString().replace("-", "")
    def syncerAddress = "127.0.0.1:9190"
    def test_num = 0
    def insert_num = 5
    def sync_gap_time = 5000
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

    def checkData = { data, beginCol, value -> Boolean
        if (data.size() < beginCol + value.size()) {
            return false
        }

        for (int i = 0; i < value.size(); ++i) {
            if ((data[beginCol + i] as int) != value[i]) {
                return false
            }
        }

        return true
    }

    sql """
        CREATE TABLE if NOT EXISTS ${tableName}
        (
            `test` INT,
            `id` INT,
            `num` INT
        )
        ENGINE=OLAP
        UNIQUE KEY(`test`, `id`, `num`)
        PARTITION BY RANGE(`id`)
        (
            PARTITION `ZERO` VALUES LESS THAN ("1"),
            PARTITION `ONE` VALUES LESS THAN ("2"),
            PARTITION `TWO` VALUES LESS THAN ("3"),
            PARTITION `THREE` VALUES LESS THAN ("4")
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
    """


    // test 1: target cluster follow source cluster
    logger.info("=== Test 1: backup/restore case ===")
    httpTest {
        uri "/create_ccr"
        endpoint syncerAddress
        def bodyJson = get_ccr_body "${tableName}"
        body "${bodyJson}"
        op "post"
        result response
    }
    assertTrue(checkRestoreFinishTimesOf("${tableName}", 30))



    logger.info("=== Test 2: full partitions ===")
    test_num = 2
    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${tableName} VALUES (${test_num}, 0, ${index})
            """
    }
    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${tableName} VALUES (${test_num}, 1, ${index})
            """
    }
    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${tableName} VALUES (${test_num}, 2, ${index})
            """
    }
    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${tableName} VALUES (${test_num}, 3, ${index})
            """
    }
    sql "sync"
    assertTrue(checkSelectTimesOf("SELECT * FROM ${tableName} WHERE test=${test_num} AND id=0",
                                   insert_num, 30))
    assertTrue(checkSelectTimesOf("SELECT * FROM ${tableName} WHERE test=${test_num} AND id=1",
                                   insert_num, 30))
    assertTrue(checkSelectTimesOf("SELECT * FROM ${tableName} WHERE test=${test_num} AND id=2",
                                   insert_num, 30))
    assertTrue(checkSelectTimesOf("SELECT * FROM ${tableName} WHERE test=${test_num} AND id=3",
                                   insert_num, 30))




    logger.info("=== Test 3: truncate partition ===")
    // TRUNCATE TABLE tbl PARTITION(p1, p2);
    sql """TRUNCATE TABLE ${tableName} PARTITION(`ONE`, `THREE`)"""

    assertTrue(checkSelectTimesOf("SELECT * FROM ${tableName} WHERE id=0",
                                   insert_num, 30))
    assertTrue(checkSelectTimesOf("SELECT * FROM ${tableName} WHERE id=1",
                                   0, 30))
    assertTrue(checkSelectTimesOf("SELECT * FROM ${tableName} WHERE id=2",
                                   insert_num, 30))
    assertTrue(checkSelectTimesOf("SELECT * FROM ${tableName} WHERE id=3",
                                   0, 30))




    logger.info("=== Test 4: truncate table ===")
    // TRUNCATE TABLE tbl PARTITION(p1, p2);
    sql """TRUNCATE TABLE ${tableName}"""

    assertTrue(checkSelectTimesOf("SELECT * FROM ${tableName} WHERE id=0",
                                   0, 30))
    assertTrue(checkSelectTimesOf("SELECT * FROM ${tableName} WHERE id=1",
                                   0, 30))
    assertTrue(checkSelectTimesOf("SELECT * FROM ${tableName} WHERE id=2",
                                   0, 30))
    assertTrue(checkSelectTimesOf("SELECT * FROM ${tableName} WHERE id=3",
                                   0, 30))
}
