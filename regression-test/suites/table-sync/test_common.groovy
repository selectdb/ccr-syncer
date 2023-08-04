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
suite("test_common") {

    def tableName = "tbl_common_" + UUID.randomUUID().toString().replace("-", "")
    def uniqueTable = "${tableName}_unique"
    def aggregateTable = "${tableName}_aggregate"
    def duplicateTable = "${tableName}_duplicate"
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
        CREATE TABLE if NOT EXISTS ${uniqueTable}
        (
            `test` INT,
            `id` INT
        )
        ENGINE=OLAP
        UNIQUE KEY(`test`, `id`)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true",
            "binlog.ttl_seconds" = "180"
        )
    """

    sql """
        CREATE TABLE if NOT EXISTS ${aggregateTable}
        (
            `test` INT,
            `last` INT REPLACE DEFAULT "0",
            `cost` INT SUM DEFAULT "0",
            `max` INT MAX DEFAULT "0",
            `min` INT MIN DEFAULT "0"
        )
        ENGINE=OLAP
        AGGREGATE KEY(`test`)
        DISTRIBUTED BY HASH(`test`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true",
            "binlog.ttl_seconds" = "180"
        )
    """

    sql """
        CREATE TABLE if NOT EXISTS ${duplicateTable}
        (
            `test` INT,
            `id` INT
        )
        ENGINE=OLAP
        DUPLICATE KEY(`test`, `id`)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true",
            "binlog.ttl_seconds" = "180"
        )
    """



    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${uniqueTable} VALUES (${test_num}, ${index})
            """
    }
    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${aggregateTable} VALUES (${test_num}, ${index}, ${index}, ${index}, ${index})
            """
    }
    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${duplicateTable} VALUES (0, 99)
            """
    }


    // test 1: target cluster follow source cluster
    logger.info("=== Test 1: backup/restore case ===")
    httpTest {
        uri "/create_ccr"
        endpoint syncerAddress
        def bodyJson = get_ccr_body "${uniqueTable}"
        body "${bodyJson}"
        op "post"
        result respone
    }
    assertTrue(checkRestoreFinishTimesOf("${uniqueTable}", 30))
    assertTrue(checkSelectTimesOf("SELECT * FROM ${uniqueTable} WHERE test=${test_num}",
                                   insert_num, 30))

    httpTest {
        uri "/create_ccr"
        endpoint syncerAddress
        def bodyJson = get_ccr_body "${aggregateTable}"
        body "${bodyJson}"
        op "post"
        result respone
    }
    assertTrue(checkRestoreFinishTimesOf("${aggregateTable}", 30))
    assertTrue(checkSelectTimesOf("SELECT * FROM ${aggregateTable} WHERE test=${test_num}",
                                   1, 30))
    def resList = [4, 10, 4, 0]
    def resData = target_sql "SELECT * FROM ${aggregateTable} WHERE test=${test_num}"
    assertTrue(checkData(resData[0], 1, resList))

    httpTest {
        uri "/create_ccr"
        endpoint syncerAddress
        def bodyJson = get_ccr_body "${duplicateTable}"
        body "${bodyJson}"
        op "post"
        result respone
    }
    assertTrue(checkRestoreFinishTimesOf("${duplicateTable}", 30))
    assertTrue(checkSelectTimesOf("SELECT * FROM ${duplicateTable} WHERE test=${test_num}",
                                   insert_num, 30))





    logger.info("=== Test 2: dest cluster follow source cluster case ===")
    test_num = 2
    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${uniqueTable} VALUES (${test_num}, ${index})
            """
    }
    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${aggregateTable} VALUES (${test_num}, ${index}, ${index}, ${index}, ${index})
            """
    }
    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${duplicateTable} VALUES (0, 99)
            """
    }
    assertTrue(checkSelectTimesOf("SELECT * FROM ${uniqueTable} WHERE test=${test_num}",
                                   insert_num, 30))
    assertTrue(checkSelectTimesOf("SELECT * FROM ${aggregateTable} WHERE test=${test_num}",
                                   1, 30))
    resData = target_sql "SELECT * FROM ${aggregateTable} WHERE test=${test_num}"
    assertTrue(checkData(resData[0], 1, resList))
    assertTrue(checkSelectTimesOf("SELECT * FROM ${duplicateTable} WHERE test=0",
                                   2 * insert_num, 30))

}