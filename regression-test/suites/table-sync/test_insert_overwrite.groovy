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
suite("test_insert_overwrite") {
    def versions = sql_return_maparray "show variables like 'version_comment'"
    if (versions[0].Value.contains('doris-2.0.')) {
        logger.info("2.0 not support this case, current version is: ${versions[0].Value}")
        return
    }

    // The doris has two kind of insert overwrite handle logic: leagcy and nereids.
    // The first will
    //  1. create temp table
    //  2. insert into temp table
    //  3. replace table
    // The second will
    //  1. create temp partitions
    //  2. insert into temp partitions
    //  3. replace overlap partitions
    def tableName = "tbl_insert_overwrite_" + UUID.randomUUID().toString().replace("-", "")
    def uniqueTable = "${tableName}_unique"
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
        PARTITION BY RANGE(id)
        (
            PARTITION `p1` VALUES LESS THAN ("100"),
            PARTITION `p2` VALUES LESS THAN ("200")
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true",
            "binlog.ttl_seconds" = "180"
        )
    """

    sql """
    INSERT INTO ${uniqueTable} VALUES
        (1, 0),
        (1, 1),
        (1, 2),
        (1, 3),
        (1, 4)
    """
    sql "sync"

    // test 1: target cluster follow source cluster
    logger.info("=== Test 1: backup/restore case ===")
    httpTest {
        uri "/create_ccr"
        endpoint syncerAddress
        def bodyJson = get_ccr_body "${uniqueTable}"
        body "${bodyJson}"
        op "post"
        result response
    }
    assertTrue(checkRestoreFinishTimesOf("${uniqueTable}", 60))
    assertTrue(checkSelectTimesOf("SELECT * FROM ${uniqueTable} WHERE test = 1 ORDER BY id", 5, 60))
    qt_sql "SELECT * FROM ${uniqueTable} WHERE test = 1 ORDER BY id"
    qt_target_sql "SELECT * FROM ${uniqueTable} WHERE test = 1 ORDER BY id"

    logger.info("=== Test 2: dest cluster follow source cluster case ===")

    sql """
    INSERT INTO ${uniqueTable} VALUES
        (2, 0),
        (2, 1),
        (2, 2),
        (2, 3),
        (2, 4)
    """
    sql "sync"
    assertTrue(checkSelectTimesOf("SELECT * FROM ${uniqueTable} WHERE test=2", 5, 60))
    qt_sql "SELECT * FROM ${uniqueTable} WHERE test=2 ORDER BY id"
    qt_target_sql "SELECT * FROM ${uniqueTable} WHERE test=2 ORDER BY id"

    logger.info("=== Test 3: insert overwrite source table ===")

    sql """
    INSERT OVERWRITE TABLE ${uniqueTable} VALUES
        (3, 0),
        (3, 1),
        (3, 2),
        (3, 3),
        (3, 4)
    """
    sql "sync"

    sleep(10000)
    assertTrue(checkBackupFinishTimesOf("${uniqueTable}", 60))
    sleep(10000)
    assertTrue(checkRestoreFinishTimesOf("${uniqueTable}", 60))

    assertTrue(checkSelectTimesOf("SELECT * FROM ${uniqueTable} WHERE test=3", 5, 60))
    assertTrue(checkSelectTimesOf("SELECT * FROM ${uniqueTable}", 5, 60))

    qt_sql "SELECT * FROM ${uniqueTable} ORDER BY test, id"
    qt_target_sql "SELECT * FROM ${uniqueTable} ORDER BY test, id"
}
