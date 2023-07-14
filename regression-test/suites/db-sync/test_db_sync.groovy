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

suite("test_db_sync") {
    def test_num = 0
    def insert_num = 5
    def sync_gap_time = 5000

    def createUniqueTable = { tableName ->
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
    }
    def createAggergateTable = { tableName ->
        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """
            CREATE TABLE if NOT EXISTS ${tableName}
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
                "binlog.enable" = "true"
            )
        """
    }

    def createDuplicateTable = { tableName ->
        sql "DROP TABLE IF EXISTS ${tableName}"
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
                "replication_allocation" = "tag.location.default: 1",
                "binlog.enable" = "true"
            )
        """
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

    def exist = { res -> Boolean
        return res.size() != 0
    }
    def notExist = { res -> Boolean
        return res.size() == 0
    }

    def tableUnique0 = "tbl_common_0"
    def tableAggregate0 = "tbl_aggregate_0"
    def tableDuplicate0 = "tbl_duplicate_0"

    createUniqueTable(tableUnique0)
    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${tableUnique0} VALUES (${test_num}, ${index})
            """
    }

    createAggergateTable(tableAggregate0)
    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${tableAggregate0} VALUES (${test_num}, ${index}, ${index}, ${index}, ${index})
            """
    }

    createDuplicateTable(tableDuplicate0)
    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${tableDuplicate0} VALUES (0, 99)
            """
    }

    sql "ALTER DATABASE ${context.dbName} SET properties (\"binlog.enable\" = \"true\")"

    String respone
    httpTest {
        uri "/create_ccr"
        endpoint context.config.syncerAddress
        def bodyJson = get_ccr_body ""
        body "${bodyJson}"
        op "post"
        result respone
    }

    assertTrue(checkShowTimesOf("SHOW CREATE TABLE TEST_${context.dbName}.${tableUnique0}",
                                exist, 30))
    assertTrue(checkSelectTimesOf("SELECT * FROM ${tableUnique0} WHERE test=${test_num}",
                                   insert_num, 30))

    assertTrue(checkShowTimesOf("SHOW CREATE TABLE TEST_${context.dbName}.${tableAggregate0}",
                                exist, 30))
    assertTrue(checkSelectTimesOf("SELECT * FROM ${tableAggregate0} WHERE test=${test_num}",
                                   1, 30))

    assertTrue(checkShowTimesOf("SHOW CREATE TABLE TEST_${context.dbName}.${tableDuplicate0}",
                                exist, 30))
    assertTrue(checkSelectTimesOf("SELECT * FROM ${tableDuplicate0} WHERE test=${test_num}",
                                   insert_num, 30))

    logger.info("=== Test 1: dest cluster follow source cluster case ===")
    test_num = 1
    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${tableUnique0} VALUES (${test_num}, ${index})
            """
    }
    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${tableAggregate0} VALUES (${test_num}, ${index}, ${index}, ${index}, ${index})
            """
    }
    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${tableDuplicate0} VALUES (0, 99)
            """
    }

    assertTrue(checkSelectTimesOf("SELECT * FROM ${tableUnique0} WHERE test=${test_num}",
                                   insert_num, 30))
    assertTrue(checkSelectTimesOf("SELECT * FROM ${tableAggregate0} WHERE test=${test_num}",
                                   1, 30))
    assertTrue(checkSelectTimesOf("SELECT * FROM ${tableDuplicate0} WHERE test=0",
                                   insert_num * (test_num + 1), 30))



    logger.info("=== Test 2: create table case ===")
    test_num = 2
    def tableUnique1 = "tbl_common_1"
    def tableAggregate1 = "tbl_aggregate_1"
    def tableDuplicate1 = "tbl_duplicate_1"

    createUniqueTable(tableUnique1)
    createAggergateTable(tableAggregate1)
    createDuplicateTable(tableDuplicate1)

    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${tableUnique1} VALUES (${test_num}, ${index})
            """
    }
    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${tableAggregate1} VALUES (${test_num}, ${index}, ${index}, ${index}, ${index})
            """
    }
    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${tableDuplicate1} VALUES (0, 99)
            """
    }

    assertTrue(checkShowTimesOf("SHOW CREATE TABLE TEST_${context.dbName}.${tableUnique1}",
                                exist, 30))
    assertTrue(checkSelectTimesOf("SELECT * FROM ${tableUnique1} WHERE test=${test_num}",
                                   insert_num, 30))

    assertTrue(checkShowTimesOf("SHOW CREATE TABLE TEST_${context.dbName}.${tableAggregate1}",
                                exist, 30))
    assertTrue(checkSelectTimesOf("SELECT * FROM ${tableAggregate1} WHERE test=${test_num}",
                                   1, 30))

    assertTrue(checkShowTimesOf("SHOW CREATE TABLE TEST_${context.dbName}.${tableDuplicate1}",
                                exist, 30))
    assertTrue(checkSelectTimesOf("SELECT * FROM ${tableDuplicate1} WHERE test=0",
                                   insert_num, 30))

    logger.info("=== Test 3: drop table case ===")
    sql "DROP TABLE ${tableUnique1}"
    sql "DROP TABLE ${tableAggregate1}"
    sql "DROP TABLE ${tableDuplicate1}"

    assertTrue(checkShowTimesOf("SHOW TABLES LIKE '${tableUnique1}'", 
                                notExist, 30, "target"))
    assertTrue(checkShowTimesOf("SHOW TABLES LIKE '${tableAggregate1}'",
                                notExist, 30, "target"))
    assertTrue(checkShowTimesOf("SHOW TABLES LIKE '${tableDuplicate1}'", 
                                notExist, 30, "target"))
}