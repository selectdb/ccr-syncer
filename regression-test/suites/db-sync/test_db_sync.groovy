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

    def syncerAddress = "127.0.0.1:9190"
    def test_num = 0
    def insert_num = 5
    def date_num = "2021-01-02"
    def sync_gap_time = 5000

    def createUniqueTable = { tableName ->
        sql """
            CREATE TABLE if NOT EXISTS ${tableName}
            (
                `test` INT,
                `id` INT,
                `date_time` date NOT NULL
            )
            ENGINE=OLAP
            UNIQUE KEY(`test`, `id`, `date_time`)
            AUTO PARTITION BY RANGE (date_trunc(`date_time`, 'day'))
            (
            )
            DISTRIBUTED BY HASH(id) BUCKETS AUTO
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "estimate_partition_size" = "10G",
                "binlog.enable" = "true"
            )
        """
    }
    def createAggergateTable = { tableName ->
        sql """
            CREATE TABLE if NOT EXISTS ${tableName}
            (
                `test` INT,
                `date_time` date NOT NULL,
                `last` INT REPLACE DEFAULT "0",
                `cost` INT SUM DEFAULT "0",
                `max` INT MAX DEFAULT "0",
                `min` INT MIN DEFAULT "0"
            )
            ENGINE=OLAP
            AGGREGATE KEY(`test`, `date_time`)
            AUTO PARTITION BY RANGE (date_trunc(`date_time`, 'day'))
            (
            )
            DISTRIBUTED BY HASH(`test`) BUCKETS AUTO
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "estimate_partition_size" = "10G",
                "binlog.enable" = "true"
            )
        """
    }

    def createDuplicateTable = { tableName ->
       sql """
            CREATE TABLE if NOT EXISTS ${tableName}
            (
                `test` INT,
                `id` INT,
                `date_time` date NOT NULL
            )
            ENGINE=OLAP
            DUPLICATE KEY(`test`, `id`, `date_time`)
            AUTO PARTITION BY RANGE (date_trunc(`date_time`, 'day'))
            (
            )
            DISTRIBUTED BY HASH(id) BUCKETS AUTO
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "estimate_partition_size" = "10G",
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

    def exist = { res -> Boolean
        return res.size() != 0
    }
    def notExist = { res -> Boolean
        return res.size() == 0
    }

    def tableUnique0 = "tbl_common_0_" + UUID.randomUUID().toString().replace("-", "")
    def tableAggregate0 = "tbl_aggregate_0_" + UUID.randomUUID().toString().replace("-", "")
    def tableDuplicate0 = "tbl_duplicate_0_" + UUID.randomUUID().toString().replace("-", "")

    createUniqueTable(tableUnique0)
    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${tableUnique0} VALUES (${test_num}, ${index}, '${date_num}')
            """
    }

    createAggergateTable(tableAggregate0)
    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${tableAggregate0} VALUES (${test_num}, '${date_num}', ${index}, ${index}, ${index}, ${index})
            """
    }

    createDuplicateTable(tableDuplicate0)
    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${tableDuplicate0} VALUES (0, 99, '${date_num}')
            """
    }

    logger.info("=== Test : mtmv create ===")
    sql """CREATE MATERIALIZED VIEW mv1
        BUILD IMMEDIATE REFRESH AUTO ON SCHEDULE EVERY 1 hour
        DISTRIBUTED BY RANDOM BUCKETS 3
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT t1.test, t2.last
        FROM (SELECT * FROM ${tableUnique0} where id > 1) t1
        LEFT OUTER JOIN ${tableAggregate0} as t2
        ON t1.test = t2.test"""

    sql "ALTER DATABASE ${context.dbName} SET properties (\"binlog.enable\" = \"true\")"

    String response
    httpTest {
        uri "/create_ccr"
        endpoint syncerAddress
        def bodyJson = get_ccr_body ""
        body "${bodyJson}"
        op "post"
        result response
    }

    assertTrue(checkRestoreFinishTimesOf("${tableUnique0}", 130))
    assertTrue(checkSelectTimesOf("SELECT * FROM ${tableUnique0} WHERE test=${test_num}",
                                   insert_num, 50))

    assertTrue(checkRestoreFinishTimesOf("${tableAggregate0}", 30))
    assertTrue(checkSelectTimesOf("SELECT * FROM ${tableAggregate0} WHERE test=${test_num}",
                                   1, 30))

    assertTrue(checkRestoreFinishTimesOf("${tableDuplicate0}", 30))
    assertTrue(checkSelectTimesOf("SELECT * FROM ${tableDuplicate0} WHERE test=${test_num}",
                                   insert_num, 30))

    logger.info("=== Test 1: dest cluster follow source cluster case ===")
    test_num = 1
    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${tableUnique0} VALUES (${test_num}, ${index}, '${date_num}')
            """
    }
    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${tableAggregate0} VALUES (${test_num}, '${date_num}', ${index}, ${index}, ${index}, ${index})
            """
    }
    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${tableDuplicate0} VALUES (0, 99, '${date_num}')
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
    def tableUnique1 = "tbl_common_1_" + UUID.randomUUID().toString().replace("-", "")
    def tableAggregate1 = "tbl_aggregate_1_" + UUID.randomUUID().toString().replace("-", "")
    def tableDuplicate1 = "tbl_duplicate_1_" + UUID.randomUUID().toString().replace("-", "")
    def keywordTableName = "`roles`"

    createUniqueTable(tableUnique1)
    createAggergateTable(tableAggregate1)
    createDuplicateTable(tableDuplicate1)
    createUniqueTable(keywordTableName)

    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${tableUnique1} VALUES (${test_num}, ${index}, '${date_num}')
            """
    }
    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${tableAggregate1} VALUES (${test_num}, '${date_num}', ${index}, ${index}, ${index}, ${index})
            """
    }
    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${tableDuplicate1} VALUES (0, 99, '${date_num}')
            """
    }
    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${keywordTableName} VALUES (${test_num}, ${index}, '${date_num}')
            """
    }

    assertTrue(checkShowTimesOf("SHOW CREATE TABLE TEST_${context.dbName}.${tableUnique1}",
                                exist, 30, "target"))
    assertTrue(checkSelectTimesOf("SELECT * FROM ${tableUnique1} WHERE test=${test_num}",
                                   insert_num, 30))

    assertTrue(checkShowTimesOf("SHOW CREATE TABLE TEST_${context.dbName}.${tableAggregate1}",
                                exist, 30, "target"))
    assertTrue(checkSelectTimesOf("SELECT * FROM ${tableAggregate1} WHERE test=${test_num}",
                                   1, 30))

    assertTrue(checkShowTimesOf("SHOW CREATE TABLE TEST_${context.dbName}.${tableDuplicate1}",
                                exist, 30, "target"))
    assertTrue(checkSelectTimesOf("SELECT * FROM ${tableDuplicate1} WHERE test=0",
                                   insert_num, 30))

    assertTrue(checkShowTimesOf("SHOW CREATE TABLE TEST_${context.dbName}.${keywordTableName}",
                                exist, 30, "target"))
    assertTrue(checkSelectTimesOf("SELECT * FROM ${keywordTableName} WHERE test=${test_num}",
                                   insert_num, 30))

    logger.info("=== Test 3: drop table case ===")
    sql "DROP TABLE ${tableUnique1}"
    sql "DROP TABLE ${tableAggregate1}"
    sql "DROP TABLE ${tableDuplicate1}"
    sql "DROP TABLE ${keywordTableName}"

    assertTrue(checkShowTimesOf("SHOW TABLES LIKE '${tableUnique1}'", 
                                notExist, 30, "target"))
    assertTrue(checkShowTimesOf("SHOW TABLES LIKE '${tableAggregate1}'",
                                notExist, 30, "target"))
    assertTrue(checkShowTimesOf("SHOW TABLES LIKE '${tableDuplicate1}'", 
                                notExist, 30, "target"))
    assertTrue(checkShowTimesOf("SHOW TABLES LIKE '${keywordTableName}'", 
                                notExist, 30, "target"))
    
    logger.info("=== Test : query mtmv case ===")
    assertTrue(checkSelectTimesOf("SELECT * FROM mv1", 3, 45))

    logger.info("=== Test 4: pause and resume ===")
    httpTest {
        uri "/pause"
        endpoint syncerAddress
        def bodyJson = get_ccr_body ""
        body "${bodyJson}"
        op "post"
        result response
    }

    test_num = 4
    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${tableUnique0} VALUES (${test_num}, ${index}, '${date_num}')
            """
    }

    assertTrue(!checkSelectTimesOf("SELECT * FROM ${tableUnique0} WHERE test=${test_num}",
                                   insert_num, 3))

    httpTest {
        uri "/resume"
        endpoint syncerAddress
        def bodyJson = get_ccr_body ""
        body "${bodyJson}"
        op "post"
        result response
    }
    assertTrue(checkSelectTimesOf("SELECT * FROM ${tableUnique0} WHERE test=${test_num}",
                                   insert_num, 30))


    logger.info("=== Test 5: desync job ===")
    test_num = 5
    httpTest {
        uri "/desync"
        endpoint syncerAddress
        def bodyJson = get_ccr_body ""
        body "${bodyJson}"
        op "post"
        result response
    }

    sleep(sync_gap_time)
    
    def checkDesynced = {tableName -> 
        def res = target_sql "SHOW CREATE TABLE TEST_${context.dbName}.${tableName}"
        def desynced = false
        for (List<Object> row : res) {
            if ((row[0] as String) == "${tableName}") {
                desynced = (row[1] as String).contains("\"is_being_synced\" = \"false\"")
                break
            }
        }
        assertTrue(desynced)
    }

    checkDesynced(tableUnique0)
    checkDesynced(tableAggregate0)
    checkDesynced(tableDuplicate0)


     logger.info("=== Test 5: delete job ===")
     test_num = 5
     httpTest {
        uri "/delete"
        endpoint syncerAddress
        def bodyJson = get_ccr_body ""
        body "${bodyJson}"
        op "post"
        result response
    }

    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${tableUnique0} VALUES (${test_num}, ${index}, '${date_num}')
            """
    }

    assertTrue(!checkSelectTimesOf("SELECT * FROM ${tableUnique0} WHERE test=${test_num}",
                                   insert_num, 5))
}
