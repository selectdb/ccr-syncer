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
suite("test_ts_common") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def tableName = "tbl_" + helper.randomSuffix()
    def uniqueTable = "${tableName}_unique"
    def aggregateTable = "${tableName}_aggregate"
    def duplicateTable = "${tableName}_duplicate"
    def test_num = 0
    def insert_num = 5

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
    sql "sync"

    // test 1: target cluster follow source cluster
    logger.info("=== Test 1: backup/restore case ===")
    helper.ccrJobCreate(uniqueTable)
    assertTrue(helper.checkRestoreFinishTimesOf("${uniqueTable}", 30))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${uniqueTable} WHERE test=${test_num}",
                                   insert_num, 30))

    helper.ccrJobCreate(aggregateTable)
    assertTrue(helper.checkRestoreFinishTimesOf("${aggregateTable}", 30))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${aggregateTable} WHERE test=${test_num}",
                                   1, 30))
    def resList = [4, 10, 4, 0]
    def resData = target_sql "SELECT * FROM ${aggregateTable} WHERE test=${test_num}"
    assertTrue(helper.checkData(resData[0], 1, resList))

    helper.ccrJobCreate(duplicateTable)
    assertTrue(helper.checkRestoreFinishTimesOf("${duplicateTable}", 30))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${duplicateTable} WHERE test=${test_num}",
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
    sql "sync"
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${uniqueTable} WHERE test=${test_num}",
                                   insert_num, 30))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${aggregateTable} WHERE test=${test_num}",
                                   1, 30))
    resData = target_sql "SELECT * FROM ${aggregateTable} WHERE test=${test_num}"
    assertTrue(helper.checkData(resData[0], 1, resList))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${duplicateTable} WHERE test=0",
                                   2 * insert_num, 30))




    logger.info("=== Test 3: pause and resume ===")
    helper.ccrJobPause(uniqueTable)

    test_num = 3
    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${uniqueTable} VALUES (${test_num}, ${index})
            """
    }

    sql "sync"
    assertTrue(!helper.checkSelectTimesOf("SELECT * FROM ${uniqueTable} WHERE test=${test_num}",
                                   insert_num, 3))

    helper.ccrJobResume(uniqueTable)
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${uniqueTable} WHERE test=${test_num}",
                                   insert_num, 30))


    logger.info("=== Test 4: desync job ===")
    test_num = 4
    helper.ccrJobDesync(uniqueTable)

    sleep(helper.sync_gap_time)

    def res = target_sql "SHOW CREATE TABLE TEST_${context.dbName}.${uniqueTable}"
    def desynced = false
    for (List<Object> row : res) {
        if ((row[0] as String) == "${uniqueTable}") {
            desynced = (row[1] as String).contains("\"is_being_synced\" = \"false\"")
            break
        }
    }
    assertTrue(desynced)

    logger.info("=== Test 5: delete job ===")
    test_num = 5
    helper.ccrJobDelete(uniqueTable)

    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${uniqueTable} VALUES (${test_num}, ${index})
            """
    }

    sql "sync"
    assertTrue(!helper.checkSelectTimesOf("SELECT * FROM ${uniqueTable} WHERE test=${test_num}",
                                   insert_num, 5))
}
