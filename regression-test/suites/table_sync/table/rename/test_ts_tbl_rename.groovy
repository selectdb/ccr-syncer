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

suite("test_ts_tbl_rename") {
    logger.info("exit because test_rename is not supported yet")
    return

    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def tableName = "tbl_" + helper.randomSuffix()
    def test_num = 0
    def insert_num = 5

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

    helper.ccrJobCreate(tableName)

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 30))


    logger.info("=== Test 0: Common insert case ===")
    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${tableName} VALUES (${test_num}, ${index})
            """
    }
    sql "sync"
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName} WHERE test=${test_num}",
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
    sql "sync"
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${newTableName} WHERE test=${test_num}",
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
