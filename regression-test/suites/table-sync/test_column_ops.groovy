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
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def tableName = "tbl_column_ops_" + helper.randomSuffix()
    def test_num = 0
    def insert_num = 5

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

    helper.ccrJobCreate(tableName)
    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 30))


    logger.info("=== Test 2: add column case ===")
    sql """
        ALTER TABLE ${tableName}
        ADD COLUMN (`cost` VARCHAR(3) DEFAULT "123")
        """
    
    assertTrue(helper.checkShowTimesOf("""
                                SHOW ALTER TABLE COLUMN
                                FROM ${context.dbName}
                                WHERE TableName = "${tableName}" AND State = "FINISHED"
                                """,
                                exist, 30))

    def has_column = { num ->
        return { res ->
            res.size() > 0 && res[0].size() == num
        }
    }
    assertTrue(helper.checkShowTimesOf("SELECT * FROM ${tableName} WHERE test=${test_num}",
                                      has_column(3), 30))

    logger.info("=== Test 3: modify column length case ===")
    test_num = 3
    sql """
        ALTER TABLE ${tableName}
        MODIFY COLUMN `cost` VARCHAR(4) DEFAULT "123"
        """
    sql """
        INSERT INTO ${tableName} VALUES (${test_num}, 0, "8901")
        """
    sql "sync"
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName} WHERE test=${test_num}",
                                      1, 30))


//     logger.info("=== Test 4: modify column type case ===")
//     test_num = 4
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


    logger.info("=== Test 5: drop column case ===")
    sql """
        ALTER TABLE ${tableName}
        DROP COLUMN `cost`
        """
    assertTrue(helper.checkShowTimesOf("SELECT * FROM ${tableName} WHERE test=${test_num}",
                                      has_column(2), 30))
}
