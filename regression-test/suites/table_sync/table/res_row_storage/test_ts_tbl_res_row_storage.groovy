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
suite("test_ts_tbl_res_row_storage") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def tableName = "tbl_" + helper.randomSuffix()
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
            "store_row_column" = "true",
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
    def res = target_sql "SHOW CREATE TABLE TEST_${context.dbName}.${tableName}"
    def rowStorage = false
    for (List<Object> row : res) {
        if ((row[0] as String) == "${tableName}") {
            rowStorage = (row[1] as String).contains("\"store_row_column\" = \"true\"")
            break
        }
    }
    assertTrue(rowStorage)


    logger.info("=== Test 2: add column case ===")
    sql """
        ALTER TABLE ${tableName}
        ADD COLUMN (`cost` VARCHAR(256) DEFAULT "add")
        """
    
    assertTrue(helper.checkShowTimesOf("""
                                SHOW ALTER TABLE COLUMN
                                FROM ${context.dbName}
                                WHERE TableName = "${tableName}" AND State = "FINISHED"
                                """,
                                exist, 30))

    assertTrue(helper.checkSelectColTimesOf("SELECT * FROM ${tableName} WHERE test=${test_num}",
                                      3, 30))


    logger.info("=== Test 3: add a row ===")
    test_num = 3
    sql """
        INSERT INTO ${tableName} VALUES (${test_num}, 0, "addadd")
        """
    sql "sync"
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName} WHERE test=${test_num}",
                                   1, 30))
    res = target_sql "SELECT cost FROM TEST_${context.dbName}.${tableName} WHERE test=${test_num}"
    assertTrue((res[0][0] as String) == "addadd")
}
