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
suite("test_tsa_col_drop_key") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def tableName = "tbl_" + helper.randomSuffix()
    def aliasTableName = "alias_tbl_" + helper.randomSuffix()
    def test_num = 0
    def insert_num = 5

    helper.set_alias(aliasTableName)

    def exist = { res -> Boolean
        return res.size() != 0
    }

    def has_count = { count ->
        return { res -> Boolean
            res.size() == count
        }
    }

    def id_column_not_exists = { res -> Boolean
        def not_exists = true
        for (int i = 0; i < res.size(); i++) {
            if (res[i][0] == 'id') {
                not_exists = false
            }
        }
        return not_exists
    }
    
    helper.enableDbBinlog()
    sql "DROP TABLE IF EXISTS ${tableName}"
    target_sql "DROP TABLE IF EXISTS ${aliasTableName}"

    sql """
        CREATE TABLE if NOT EXISTS ${tableName}
        (
            `test` INT,
            `id` INT,
            `value` INT
        )
        ENGINE=OLAP
        DUPLICATE KEY(`test`, `id`)
        DISTRIBUTED BY HASH(test) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
    """

    logger.info("=== Test 1: add data and sync create ===")

    def values = [];
    for (int index = 0; index < insert_num; index++) {
        values.add("(${test_num}, ${index}, ${index})")
    }
    sql """
        INSERT INTO ${tableName} VALUES ${values.join(",")}
        """

    helper.ccrJobDelete(tableName)
    helper.ccrJobCreate(tableName)
    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 30))

    logger.info("=== Test 2: drop key column ===")
    sql """
        ALTER TABLE ${tableName}
        DROP COLUMN `id`
        """
    sql "sync"

    logger.info("=== Test 2: Check key column ===")

    assertTrue(helper.checkShowTimesOf("""
                                SHOW ALTER TABLE COLUMN
                                WHERE TableName = "${tableName}" AND State = "FINISHED"
                                """,
                                has_count(1), 30))

    assertTrue(helper.checkShowTimesOf("SHOW COLUMNS FROM ${tableName}", id_column_not_exists, 60, "sql"))

    assertTrue(helper.checkShowTimesOf("SHOW COLUMNS FROM ${aliasTableName}", id_column_not_exists, 60, "target_sql"))
}

