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
suite("test_tsa_col_rename") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def tableName = "tbl_" + helper.randomSuffix()
    def aliasTableName = "alias_tbl_" + helper.randomSuffix()
    def newColName = 'test_tsa_col_rename_new_col'
    def oldColName = 'test_tsa_col_rename_old_col'
    def test_num = 0
    def insert_num = 5

    def has_count = { count ->
        return { res -> Boolean
            res.size() == count
        }
    }

    def has_column = { column ->
        return { res -> Boolean
            res[0][0] == column
        }
    }

    def not_has_column = { column ->
        return { res -> Boolean
            res[0][0] != column
        }
    }

    helper.enableDbBinlog()
    sql "DROP TABLE IF EXISTS ${tableName}"
    target_sql "DROP TABLE IF EXISTS ${aliasTableName}"

    sql """
        CREATE TABLE if NOT EXISTS ${tableName}
        (
            ${oldColName} INT,
            `id` INT,
            `value` INT
        )
        ENGINE=OLAP
        UNIQUE KEY(${oldColName}, `id`)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
    """

    def values = [];
    for (int index = 0; index < insert_num; index++) {
        values.add("(${test_num}, ${index}, ${index})")
    }
    sql """
        INSERT INTO ${tableName} VALUES ${values.join(",")}
        """

    result = sql "select * from ${tableName}"

    assertEquals(result.size(), insert_num)

    helper.ccrJobDelete(tableName)
    helper.ccrJobCreate(tableName)

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 30))

    logger.info("=== Test 1: Check old column exist and new column not exist ===")
    assertTrue(helper.checkShowTimesOf("SHOW COLUMNS FROM ${tableName}", has_column(oldColName), 60, "sql"))

    assertTrue(helper.checkShowTimesOf("SHOW COLUMNS FROM ${tableName}", not_has_column(newColName), 60, "sql"))

    assertTrue(helper.checkShowTimesOf("SHOW COLUMNS FROM ${aliasTableName}", has_column(oldColName), 60, "target_sql"))

    assertTrue(helper.checkShowTimesOf("SHOW COLUMNS FROM ${aliasTableName}", not_has_column(newColName), 60, "target_sql"))

    logger.info("=== Test 2: Alter table rename column and insert data ===")

    sql "ALTER TABLE ${tableName} RENAME COLUMN ${oldColName} ${newColName} "

    assertTrue(helper.checkShowTimesOf("SHOW COLUMNS FROM ${tableName}", has_column(newColName), 60, "sql"))

    values = [];
    for (int index = insert_num; index < insert_num * 2; index++) {
        values.add("(${test_num}, ${index}, ${index})")
    }
    sql """
        INSERT INTO ${tableName} VALUES ${values.join(",")}
        """

    logger.info("=== Test 3: Check old column not exist and new column exist ===")

    assertTrue(helper.checkShowTimesOf("SHOW COLUMNS FROM ${tableName}", not_has_column(oldColName), 60, "sql"))

    assertTrue(helper.checkShowTimesOf("SHOW COLUMNS FROM ${tableName}", has_column(newColName), 60, "sql"))

    assertTrue(helper.checkShowTimesOf("SHOW COLUMNS FROM ${aliasTableName}", not_has_column(oldColName), 60, "target_sql"))

    assertTrue(helper.checkShowTimesOf("SHOW COLUMNS FROM ${aliasTableName}", has_column(newColName), 60, "target_sql"))

    logger.info("=== Test 4: Check inserted data ===")

    result = sql " select * from ${tableName} "

    result_target = target_sql " select * from ${aliasTableName} "

    assertEquals(result, result_target)

}

