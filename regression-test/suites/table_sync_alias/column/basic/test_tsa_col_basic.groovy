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
suite("test_tsa_col_basic") {
    // 1. add first key column
    // 2. add last key column
    // 3. add value column
    // 4. add last value column

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
        UNIQUE KEY(`test`, `id`)
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
    sql "sync"

    helper.ccrJobDelete(tableName)
    helper.ccrJobCreate(tableName)

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 30))

    logger.info("=== Test 1: add first column case ===")
    sql """
        ALTER TABLE ${tableName}
        ADD COLUMN `first` INT KEY DEFAULT "0" FIRST
        """

    assertTrue(helper.checkShowTimesOf("""
                                SHOW ALTER TABLE COLUMN
                                WHERE TableName = "${tableName}" AND State = "FINISHED"
                                """,
                                has_count(1), 30))

    def has_column_first = { res -> Boolean
        // Field == 'first' && 'Key' == 'YES'
        return res[0][0] == 'first' && (res[0][3] == 'YES' || res[0][3] == 'true')
    }

    assertTrue(helper.checkShowTimesOf("SHOW COLUMNS FROM `${aliasTableName}`", has_column_first, 60, "target_sql"))

    logger.info("=== Test 2: add column after last key ===")
    sql """
        ALTER TABLE ${tableName}
        ADD COLUMN `last` INT KEY DEFAULT "0" AFTER `id`
        """

    assertTrue(helper.checkShowTimesOf("""
                                SHOW ALTER TABLE COLUMN
                                WHERE TableName = "${tableName}" AND State = "FINISHED"
                                """,
                                has_count(2), 30))

    def has_column_last = { res -> Boolean
        // Field == 'last' && 'Key' == 'YES'
        return res[3][0] == 'last' && (res[3][3] == 'YES' || res[3][3] == 'true')
    }

    assertTrue(helper.checkShowTimesOf("SHOW COLUMNS FROM `${aliasTableName}`", has_column_last, 60, "target_sql"))

    logger.info("=== Test 3: add value column after last key ===")
    sql """
        ALTER TABLE ${tableName}
        ADD COLUMN `first_value` INT DEFAULT "0" AFTER `last`
        """

    assertTrue(helper.checkShowTimesOf("""
                                SHOW ALTER TABLE COLUMN
                                WHERE TableName = "${tableName}" AND State = "FINISHED"
                                """,
                                has_count(3), 30))

    def has_column_first_value = { res -> Boolean
        // Field == 'first_value' && 'Key' == 'NO'
        return res[4][0] == 'first_value' && (res[4][3] == 'NO' || res[4][3] == 'false')
    }

    assertTrue(helper.checkShowTimesOf("SHOW COLUMNS FROM `${aliasTableName}`", has_column_first_value, 60, "target_sql"))

    logger.info("=== Test 4: add value column last ===")
    sql """
        ALTER TABLE ${tableName}
        ADD COLUMN `last_value` INT DEFAULT "0" AFTER `value`
        """

    assertTrue(helper.checkShowTimesOf("""
                                SHOW ALTER TABLE COLUMN
                                WHERE TableName = "${tableName}" AND State = "FINISHED"
                                """,
                                has_count(4), 30))

    def has_column_last_value = { res -> Boolean
        // Field == 'last_value' && 'Key' == 'NO'
        return res[6][0] == 'last_value' && (res[6][3] == 'NO' || res[6][3] == 'false')
    }

    assertTrue(helper.checkShowTimesOf("SHOW COLUMNS FROM `${aliasTableName}`", has_column_last_value, 60, "target_sql"))
}

