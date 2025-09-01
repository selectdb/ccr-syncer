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
suite("test_ds_col_default_value") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def tableNameAgg = "test_ds_col_default_value_tbl_agg"
    def tableNameDup = "test_ds_col_default_value_tbl_dup"

    def has_count = { count ->
        return { res -> Boolean
            res.size() == count
        }
    }

    def exist = { res -> Boolean
        return res.size() != 0
    }

    helper.enableDbBinlog()
    sql "DROP TABLE IF EXISTS ${tableNameAgg}"
    sql "DROP TABLE IF EXISTS ${tableNameDup}"
    target_sql "DROP TABLE IF EXISTS TEST_${tableNameAgg}"
    target_sql "DROP TABLE IF EXISTS TEST_${tableNameDup}"

    sql """
        CREATE TABLE if NOT EXISTS ${tableNameAgg}
        (
            `id` INT,
            `value` INT
        )
        ENGINE=OLAP
        AGGREGATE KEY(`id`, `value`)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
    """

    helper.ccrJobDelete()
    helper.ccrJobCreate()

    assertTrue(helper.checkRestoreFinishTimesOf("${tableNameAgg}", 30))

    sql """
        CREATE TABLE if NOT EXISTS ${tableNameDup}
        (
            `id` INT,
            `value` INT
        )
        ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
    """

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableNameAgg}\"", exist, 60, "sql"))
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableNameDup}\"", exist, 60, "target"))

    logger.info("=== Test 1: Alter agg table add column and insert data ===")

    def column = sql "SHOW ALTER TABLE COLUMN WHERE TableName = \"${tableNameAgg}\""
    sql "ALTER TABLE ${tableNameAgg} add COLUMN col1 int DEFAULT \"0\""
    assertTrue(helper.checkShowTimesOf("""
                                SHOW ALTER TABLE COLUMN
                                WHERE TableName = "${tableNameAgg}" AND State = "FINISHED"
                                """,
                                has_count(column.size() + 1), 30))

    column = sql "SHOW ALTER TABLE COLUMN WHERE TableName = \"${tableNameAgg}\""
    sql "ALTER TABLE ${tableNameAgg} add COLUMN col2 datetime DEFAULT CURRENT_TIMESTAMP"
    assertTrue(helper.checkShowTimesOf("""
                                SHOW ALTER TABLE COLUMN
                                WHERE TableName = "${tableNameAgg}" AND State = "FINISHED"
                                """,
                                has_count(column.size() + 1), 30))

    column = sql "SHOW ALTER TABLE COLUMN WHERE TableName = \"${tableNameAgg}\""
    sql "ALTER TABLE ${tableNameAgg} add COLUMN col3 varchar DEFAULT \"xxx\""
    assertTrue(helper.checkShowTimesOf("""
                                SHOW ALTER TABLE COLUMN
                                WHERE TableName = "${tableNameAgg}" AND State = "FINISHED"
                                """,
                                has_count(column.size() + 1), 30))

    column = sql "SHOW ALTER TABLE COLUMN WHERE TableName = \"${tableNameAgg}\""
    sql "ALTER TABLE ${tableNameAgg} add COLUMN col4 hll hll_union"
    assertTrue(helper.checkShowTimesOf("""
                                SHOW ALTER TABLE COLUMN
                                WHERE TableName = "${tableNameAgg}" AND State = "FINISHED"
                                """,
                                has_count(column.size() + 1), 30))

    assertTrue(helper.checkShowTimesOf("SHOW COLUMNS FROM ${tableNameAgg} WHERE Field = \"col1\"", exist, 30, "target"))
    assertTrue(helper.checkShowTimesOf("SHOW COLUMNS FROM ${tableNameAgg} WHERE Field = \"col2\"", exist, 30, "target"))
    assertTrue(helper.checkShowTimesOf("SHOW COLUMNS FROM ${tableNameAgg} WHERE Field = \"col3\"", exist, 30, "target"))
    assertTrue(helper.checkShowTimesOf("SHOW COLUMNS FROM ${tableNameAgg} WHERE Field = \"col4\"", exist, 30, "target"))

    def columns = target_sql "SHOW CREATE TABLE ${tableNameAgg}"

    assertTrue(columns[0][1].contains("`col1` int NULL DEFAULT \"0\""))
    assertTrue(columns[0][1].contains("`col2` datetime NULL DEFAULT CURRENT_TIMESTAMP"))
    assertTrue(columns[0][1].contains("`col3` varchar(65533) NULL DEFAULT \"xxx\""))
    assertTrue(columns[0][1].contains("`col4` hll HLL_UNION NOT NULL"))


    logger.info("=== Test 2: Alter dup table add column and insert data ===")

    column = sql "SHOW ALTER TABLE COLUMN WHERE TableName = \"${tableNameDup}\""
    sql "ALTER TABLE ${tableNameDup} add COLUMN col1 int DEFAULT \"0\""
    assertTrue(helper.checkShowTimesOf("""
                                SHOW ALTER TABLE COLUMN
                                WHERE TableName = "${tableNameDup}" AND State = "FINISHED"
                                """,
                                has_count(column.size() + 1), 30))

    column = sql "SHOW ALTER TABLE COLUMN WHERE TableName = \"${tableNameDup}\""
    sql "ALTER TABLE ${tableNameDup} add COLUMN col2 datetime DEFAULT CURRENT_TIMESTAMP"
    assertTrue(helper.checkShowTimesOf("""
                                SHOW ALTER TABLE COLUMN
                                WHERE TableName = "${tableNameDup}" AND State = "FINISHED"
                                """,
                                has_count(column.size() + 1), 30))

    column = sql "SHOW ALTER TABLE COLUMN WHERE TableName = \"${tableNameDup}\""
    sql "ALTER TABLE ${tableNameDup} add COLUMN col3 varchar DEFAULT \"xxx\""
    assertTrue(helper.checkShowTimesOf("""
                                SHOW ALTER TABLE COLUMN
                                WHERE TableName = "${tableNameDup}" AND State = "FINISHED"
                                """,
                                has_count(column.size() + 1), 30))

    column = sql "SHOW ALTER TABLE COLUMN WHERE TableName = \"${tableNameDup}\""
    sql "ALTER TABLE ${tableNameDup} add COLUMN col4 bitmap DEFAULT null"
    assertTrue(helper.checkShowTimesOf("""
                                SHOW ALTER TABLE COLUMN
                                WHERE TableName = "${tableNameDup}" AND State = "FINISHED"
                                """,
                                has_count(column.size() + 1), 30))

    assertTrue(helper.checkShowTimesOf("SHOW COLUMNS FROM ${tableNameDup} WHERE Field = \"col1\"", exist, 30, "target"))
    assertTrue(helper.checkShowTimesOf("SHOW COLUMNS FROM ${tableNameDup} WHERE Field = \"col2\"", exist, 30, "target"))
    assertTrue(helper.checkShowTimesOf("SHOW COLUMNS FROM ${tableNameDup} WHERE Field = \"col3\"", exist, 30, "target"))
    assertTrue(helper.checkShowTimesOf("SHOW COLUMNS FROM ${tableNameDup} WHERE Field = \"col4\"", exist, 30, "target"))

    columns = target_sql "SHOW CREATE TABLE ${tableNameDup}"

    assertTrue(columns[0][1].contains("`col1` int NULL DEFAULT \"0\""))
    assertTrue(columns[0][1].contains("`col2` datetime NULL DEFAULT CURRENT_TIMESTAMP"))
    assertTrue(columns[0][1].contains("`col3` varchar(65533) NULL DEFAULT \"xxx\""))
    assertTrue(columns[0][1].contains("`col4` bitmap NOT NULL DEFAULT BITMAP_EMPTY"))
}
