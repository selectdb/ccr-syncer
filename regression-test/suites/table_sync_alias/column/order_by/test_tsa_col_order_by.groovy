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
suite("test_tsa_col_order_by") {
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

    def key_columns_order = { res -> Boolean
        return res[0][0] == 'id' && (res[0][3] == 'YES' || res[0][3] == 'true') &&
            res[1][0] == 'test' && (res[1][3] == 'YES' || res[1][3] == 'true') &&
            res[2][0] == 'value1' && (res[2][3] == 'NO' || res[2][3] == 'false') &&
            res[3][0] == 'value' && (res[3][3] == 'NO' || res[3][3] == 'false')
    }

    helper.enableDbBinlog()
    sql "DROP TABLE IF EXISTS ${tableName}"
    target_sql "DROP TABLE IF EXISTS ${aliasTableName}"

    sql """
        CREATE TABLE if NOT EXISTS ${tableName}
        (
            `test` INT,
            `id` INT,
            `value` INT,
            `value1` INT
        )
        ENGINE=OLAP
        UNIQUE KEY(`test`, `id`)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
    """

    logger.info("=== Test 1: add data and sync create ===")

    def values = [];
    for (int index = 0; index < insert_num; index++) {
        values.add("(${test_num}, ${index}, ${index}, ${index})")
    }
    sql """
        INSERT INTO ${tableName} VALUES ${values.join(",")}
        """

    helper.ccrJobDelete(tableName)
    helper.ccrJobCreate(tableName)
    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 30))

    logger.info("=== Test 2: order by column case ===")

    sql """
        ALTER TABLE ${tableName}
        ORDER BY (`id`, `test`, `value1`, `value`)
        """

    logger.info("=== Test 3: Check ordered column ===")


    assertTrue(helper.checkShowTimesOf("""
                                SHOW ALTER TABLE COLUMN
                                WHERE TableName = "${tableName}" AND State = "FINISHED"
                                """,
                                exist, 30))

    assertTrue(helper.checkShowTimesOf("SHOW COLUMNS FROM ${tableName}", key_columns_order, 60, "sql"))

    assertTrue(helper.checkShowTimesOf("SHOW COLUMNS FROM ${aliasTableName}", key_columns_order, 60, "target_sql"))
}

