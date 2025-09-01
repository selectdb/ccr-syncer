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

suite("test_ds_prop_incrsync_auto_increment") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def dbName = context.dbName
    def tableNameFullDefault = "tbl_full_default"
    def tableNameFull = "tbl_full"
    def tableNameIncrementDefault = "tbl_incr_fault"
    def tableNameIncrement = "tbl_incr"
    def test_num = 0
    def insert_num = 5

    def exist = { res -> Boolean
        return res.size() != 0
    }

    sql "DROP TABLE IF EXISTS ${dbName}.${tableNameFull}"
    sql "DROP TABLE IF EXISTS ${dbName}.${tableNameFullDefault}"
    target_sql "DROP TABLE IF EXISTS TEST_${dbName}.${tableNameFull}"
    target_sql "DROP TABLE IF EXISTS TEST_${dbName}.${tableNameFullDefault}"
    sql "DROP TABLE IF EXISTS ${dbName}.${tableNameIncrement}"
    sql "DROP TABLE IF EXISTS ${dbName}.${tableNameIncrementDefault}"
    target_sql "DROP TABLE IF EXISTS TEST_${dbName}.${tableNameIncrement}"
    target_sql "DROP TABLE IF EXISTS TEST_${dbName}.${tableNameIncrementDefault}"

    helper.enableDbBinlog()
    helper.ccrJobDelete()
    helper.ccrJobCreate()

    sql """
            CREATE TABLE ${tableNameFull} (
                `id` BIGINT NOT NULL AUTO_INCREMENT(100),
                `value` int(11) NOT NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
            )
    """

    sql """
            CREATE TABLE ${tableNameFullDefault} (
                `id` BIGINT NOT NULL AUTO_INCREMENT,
                `value` int(11) NOT NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
            )
    """

    for (int index = 0; index < insert_num; index++) {
        sql "INSERT INTO ${tableNameFull} (value) VALUES (${insert_num})"
    }
    sql "sync"

    for (int index = 0; index < insert_num; index++) {
        sql "INSERT INTO ${tableNameFullDefault} (value) VALUES (${insert_num})"
    }
    sql "sync"

    assertTrue(helper.checkRestoreFinishTimesOf("${tableNameFull}", 30))

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableNameFull}\"", exist, 60, "sql"))

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableNameFullDefault}\"", exist, 60, "sql"))

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableNameFull}\"", exist, 60, "target"))

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableNameFullDefault}\"", exist, 60, "target"))

    def target_res = target_sql "SHOW CREATE TABLE ${tableNameFull}"

    assertTrue(target_res[0][1].contains("`id` bigint NOT NULL AUTO_INCREMENT(100)"))

    target_res = target_sql "SHOW CREATE TABLE ${tableNameFullDefault}"

    assertTrue(target_res[0][1].contains("`id` bigint NOT NULL AUTO_INCREMENT(1)"))

    def res = sql "select * from ${tableNameFull} order by id"

    target_res = target_sql "select * from ${tableNameFull} order by id"

    assertEquals(target_res, res)

    res = sql "select * from ${tableNameFullDefault} order by id"

    target_res = target_sql "select * from ${tableNameFullDefault} order by id"

    assertEquals(target_res, res)

    sql """
        CREATE TABLE ${tableNameIncrement} (
            `id` BIGINT NOT NULL AUTO_INCREMENT(100),
            `value` int(11) NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        )
    """

    sql """
        CREATE TABLE ${tableNameIncrementDefault} (
            `id` BIGINT NOT NULL AUTO_INCREMENT,
            `value` int(11) NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        )
    """

    for (int index = 0; index < insert_num; index++) {
        sql "INSERT INTO ${tableNameIncrement} (value) VALUES (${insert_num})"
    }
    sql "sync"

    for (int index = 0; index < insert_num; index++) {
        sql "INSERT INTO ${tableNameIncrementDefault} (value) VALUES (${insert_num})"
    }
    sql "sync"

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableNameIncrement}\"", exist, 60, "sql"))

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableNameIncrementDefault}\"", exist, 60, "sql"))

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableNameIncrement}\"", exist, 60, "target"))

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableNameIncrementDefault}\"", exist, 60, "target"))

    target_res = target_sql "SHOW CREATE TABLE ${tableNameIncrement}"

    assertTrue(target_res[0][1].contains("`id` bigint NOT NULL AUTO_INCREMENT(100)"))

    target_res = target_sql "SHOW CREATE TABLE ${tableNameIncrementDefault}"

    assertTrue(target_res[0][1].contains("`id` bigint NOT NULL AUTO_INCREMENT(1)"))

    res = sql "select * from ${tableNameIncrement} order by id"

    target_res = target_sql "select * from ${tableNameIncrement} order by id"

    assertEquals(target_res, res)

    res = sql "select * from ${tableNameIncrementDefault} order by id"

    target_res = target_sql "select * from ${tableNameIncrementDefault} order by id"

    assertEquals(target_res, res)
}
