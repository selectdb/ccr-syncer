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

suite("test_ds_partition_default_list_insert") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def dbName = context.dbName
    def tableName = "tbl_" + helper.randomSuffix()
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
    target_sql "DROP TABLE IF EXISTS TEST_${dbName}.${tableName}"

    helper.enableDbBinlog()

    def exist = { res -> Boolean
        return res.size() != 0
    }

    def notExist = { res -> Boolean
        return res.size() == 0
    }

    helper.ccrJobDelete()
    helper.ccrJobCreate()

    logger.info("=== insert into fullsync table with default partition ===")
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            id INT,
            k1 INT
        )
        DUPLICATE KEY(id)
        PARTITION BY LIST (id)
        (PARTITION p1)
        DISTRIBUTED BY HASH(id) BUCKETS 10
        PROPERTIES(
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
        """
    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 30))
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}\"", exist, 30, "sql"))
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}\"", exist, 30, "target"))

    sql "insert into ${tableName} values (1, 1)"
    assertTrue(helper.checkShowTimesOf(""" select * from ${tableName} """, { r -> r.size() == 1}, 60, "target"))
    sql "insert into ${tableName} values (2, 2)"
    assertTrue(helper.checkShowTimesOf(""" select * from ${tableName} """, { r -> r.size() == 2}, 60, "target"))

    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
    target_sql "DROP TABLE IF EXISTS TEST_${dbName}.${tableName}"
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}\"", notExist, 30, "sql"))
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}\"", notExist, 30, "target"))

    logger.info("=== insert into increment table with default partition ===")
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            id INT,
            k1 INT
        )
        DUPLICATE KEY(id)
        PARTITION BY LIST (id)
        (PARTITION p1)
        DISTRIBUTED BY HASH(id) BUCKETS 10
        PROPERTIES(
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
        """

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}\"", exist, 30, "sql"))
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}\"", exist, 30, "target"))

    sql "insert into ${tableName} values (1, 1)"
    assertTrue(helper.checkShowTimesOf(""" select * from ${tableName} """, { r -> r.size() == 1}, 60, "target"))
    sql "insert into ${tableName} values (2, 2)"
    assertTrue(helper.checkShowTimesOf(""" select * from ${tableName} """, { r -> r.size() == 2}, 60, "target"))

    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
    target_sql "DROP TABLE IF EXISTS TEST_${dbName}.${tableName}"
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}\"", notExist, 30, "sql"))
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}\"", notExist, 30, "target"))

    logger.info("=== insert into increment table with alter add default partition ===")
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            id INT,
            k1 INT
        )
        DUPLICATE KEY(id)
        PARTITION BY LIST (id)
        (PARTITION p1 values in("1"))
        DISTRIBUTED BY HASH(id) BUCKETS 10
        PROPERTIES(
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
        """

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}\"", exist, 30, "sql"))
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}\"", exist, 30, "target"))

    sql "ALTER TABLE ${tableName} ADD PARTITION `p2`;"

    sql "insert into ${tableName} values (1, 1)"
    assertTrue(helper.checkShowTimesOf(""" select * from ${tableName} """, { r -> r.size() == 1}, 60, "target"))
    sql "insert into ${tableName} values (2, 2)"
    assertTrue(helper.checkShowTimesOf(""" select * from ${tableName} """, { r -> r.size() == 2}, 60, "target"))
}