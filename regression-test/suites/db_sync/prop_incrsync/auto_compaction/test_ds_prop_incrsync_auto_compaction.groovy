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

suite("test_ds_prop_incrsync_incsync_auto_compaction") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def dbName = context.dbName
    def tableNameFullTrue = "tbl_full_true"
    def tableNameFullFalse = "tbl_full_false"
    def tableNameIncrementTrue = "tbl_incr_true"
    def tableNameIncrementFalse = "tbl_incr_false"
    def test_num = 0
    def insert_num = 5

    def exist = { res -> Boolean
        return res.size() != 0
    }

    sql "DROP TABLE IF EXISTS ${dbName}.${tableNameFullTrue}"
    sql "DROP TABLE IF EXISTS ${dbName}.${tableNameFullFalse}"
    target_sql "DROP TABLE IF EXISTS TEST_${dbName}.${tableNameFullTrue}"
    target_sql "DROP TABLE IF EXISTS TEST_${dbName}.${tableNameFullFalse}"
    sql "DROP TABLE IF EXISTS ${dbName}.${tableNameIncrementTrue}"
    sql "DROP TABLE IF EXISTS ${dbName}.${tableNameIncrementFalse}"
    target_sql "DROP TABLE IF EXISTS TEST_${dbName}.${tableNameIncrementTrue}"
    target_sql "DROP TABLE IF EXISTS TEST_${dbName}.${tableNameIncrementFalse}"

    helper.enableDbBinlog()
    helper.ccrJobDelete()
    helper.ccrJobCreate()

    sql """
        CREATE TABLE if NOT EXISTS ${tableNameFullTrue}
        (
            `test` INT,
            `id` INT
        )
        ENGINE=OLAP
        AGGREGATE KEY(`test`, `id`)
        PARTITION BY RANGE(`id`)
        (
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true",
            "disable_auto_compaction" = "true"
        )
    """

    sql """
        CREATE TABLE if NOT EXISTS ${tableNameFullFalse}
        (
            `test` INT,
            `id` INT
        )
        ENGINE=OLAP
        AGGREGATE KEY(`test`, `id`)
        PARTITION BY RANGE(`id`)
        (
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true",
            "disable_auto_compaction" = "false"
        )
    """

    assertTrue(helper.checkRestoreFinishTimesOf("${tableNameFullTrue}", 30))

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableNameFullTrue}\"", exist, 60, "sql"))
    
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableNameFullTrue}\"", exist, 60, "sql"))

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableNameFullFalse}\"", exist, 60, "target"))

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableNameFullFalse}\"", exist, 60, "target"))

    def target_res = target_sql "SHOW CREATE TABLE ${tableNameFullTrue}"

    assertTrue(target_res[0][1].contains("\"disable_auto_compaction\" = \"true\""))

    target_res = target_sql "SHOW CREATE TABLE ${tableNameFullFalse}"

    assertTrue(target_res[0][1].contains("\"disable_auto_compaction\" = \"false\""))

    sql """
        CREATE TABLE if NOT EXISTS ${tableNameIncrementTrue}
        (
            `test` INT,
            `id` INT
        )
        ENGINE=OLAP
        AGGREGATE KEY(`test`, `id`)
        PARTITION BY RANGE(`id`)
        (
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true",
            "disable_auto_compaction" = "true"
        )
    """

    sql """
        CREATE TABLE if NOT EXISTS ${tableNameIncrementFalse}
        (
            `test` INT,
            `id` INT
        )
        ENGINE=OLAP
        AGGREGATE KEY(`test`, `id`)
        PARTITION BY RANGE(`id`)
        (
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true",
            "disable_auto_compaction" = "false"
        )
    """

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableNameIncrementTrue}\"", exist, 60, "sql"))

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableNameIncrementTrue}\"", exist, 60, "sql"))

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableNameIncrementFalse}\"", exist, 60, "target"))

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableNameIncrementFalse}\"", exist, 60, "target"))

    target_res = target_sql "SHOW CREATE TABLE ${tableNameIncrementFalse}"

    assertTrue(target_res[0][1].contains("\"disable_auto_compaction\" = \"false\""))

    target_res = target_sql "SHOW CREATE TABLE ${tableNameIncrementTrue}"

    assertTrue(target_res[0][1].contains("\"disable_auto_compaction\" = \"true\""))
}