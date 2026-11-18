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

suite("test_ds_prop_incrsync_incsync_seq_col") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def dbName = context.dbName
    def tableNameFullF = "tbl_full_1"
    def tableNameFullS = "tbl_full_2"
    def tableNameIncrementF = "tbl_incr_1"
    def tableNameIncrementS = "tbl_incr_2"

    def exist = { res -> Boolean
        return res.size() != 0
    }

    sql "DROP TABLE IF EXISTS ${dbName}.${tableNameFullF}"
    sql "DROP TABLE IF EXISTS ${dbName}.${tableNameFullS}"
    target_sql "DROP TABLE IF EXISTS TEST_${dbName}.${tableNameFullF}"
    target_sql "DROP TABLE IF EXISTS TEST_${dbName}.${tableNameFullS}"

    sql "DROP TABLE IF EXISTS ${dbName}.${tableNameIncrementF}"
    sql "DROP TABLE IF EXISTS ${dbName}.${tableNameIncrementS}"
    target_sql "DROP TABLE IF EXISTS TEST_${dbName}.${tableNameIncrementF}"
    target_sql "DROP TABLE IF EXISTS TEST_${dbName}.${tableNameIncrementS}"


    helper.enableDbBinlog()
    helper.ccrJobDelete()
    helper.ccrJobCreate()

    sql """
            CREATE TABLE if NOT EXISTS ${tableNameFullF}
            (
                `test` INT,
                `id` INT
            )
            ENGINE=OLAP
            UNIQUE KEY(`test`)
            PARTITION BY RANGE(`test`)
            (
            )
            DISTRIBUTED BY HASH(test) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "binlog.enable" = "true",
                "function_column.sequence_col" = "test"
            )
    """
    sql """
            CREATE TABLE if NOT EXISTS ${tableNameFullS}
            (
                `test` INT,
                `id` INT
            )
            ENGINE=OLAP
            UNIQUE KEY(`test`)
            PARTITION BY RANGE(`test`)
            (
            )
            DISTRIBUTED BY HASH(test) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "binlog.enable" = "true",
                "function_column.sequence_type" = "int"
            )
    """

    assertTrue(helper.checkRestoreFinishTimesOf("${tableNameFullF}", 30))
    assertTrue(helper.checkRestoreFinishTimesOf("${tableNameFullS}", 30))

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableNameFullF}\"", exist, 60, "sql"))
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableNameFullS}\"", exist, 60, "sql"))

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableNameFullF}\"", exist, 60, "target"))
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableNameFullS}\"", exist, 60, "target"))

    def target_res_1 = target_sql "SHOW CREATE TABLE ${tableNameFullF}"
    def target_res_2 = target_sql "SHOW CREATE TABLE ${tableNameFullS}"

    assertTrue(target_res_1[0][1].contains("\"function_column.sequence_col\" = \"test\""))
    assertTrue(target_res_2[0][1].contains("\"function_column.sequence_type\" = \"int\""))

    sql """
            CREATE TABLE if NOT EXISTS ${tableNameIncrementF}
            (
                `test` INT,
                `id` INT
            )
            ENGINE=OLAP
            UNIQUE KEY(`test`)
            PARTITION BY RANGE(`test`)
            (
            )
            DISTRIBUTED BY HASH(test) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "binlog.enable" = "true",
                "function_column.sequence_col" = "test"
            )
    """
    sql """
            CREATE TABLE if NOT EXISTS ${tableNameIncrementS}
            (
                `test` INT,
                `id` INT
            )
            ENGINE=OLAP
            UNIQUE KEY(`test`)
            PARTITION BY RANGE(`test`)
            (
            )
            DISTRIBUTED BY HASH(test) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "binlog.enable" = "true",
                "function_column.sequence_type" = "int"
            )
    """

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableNameIncrementF}\"", exist, 60, "sql"))
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableNameIncrementS}\"", exist, 60, "sql"))

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableNameIncrementF}\"", exist, 60, "target"))
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableNameIncrementS}\"", exist, 60, "target"))

    target_res_1 = target_sql "SHOW CREATE TABLE ${tableNameIncrementF}"
    target_res_2 = target_sql "SHOW CREATE TABLE ${tableNameIncrementS}"

    assertTrue(target_res_1[0][1].contains("\"function_column.sequence_col\" = \"test\""))
    assertTrue(target_res_2[0][1].contains("\"function_column.sequence_type\" = \"int\""))
}