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

suite("test_ts_prop_seq_col") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def dbName = context.dbName
    def tableName1 = "tbl_" + helper.randomSuffix()
    def tableName2 = "tbl_" + helper.randomSuffix()

    def exist = { res -> Boolean
        return res.size() != 0
    }

    sql "DROP TABLE IF EXISTS ${dbName}.${tableName1}"
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName2}"
    target_sql "DROP TABLE IF EXISTS TEST_${dbName}.${tableName1}"
    target_sql "DROP TABLE IF EXISTS TEST_${dbName}.${tableName2}"

    helper.enableDbBinlog()

    sql """
            CREATE TABLE if NOT EXISTS ${tableName1}
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
            CREATE TABLE if NOT EXISTS ${tableName2}
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

    helper.ccrJobDelete(tableName1)
    helper.ccrJobDelete(tableName2)
    helper.ccrJobCreate(tableName1)
    helper.ccrJobCreate(tableName2)

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName1}", 30))
    assertTrue(helper.checkRestoreFinishTimesOf("${tableName2}", 30))

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName1}\"", exist, 60, "sql"))
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName2}\"", exist, 60, "sql"))

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName1}\"", exist, 60, "target"))
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName2}\"", exist, 60, "target"))

    def target_res_1 = target_sql "SHOW CREATE TABLE ${tableName1}"
    def target_res_2 = target_sql "SHOW CREATE TABLE ${tableName2}"

    assertTrue(target_res_1[0][1].contains("\"function_column.sequence_col\" = \"test\""))
    assertTrue(target_res_2[0][1].contains("\"function_column.sequence_type\" = \"int\""))
}