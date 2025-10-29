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

suite("test_prop_retention_count") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def dbName = context.dbName
    def tableName = "tbl_" + helper.randomSuffix()

    def exist = { res -> Boolean
        return res.size() != 0
    }

    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
    target_sql "DROP TABLE IF EXISTS TEST_${dbName}.${tableName}"

    helper.enableDbBinlog()

    sql """
        CREATE TABLE ${tableName} (
            k0 DATETIME NOT NULL
        )
        partition by range (date_trunc(k0, 'day')) ()
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "partition.retention_count" = "3"
        );
    """

    helper.ccrJobDelete()
    helper.ccrJobCreate()

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 30))

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}\"", exist, 60, "sql"))

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}\"", exist, 60, "target"))

    def target_res = target_sql "SHOW CREATE TABLE ${tableName}"

    assertTrue(target_res[0][1].contains("\"partition.retention_count\" = \"3\""))

    target_sql """
        insert into ${tableName} select date_add('2022-01-01 00:00:00', interval number day) from numbers("number" = "100");
    """
    target_sql """ admin set frontend config ('dynamic_partition_check_interval_seconds' = '1') """
    sleep(8000)
    target_res = target_sql "show partitions from ${tableName}"
    assertEquals(target_res.size(), 3)

    target_sql """ admin set frontend config ('dynamic_partition_check_interval_seconds' = '600') """
}