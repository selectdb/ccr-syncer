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

suite("test_ts_tbl_aggregate_key") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def dbName = context.dbName
    def tableName = "tbl_" + helper.randomSuffix()
    def test_num = 0
    def insert_num = 5

    def exist = { res -> Boolean
        return res.size() != 0
    }

    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
    target_sql "DROP TABLE IF EXISTS TEST_${dbName}.${tableName}"

    helper.enableDbBinlog()

    sql """
        CREATE TABLE ${tableName}(
            `id` int,
            `age` int REPLACE_IF_NOT_NULL NULL
        )
        AGGREGATE KEY(`id`)
        DISTRIBUTED BY HASH(id) BUCKETS 3 
        PROPERTIES ( 
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        );
    """

    helper.ccrJobDelete(tableName)
    helper.ccrJobCreate(tableName)

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 30))

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}\"", exist, 60, "sql"))

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}\"", exist, 60, "target"))

    sql "INSERT INTO ${tableName} VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)"

    assertTrue(helper.checkSelectTimesOf(""" select * from ${tableName} """, 5, 30))

    assertTrue(helper.checkShowTimesOf(" select age from ${tableName} where id = 3 ", { r -> r[0][0] == 3}, 60, "target"))

    sql "INSERT INTO ${tableName} VALUES (3, 5) "

    assertTrue(helper.checkShowTimesOf(" select age from ${tableName} where id = 3 ", { r -> r[0][0] == 5}, 60, "target"))

    sql "INSERT INTO ${tableName} VALUES (2, 4), (5, 7) "

    assertTrue(helper.checkShowTimesOf(" select age from ${tableName} where id = 2 ", { r -> r[0][0] == 4}, 60, "target"))

    assertTrue(helper.checkShowTimesOf(" select age from ${tableName} where id = 5 ", { r -> r[0][0] == 7}, 60, "target"))
}
