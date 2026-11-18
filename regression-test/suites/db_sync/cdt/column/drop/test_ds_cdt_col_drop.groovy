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

suite("test_ds_cdt_col_drop") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def dbName = context.dbName
    def tableName = "tbl_" + helper.randomSuffix()
    def test_num = 0
    def insert_num = 5

    def exist = { res -> Boolean
        return res.size() != 0
    }

    def notExist = { res -> Boolean
        return res.size() == 0
    }

    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
    target_sql "DROP TABLE IF EXISTS TEST_${dbName}.${tableName}"

    helper.enableDbBinlog()

    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            id INT,
            j JSON,
            v VARIANT,
            s STRUCT<f1:INT>,
            a ARRAY<CHAR(5)>,
            m MAP<TEXT,INT>,
            b bitmap
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 10
        PROPERTIES(
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
        """

    helper.ccrJobDelete()
    helper.ccrJobCreate()

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 30))

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}\"", exist, 60, "sql"))

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}\"", exist, 60, "target"))

    sql """ ALTER TABLE ${tableName} DROP COLUMN j """
    sql """ ALTER TABLE ${tableName} DROP COLUMN v """
    sql """ ALTER TABLE ${tableName} DROP COLUMN s """
    sql """ ALTER TABLE ${tableName} DROP COLUMN a """
    sql """ ALTER TABLE ${tableName} DROP COLUMN m """
    sql """ ALTER TABLE ${tableName} DROP COLUMN b """

    assertTrue(helper.checkShowTimesOf(" SHOW COLUMNS FROM ${tableName} where Field = \"j\" ", notExist, 60, "target"))
    assertTrue(helper.checkShowTimesOf(" SHOW COLUMNS FROM ${tableName} where Field = \"v\" ", notExist, 60, "target"))
    assertTrue(helper.checkShowTimesOf(" SHOW COLUMNS FROM ${tableName} where Field = \"s\" ", notExist, 60, "target"))
    assertTrue(helper.checkShowTimesOf(" SHOW COLUMNS FROM ${tableName} where Field = \"a\" ", notExist, 60, "target"))
    assertTrue(helper.checkShowTimesOf(" SHOW COLUMNS FROM ${tableName} where Field = \"m\" ", notExist, 60, "target"))
    assertTrue(helper.checkShowTimesOf(" SHOW COLUMNS FROM ${tableName} where Field = \"b\" ", notExist, 60, "target"))

    sql """
        CREATE TABLE IF NOT EXISTS ${tableName}_0 (
            id INT,
            h HLL HLL_UNION
        )
        AGGREGATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 10
        PROPERTIES(
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
        """
    sql """ ALTER TABLE ${tableName}_0 DROP COLUMN h """
    assertTrue(helper.checkShowTimesOf(" SHOW COLUMNS FROM ${tableName}_0 where Field = \"h\" ", notExist, 60, "target"))
}

