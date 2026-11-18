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

suite("test_ds_cdt_col_rename") {
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

    sql """ ALTER TABLE ${tableName} RENAME COLUMN j new_j """
    sql """ ALTER TABLE ${tableName} RENAME COLUMN v new_v """
    sql """ ALTER TABLE ${tableName} RENAME COLUMN s new_s """
    sql """ ALTER TABLE ${tableName} RENAME COLUMN a new_a """
    sql """ ALTER TABLE ${tableName} RENAME COLUMN m new_m """
    sql """ ALTER TABLE ${tableName} RENAME COLUMN b new_b """

    assertTrue(helper.checkShowTimesOf(" SHOW COLUMNS FROM ${tableName} where Field = \"new_j\" ", exist, 60, "target"))
    assertTrue(helper.checkShowTimesOf(" SHOW COLUMNS FROM ${tableName} where Field = \"new_v\" ", exist, 60, "target"))
    assertTrue(helper.checkShowTimesOf(" SHOW COLUMNS FROM ${tableName} where Field = \"new_s\" ", exist, 60, "target"))
    assertTrue(helper.checkShowTimesOf(" SHOW COLUMNS FROM ${tableName} where Field = \"new_a\" ", exist, 60, "target"))
    assertTrue(helper.checkShowTimesOf(" SHOW COLUMNS FROM ${tableName} where Field = \"new_m\" ", exist, 60, "target"))
    assertTrue(helper.checkShowTimesOf(" SHOW COLUMNS FROM ${tableName} where Field = \"new_b\" ", exist, 60, "target"))

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

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}\"", exist, 60, "sql"))

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}\"", exist, 60, "target"))

    sql """ ALTER TABLE ${tableName}_0 RENAME COLUMN h new_h """
    assertTrue(helper.checkShowTimesOf(" SHOW COLUMNS FROM ${tableName}_0 where Field = \"new_h\" ", exist, 60, "target"))
}
