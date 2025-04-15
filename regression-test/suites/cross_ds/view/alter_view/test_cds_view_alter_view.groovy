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

suite("test_cds_view_alter_view") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def suffix = helper.randomSuffix()
    def tableName = 'tbl_' + suffix

    def has_count = { count ->
        return { res -> Boolean
            res.size() == count
        }
    }

    def exist = { res -> Boolean
        return res.size() != 0
    }

    sql " DROP TABLE IF EXISTS ${tableName} "
    sql " DROP VIEW IF EXISTS v "
    target_sql " DROP TABLE IF EXISTS ${tableName} "
    target_sql " DROP VIEW IF EXISTS v "

    helper.enableDbBinlog()
    sql """
        CREATE TABLE if NOT EXISTS ${tableName}
        (
            `test` INT,
            `id` INT,
            `k1` INT,
            `k2` INT
        )
        ENGINE=OLAP
        DUPLICATE KEY(`test`, `id`)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true",
            "binlog.ttl_seconds" = "180",
            "light_schema_change" = "true"
        )
    """

    sql """
        CREATE VIEW v AS SELECT K1 FROM ${tableName}
    """
    helper.ccrJobDelete()
    helper.ccrJobCreate()

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 60))
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}\"", has_count(1), 50, "target"))

    helper.ccrJobPause()

    sql """
        ALTER TABLE ${tableName} ADD ROLLUP RollupIndex(`k1`)
        """
    def rollupFullFinished = { res -> Boolean
        for (List<Object> row : res) {
            if ((row[5] as String).contains("RollupIndex")) {
                return true
            }
        }
        return false
    }
    assertTrue(helper.checkShowTimesOf("""
                                SHOW ALTER TABLE ROLLUP
                                FROM ${context.dbName}
                                WHERE TableName = "${tableName}" AND State = "FINISHED"
                                """,
                                rollupFullFinished, 60))

    sql """
        ALTER VIEW v (k1) AS SELECT k2 FROM ${tableName}
    """

    sql """
        ALTER TABLE ${tableName} RENAME COLUMN k2 k3
    """

    assertTrue(helper.checkShowTimesOf(""" SHOW COLUMNS FROM ${tableName} where Field = \"k3\" """, exist, 60))

    helper.ccrJobResume()

    assertTrue(helper.checkShowTimesOf(""" SHOW COLUMNS FROM ${tableName} where Field = \"k3\" """, exist, 60, "target"))
    assertTrue(helper.checkShowTimesOf(""" SHOW VIEWS LIKE "v" """, exist, 60, "target"))
    assertTrue(helper.checkShowTimesOf(""" SHOW CREATE VIEW v """, { r -> r[0][1].contains("k2") } , 60, "target"))
}
