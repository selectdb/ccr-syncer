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

suite("test_tsa_mv_create_drop") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def tableName = "tbl_" + helper.randomSuffix()
    def aliasTableName = "alias_tbl_" + helper.randomSuffix()
    def test_num = 0
    def insert_num = 5

    helper.enableDbBinlog()
    helper.set_alias(aliasTableName)

    sql "DROP TABLE IF EXISTS ${tableName}"
    target_sql "DROP TABLE IF EXISTS ${aliasTableName}"

    sql """
        CREATE TABLE if NOT EXISTS ${tableName} 
        (
            `id` INT,
            `col1` INT,
            `col2` INT,
            `col3` INT,
            `col4` INT,
        )
        ENGINE=OLAP
        DISTRIBUTED BY HASH(id) BUCKETS 1 
        PROPERTIES ( 
            "replication_allocation" = "tag.location.default: 1"
        )
    """

    sql """
        CREATE MATERIALIZED VIEW mtr_${tableName}_full AS
        SELECT id, col1, col3 FROM ${tableName}
        """

    def materializedFinished = { res -> Boolean
        for (List<Object> row : res) {
            if ((row[5] as String).contains("mtr_${tableName}_full")) {
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
                                materializedFinished, 30))
    sql """ALTER TABLE ${tableName} set ("binlog.enable" = "true")"""


    logger.info("=== Test 1: full update rollup ===")

    helper.ccrJobDelete(tableName)
    helper.ccrJobCreate(tableName)

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 30))

    def checkViewExists = { res -> Boolean
        for (List<Object> row : res) {
            if ((row[1] as String).contains("mtr_${tableName}_full")) {
                return true
            }
        }
        return false
    }
    assertTrue(helper.checkShowTimesOf("""
                                SHOW CREATE MATERIALIZED VIEW mtr_${tableName}_full
                                ON ${aliasTableName}
                                """,
                                checkViewExists, 30, "target"))


    logger.info("=== Test 2: incremental update rollup ===")
    sql """
        CREATE MATERIALIZED VIEW ${tableName}_incr AS
        SELECT id, col2, col4 FROM ${tableName}
        """

    def materializedFinished1 = { res -> Boolean
        for (List<Object> row : res) {
            if ((row[5] as String).contains("${tableName}_incr")) {
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
                                materializedFinished1, 30, "sql"))

    def checkViewExists1 = { res -> Boolean
        for (List<Object> row : res) {
            if ((row[1] as String).contains("${tableName}_incr")) {
                return true
            }
        }
        return false
    }
    assertTrue(helper.checkShowTimesOf("""
                                SHOW CREATE MATERIALIZED VIEW ${tableName}_incr
                                ON ${aliasTableName}
                                """,
                                checkViewExists1, 30, "target"))

    logger.info("=== Test 3: drop materialized view ===")

    sql """
        DROP MATERIALIZED VIEW ${tableName}_incr ON ${tableName}
        """
    assertTrue(helper.checkShowTimesOf("""
                                SHOW CREATE MATERIALIZED VIEW ${tableName}_incr
                                ON ${aliasTableName}
                                """,
                                { res -> res.size() == 0 }, 30, "target"))
}
