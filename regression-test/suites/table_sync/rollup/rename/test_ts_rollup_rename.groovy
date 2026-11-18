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

suite("test_ts_rollup_rename") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    if (helper.has_feature("feature_skip_rollup_binlogs")) {
        logger.info("skip this suite because feature_skip_rollup_binlogs is enabled")
        return
    }

    def tableName = "tbl_" + helper.randomSuffix()
    def test_num = 0
    def insert_num = 5

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
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
    """
    sql """
        ALTER TABLE ${tableName}
        ADD ROLLUP rollup_${tableName}_full (id, col2, col4)
        """

    def rollupFullFinished = { res -> Boolean
        for (List<Object> row : res) {
            if ((row[5] as String).contains("rollup_${tableName}_full")) {
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
                                rollupFullFinished, 30))
    sql """ALTER TABLE ${tableName} set ("binlog.enable" = "true")"""

    logger.info("=== Test 1: full update rollup ===")
    helper.ccrJobCreate(tableName)

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 30))

    def hasRollupFull = { res -> Boolean
        for (List<Object> row : res) {
            if ((row[0] as String) == "rollup_${tableName}_full") {
                return true
            }
        }

        return false
    }
    assertTrue(helper.checkShowTimesOf("DESC TEST_${context.dbName}.${tableName} ALL",
                                hasRollupFull, 30, "target"))

    logger.info("=== Test 2: Rename rollup ===")
    //  binlog type: RENAME_ROLLUP
    // binlog data: {"db":10208,"tb":10211,"ind":10216,"p":-1,"nT":"","oT":"","nR":"rollup_tbl_525975341_full_new","oR":"rollup_tbl_525975341_full","nP":"","oP":""} 
    sql """
        ALTER TABLE ${tableName}
        RENAME ROLLUP rollup_${tableName}_full rollup_${tableName}_full_new
    """
    def hasRollupFullNew = { res -> Boolean
        for (List<Object> row : res) {
            if ((row[0] as String) == "rollup_${tableName}_full_new") {
                return true
            }
        }

        return false
    }
    assertTrue(helper.checkShowTimesOf("DESC TEST_${context.dbName}.${tableName} ALL",
                                hasRollupFullNew, 30, "target"))
}
