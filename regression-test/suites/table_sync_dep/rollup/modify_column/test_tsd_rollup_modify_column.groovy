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

suite('test_tsd_rollup_modify_column') {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", 'helper.groovy'))

    // errCode = 2, detailMessage = Cannot modify column in rollup rollup_tbl_1369738189_full
    logger.info("modify column in rollup is forbidden, skip this suite");
    return

    if (helper.has_feature('feature_skip_rollup_binlogs')) {
        logger.info('skip this suite because feature_skip_rollup_binlogs is enabled')
        return
    }

    def tableName = 'tbl_' + helper.randomSuffix()

    def hasRollupAdded = { res -> Boolean
        for (List<Object> row : res) {
            if ((row[0] as String) == "rollup_${tableName}_full") {
                return true
            }
        }
        return false
    }

    def has_count = { count ->
        return { res -> Boolean
            res.size() == count
        }
    }

    def exist = { res -> Boolean
        return res.size() != 0
    }

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

    helper.enableDbBinlog()
    helper.ccrJobDelete(tableName)
    helper.ccrJobCreate(tableName)

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 30))
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}\"", exist, 30, 'target'))

    helper.ccrJobPause(tableName)
    sql """ INSERT INTO ${tableName} VALUES (1, 1, 1, 1, 1) """
    sql """ INSERT INTO ${tableName} VALUES (2, 2, 2, 2, 2) """
    sql """
        ALTER TABLE ${tableName} MODIFY COLUMN col3 BIGINT AFTER col4 FROM rollup_${tableName}_full
        """

    assertTrue(helper.checkShowTimesOf("""
                                SHOW ALTER TABLE COLUMN
                                FROM ${context.dbName}
                                WHERE TableName = "${tableName}"
                                    AND IndexName = "rollup_${tableName}_full"
                                    AND State = "FINISHED"
                                """,
                                has_count(1), 30))

    sql """ INSERT INTO ${tableName} VALUES (3, 3, 3, 3, 3, 3) """
    sql """ INSERT INTO ${tableName} VALUES (4, 4, 4, 4, 4, 4) """

    helper.ccrJobResume(tableName)

    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName}", 4, 30))
    assertTrue(helper.checkShowTimesOf("""
                                SHOW ALTER TABLE COLUMN
                                FROM TEST_${context.dbName}
                                WHERE TableName = "${tableName}"
                                    AND IndexName = "rollup_${tableName}_full"
                                    AND State = "FINISHED"
                                """,
                                has_count(1), 30, "target"))
}

