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

suite('test_dsd_rollup_drop_2') {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", 'helper.groovy'))

    if (helper.has_feature('feature_skip_rollup_binlogs')) {
        logger.info('skip this suite because feature_skip_rollup_binlogs is enabled')
        return
    }
    def exist = { res -> Boolean
        return res.size() != 0
    }
    def tableName = 'tbl_' + helper.randomSuffix()
    def test_num = 0
    def insert_num = 5

    sql "DROP TABLE IF EXISTS ${tableName}"
    target_sql "DROP TABLE IF EXISTS ${tableName}"

    helper.enableDbBinlog()

    sql """
        CREATE TABLE if NOT EXISTS ${tableName}
        (
            `id` INT,
            `col1` INT,
            `col2` INT
        )
        ENGINE=OLAP
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
    """

    helper.ccrJobDelete()
    helper.ccrJobCreate()

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}\"", exist, 60, "sql"))
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}\"", exist, 60, "target"))
    helper.ccrJobPause()

    sql "INSERT INTO ${tableName} VALUES (1, 1, 1)"

    sql """
        CREATE TABLE if NOT EXISTS ${tableName}_0
        (
            `id` INT,
            `col1` INT,
            `col2` INT
        )
        ENGINE=OLAP
        DISTRIBUTED BY HASH(id) BUCKETS 1
        ROLLUP(
            r1(`col1`)
        )
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
    """

    sql """
        ALTER TABLE ${tableName}_0
        DROP ROLLUP r1
        """

    helper.ccrJobResume()

    def hasRollupDropped = { res -> Boolean
        for (List<Object> row : res) {
            if ((row[0] as String) == "r1") {
                return false
            }
        }
        return true
    }

    assertTrue(helper.checkShowTimesOf("DESC ${context.dbName}.${tableName} ALL",
                                hasRollupDropped, 30, 'target'))

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}_0\"", exist, 60, "sql"))
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}_0\"", exist, 60, "target"))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName}", 1, 30))
}
