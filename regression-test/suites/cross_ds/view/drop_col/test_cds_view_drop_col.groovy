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

suite("test_cds_view_drop_col") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def suffix = helper.randomSuffix()
    def tableName = 'tbl_' + suffix

    def has_count = { count ->
        return { res -> Boolean
            res.size() == count
        }
    }

    helper.enableDbBinlog()
    sql """
        CREATE TABLE if NOT EXISTS ${tableName}
        (
            `test` INT,
            `id` INT
        )
        ENGINE=OLAP
        DUPLICATE KEY(`test`, `id`)
        PARTITION BY RANGE(id)
        (
            PARTITION `p1` VALUES LESS THAN ("100"),
            PARTITION `p2` VALUES LESS THAN ("200")
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true",
            "binlog.ttl_seconds" = "180"
        )
    """
    helper.ccrJobDelete()
    helper.ccrJobCreate()

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 60))
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}\"", has_count(1), 50, "target"))

    helper.ccrJobPause()

    // Add key column, trigger a partial snapshot
    sql """
        ALTER TABLE ${tableName}
        ADD COLUMN `first` INT KEY DEFAULT "0"
        """
    sql "sync"

    assertTrue(helper.checkShowTimesOf("""
                                SHOW ALTER TABLE COLUMN
                                FROM ${context.dbName}
                                WHERE TableName = "${tableName}" AND State = "FINISHED"
                                """,
                                has_count(1), 30))

    for (int i = 0; i < 5; i++) {
        sql """
            INSERT INTO ${tableName} VALUES (${i}, ${i}, ${i})
            """
    }

    // Create a view depends on the new column
    sql """
        CREATE VIEW view_${suffix} AS
        SELECT first, id FROM ${tableName}
        WHERE id > 100
        """

    // Drop the key column
    sql """
        ALTER TABLE ${tableName}
        DROP COLUMN `first`
        """
    sql "sync"

    assertTrue(helper.checkShowTimesOf("""
                                SHOW ALTER TABLE COLUMN
                                FROM ${context.dbName}
                                WHERE TableName = "${tableName}" AND State = "FINISHED"
                                """,
                                has_count(2), 30))

    helper.ccrJobResume()

    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName}", 5, 50))

    for (int i = 0; i < 5; i++) {
        sql """ INSERT INTO ${tableName} VALUES (${i}, ${i}) """
    }

    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName}", 10, 50))
}
