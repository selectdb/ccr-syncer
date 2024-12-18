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

suite('test_cds_tbl_idx_inverted_drop') {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", 'helper.groovy'))

    if (!helper.is_version_supported([30003, 20108, 20016])) {
        // at least doris 3.0.3, 2.1.8 and doris 2.0.16
        def version = helper.upstream_version()
        logger.info("skip this suite because version is not supported, upstream version ${version}")
        return
    }

    def tableName = 'tbl_' + helper.randomSuffix()

    def exist = { res -> Boolean
        return res.size() != 0
    }
    def notExist = { res -> Boolean
        return res.size() == 0
    }
    def has_count = { count -> { res -> Boolean
            return res.size() == count
    } }

    logger.info('=== Create a fake table ===')
    sql """
        CREATE TABLE if NOT EXISTS ${tableName}_fake
        (
            `test` INT,
            `id` INT
        )
        ENGINE=OLAP
        UNIQUE KEY(`test`, `id`)
        PARTITION BY RANGE(`id`)
        (
            PARTITION `p1` VALUES LESS THAN ("0"),
            PARTITION `p2` VALUES LESS THAN ("100"),
            PARTITION `p3` VALUES LESS THAN ("200"),
            PARTITION `p4` VALUES LESS THAN ("300"),
            PARTITION `p5` VALUES LESS THAN ("1000")
        )
        DISTRIBUTED BY HASH(id) BUCKETS AUTO
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
    """

    helper.enableDbBinlog()
    helper.ccrJobDelete()
    helper.ccrJobCreate()

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}_fake", 60))

    logger.info(' ==== create table and drop ==== ')

    def first_job_progress = helper.get_job_progress()

    helper.ccrJobPause()

    sql """
        CREATE TABLE if NOT EXISTS ${tableName}
        (
            `test` INT,
            `id` INT,
            `value` STRING NULL
        )
        ENGINE=OLAP
        UNIQUE KEY(`test`, `id`)
        PARTITION BY RANGE(`id`)
        (
            PARTITION `p1` VALUES LESS THAN ("0"),
            PARTITION `p2` VALUES LESS THAN ("100"),
            PARTITION `p3` VALUES LESS THAN ("200"),
            PARTITION `p4` VALUES LESS THAN ("300"),
            PARTITION `p5` VALUES LESS THAN ("1000")
        )
        DISTRIBUTED BY HASH(id) BUCKETS AUTO
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
    """

    sql "INSERT INTO ${tableName} VALUES (1, 100, ''), (100, 1, ''), (2, 200, ''), (200, 2, '')"

    logger.info('=== add inverted index ===')
    sql """
        CREATE INDEX idx_inverted ON ${tableName} (value) USING INVERTED
        """
    sql 'sync'

    assertTrue(helper.checkShowTimesOf("""
                                SHOW ALTER TABLE COLUMN
                                FROM ${context.dbName}
                                WHERE TableName = "${tableName}" AND State = "FINISHED"
                                """,
                                has_count(1), 30))

    sql """ INSERT INTO ${tableName} VALUES (1, 1, "1") """

    def show_indexes_result = sql "show indexes from ${tableName}"
    logger.info("show indexes: ${show_indexes_result}")

    // The drop index will be ignored since table is dropped
    sql """
        DROP INDEX idx_inverted ON ${tableName}
        """
    sql 'sync'

    assertTrue(helper.checkShowTimesOf("""
                                SHOW ALTER TABLE COLUMN
                                FROM ${context.dbName}
                                WHERE TableName = "${tableName}" AND State = "FINISHED"
                                """,
                                has_count(2), 30))

    show_indexes_result = sql "show indexes from ${tableName}"
    logger.info("show indexes: ${show_indexes_result}")

    sql "INSERT INTO ${tableName} VALUES (5, 500, 'test')"
    sql "DROP TABLE ${tableName} FORCE"
    sql "INSERT INTO ${tableName}_fake VALUES (5, 500)"

    helper.ccrJobResume()

    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName}_fake", 1, 60))
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}\"", notExist, 60, 'target'))

    // no fullsync are triggered
    def last_job_progress = helper.get_job_progress()
    assertTrue(last_job_progress.full_sync_start_at == first_job_progress.full_sync_start_at)
}
