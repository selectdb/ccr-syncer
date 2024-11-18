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

suite("test_cds_tbl_rename_create") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    if (!helper.is_version_supported([30003, 20108, 20016])) {
        // at least doris 3.0.3, 2.1.8 and doris 2.0.16
        def version = helper.upstream_version()
        logger.info("skip this suite because version is not supported, upstream version ${version}")
        return
    }

    def oldTableName = "tbl_old_" + helper.randomSuffix()
    def newTableName = "tbl_new_" + helper.randomSuffix()

    def exist = { res -> Boolean
        return res.size() != 0
    }
    def notExist = { res -> Boolean
        return res.size() == 0
    }

    logger.info("=== Create a fake table ===")
    sql """
        CREATE TABLE if NOT EXISTS ${oldTableName}_fake
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

    assertTrue(helper.checkRestoreFinishTimesOf("${oldTableName}_fake", 60))

    logger.info(" ==== create table and rename ==== ")

    def first_job_progress = helper.get_job_progress()

    helper.ccrJobPause()

    sql """
        CREATE TABLE if NOT EXISTS ${oldTableName}
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

    sql "INSERT INTO ${oldTableName} VALUES (1, 100), (100, 1), (2, 200), (200, 2)"
    sql "INSERT INTO ${oldTableName} VALUES (5, 500)"
    sql "ALTER TABLE ${oldTableName} RENAME ${newTableName}"
    sql "INSERT INTO ${newTableName} VALUES (6, 600)"

    helper.ccrJobResume()

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${newTableName}\"", exist, 60, "target"))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${newTableName}", 6, 60))

    // no fullsync are triggered
    def last_job_progress = helper.get_job_progress()
    assertTrue(last_job_progress.full_sync_start_at == first_job_progress.full_sync_start_at)
}



