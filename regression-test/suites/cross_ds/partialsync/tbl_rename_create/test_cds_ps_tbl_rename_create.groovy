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

suite('test_cds_ps_tbl_rename_create') {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", 'helper.groovy'))

    if (!helper.is_version_supported([30004, 20108, 20099])) {
        // at least doris 3.0.4, 2.1.8
        def version = helper.upstream_version()
        logger.info("skip this suite because version is not supported, upstream version ${version}")
        return
    }

    def tableName = 'tbl_' + helper.randomSuffix()
    def newTableName = 'tbl_' + helper.randomSuffix()

    def exist = { res -> Boolean
        return res.size() != 0
    }
    def notExist = { res -> Boolean
        return res.size() == 0
    }

    sql """
        CREATE TABLE if NOT EXISTS ${tableName}
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

    assertTrue(helper.checkRestoreFinishTimesOf(tableName, 60))

    def first_job_progress = helper.get_job_progress()

    helper.ccrJobPause()

    logger.info(' === Add a key column to trigger a patrial sync === ')
    sql "ALTER TABLE ${tableName} ADD COLUMN `new_col` INT KEY DEFAULT \"0\""

    assertTrue(helper.checkShowTimesOf("""
                                SHOW ALTER TABLE COLUMN
                                FROM ${context.dbName}
                                WHERE TableName = "${tableName}" AND State = "FINISHED"
                                """,
                                exist, 30))

    sql "INSERT INTO ${tableName} VALUES (1, 100, 100), (100, 1, 1), (2, 200, 200), (200, 2, 2)"

    logger.info(" === Rename ${tableName} to ${newTableName} === ")
    sql """
        ALTER TABLE ${tableName} RENAME ${newTableName}
    """
    sql "INSERT INTO ${newTableName} VALUES (5, 500, 500)"

    logger.info(' === Create a new table with the same name === ')
    sql """
        CREATE TABLE if NOT EXISTS ${tableName}
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
    sql "INSERT INTO ${tableName} VALUES (1, 100)"

    helper.ccrJobResume()

    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${newTableName}", 5, 60))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName}", 1, 60))

    // FIXME
    // [2024-12-24 06:20:51.217] DEBUG table commit seq map: map[16915:2153], table name mapping: map[16915:tbl_1240863887] job=test_cds_ps_tbl_rename_create line=ccr/job.go:524
    // [2024-12-24 06:20:51.217]  WARN partial sync table tbl_1240863887 id not match, force full sync. table id 16472, backup object id 16915 job=test_cds_ps_tbl_rename_create line=ccr/job.go:528
    // [2024-12-24 06:20:51.218]  INFO new snapshot, commitSeq: 2098 job=test_cds_ps_tbl_rename_create line=ccr/job.go:3116
    //
    // // no fullsync are triggered
    // def last_job_progress = helper.get_job_progress()
    // assertTrue(last_job_progress.full_sync_start_at == first_job_progress.full_sync_start_at)
}


