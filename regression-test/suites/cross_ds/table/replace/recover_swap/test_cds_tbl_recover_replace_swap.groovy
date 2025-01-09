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

suite('test_cds_tbl_recover_replace_swap') {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", 'helper.groovy'))

    if (!helper.is_version_supported([30099, 20199, 20099])) {
        def version = helper.upstream_version()
        logger.info("skip this suite because version is not supported, upstream version ${version}")
        return
    }

    def tableNameA = 'tbl_a_' + helper.randomSuffix()
    def tableNameB = 'tbl_b_' + helper.randomSuffix()

    def exist = { res -> Boolean
        return res.size() != 0
    }
    def notExist = { res -> Boolean
        return res.size() == 0
    }

    sql """
        CREATE TABLE if NOT EXISTS ${tableNameA}
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
    sql """
        CREATE TABLE if NOT EXISTS ${tableNameB}
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

    assertTrue(helper.checkRestoreFinishTimesOf(tableNameA, 60))
    sql "INSERT INTO ${tableNameA} VALUES (1, 100)"

    helper.ccrJobPause()
    logger.info(' === Replace table A with table B, then recover table A as table B')
    sql """
        ALTER TABLE ${tableNameA} REPLACE WITH TABLE ${tableNameB} PROPERTIES ("swap" = "false")
        """
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE '${tableNameB}'", notExist, 30))

    sql """
        RECOVER TABLE ${tableNameA} AS ${tableNameB}
        """

    // tableNameB was renamed to tableNameA
    sql """
        ALTER TABLE ${tableNameA} REPLACE WITH TABLE ${tableNameB} PROPERTIES ("swap" = "true")
        """
    sql " DROP TABLE ${tableNameB}"

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE '${tableNameB}'", notExist, 30))

    sql "INSERT INTO ${tableNameA} VALUES (5, 500)"
    helper.ccrJobResume()

    // [2024-12-24 11:56:33.218]  WARN force new partial snapshot, since table 37663 has renamed from tbl_b_104306046 to tbl_a_735009328 job=test_cds_tbl_recover_replace line=ccr/job.go:399
    // [2024-12-24 11:56:33.218]  INFO new partial snapshot, commitSeq: 4568, table id: 37663, table: tbl_a_735009328, alias: __ccr_tbl_a_735009328_1735041393 job=test_cds_tbl_recover_replace line=ccr/job.go:3200

    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableNameA}", 2, 60))

    sql """
        RECOVER TABLE ${tableNameB}
        """
    sql "INSERT INTO ${tableNameB} VALUES (5, 500)"

    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableNameB}", 1, 60))

    // FIXME
    // [2024-12-24 11:19:12.067] DEBUG table commit seq map: map[27308:3734], table name mapping: map[27308:tbl_b_301944667] job=test_cds_ps_tbl_recover line=ccr/job.go:524
    // [2024-12-24 11:19:12.067]  WARN partial sync table tbl_b_301944667 id not match, force full sync. table id 27415, backup object id 27308 job=test_cds_ps_tbl_recover line=ccr/job.go:528
    // [2024-12-24 11:19:12.067]  INFO new snapshot, commitSeq: 3712 job=test_cds_ps_tbl_recover line=ccr/job.go:3150

// // no fullsync are triggered
// def last_job_progress = helper.get_job_progress()
// assertTrue(last_job_progress.full_sync_start_at == first_job_progress.full_sync_start_at)
}


