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

suite("test_ds_tbl_rename_dep") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))
    if (!helper.is_version_supported([30003, 20108, 20016])) {
        // at least doris 3.0.3, 2.1.8 and doris 2.0.16
        def version = helper.upstream_version()
        logger.info("skip this suite because version is not supported, upstream version ${version}")
        return
    }

    def tableName = "tbl_" + helper.randomSuffix()
    def test_num = 0
    def insert_num = 10
    def opPartitonName = "less"
    def new_rollup_name = "rn_new"

    def exist = { res -> Boolean
        return res.size() != 0
    }
    def notExist = { res -> Boolean
        return res.size() == 0
    }

    sql """
        CREATE TABLE IF NOT EXISTS ${tableName}
        (
            `id` int,
            `no` int,
            `name` varchar(10)
        ) ENGINE = olap
        UNIQUE KEY(`id`, `no`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "enable_unique_key_merge_on_write" = "false"
        );
    """
    sql """ INSERT INTO ${tableName} VALUES (2, 1, 'b') """

    helper.enableDbBinlog()
    helper.ccrJobDelete()
    helper.ccrJobCreate()

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 60))
    assertTrue(helper.checkShowTimesOf("SELECT * FROM ${tableName} ", exist, 60, "target"))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName}", 1, 30))

    logger.info("=== Test 1: Rename table case ===")

    def first_job_progress = helper.get_job_progress()

    helper.ccrJobPause()
    sql "INSERT INTO ${tableName} VALUES (3, 1, 'c')"
    def newTableName = "NEW_${tableName}"
    sql "ALTER TABLE ${tableName} RENAME ${newTableName}"
    sql "INSERT INTO ${newTableName} VALUES (4, 1, 'd')"
    sql "sync"
    helper.ccrJobResume()

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${newTableName}\"", exist, 60, "target"))
    assertTrue(helper.checkShowTimesOf("SELECT * FROM ${newTableName} ", exist, 60, "target"))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${newTableName} WHERE id = 3", 1, 30))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${newTableName} WHERE id = 4", 1, 30))

    def last_job_progress = helper.get_job_progress()
    if (first_job_progress.full_sync_start_at != last_job_progress.full_sync_start_at) {
        logger.error("full sync should not be triggered")
        assertTrue(false)
    }
}

