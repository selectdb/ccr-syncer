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
suite("test_cds_tbl_backup_create_drop") {
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

    def exist = { res -> Boolean
        return res.size() != 0
    }
    def notExist = { res -> Boolean
        return res.size() == 0
    }

    sql """
        CREATE TABLE if NOT EXISTS ${tableName}_1
        (
            `test` INT,
            `id` INT
        )
        ENGINE=OLAP
        UNIQUE KEY(`test`, `id`)
        PARTITION BY RANGE(`id`)
        (
            PARTITION `${opPartitonName}_0` VALUES LESS THAN ("0"),
            PARTITION `${opPartitonName}_1` VALUES LESS THAN ("1000")
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
    """

    def prefix = helper.get_backup_label_prefix()
    GetDebugPoint().enableDebugPointForAllFEs("FE.PAUSE_PENDING_BACKUP_JOB", [value: prefix])

    helper.enableDbBinlog()
    helper.ccrJobDelete()
    helper.ccrJobCreate()

    // Wait for backup job to running
    def is_backup_running = { res ->
        for (int i = 0; i < res.size(); i++) {
            logger.info("backup job status: ${res[i]}")
            if (res[i][3] != "CANCELLED" && res[i][3] != "FINISHED") {
                return true
            }
        }
        return false
    }
    assertTrue(helper.checkShowTimesOf(
        """ SHOW BACKUP WHERE SnapshotName LIKE "${prefix}%" """,
        is_backup_running, 60))

    logger.info("create new table and drop it immediatelly")

    sql """
        CREATE TABLE if NOT EXISTS ${tableName}_2
        (
            `test` INT,
            `id` INT
        )
        ENGINE=OLAP
        UNIQUE KEY(`test`, `id`)
        PARTITION BY RANGE(`id`)
        (
            PARTITION `${opPartitonName}_0` VALUES LESS THAN ("0"),
            PARTITION `${opPartitonName}_1` VALUES LESS THAN ("1000")
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
    """
    sql "DROP TABLE ${tableName}_2 FORCE"
    sql "INSERT INTO ${tableName}_1 VALUES (1, 1)"
    sql "sync"

    GetDebugPoint().disableDebugPointForAllFEs("FE.PAUSE_PENDING_BACKUP_JOB")
    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}_1", 60))

    sql "INSERT INTO ${tableName}_1 VALUES (2, 2)"
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName}_1", 2, 60))

    logger.info("drop table and create it again, during full sync")
    // 1. pause fullsync backup job
    // 2. drop table A
    // 3. create table A again
    // 4. resume fullsync backup job
    // The drop table A should be skipped.

    GetDebugPoint().enableDebugPointForAllFEs("FE.PAUSE_PENDING_BACKUP_JOB", [value: prefix])
    helper.ccrJobDelete()

    target_sql "DROP DATABASE TEST_${context.DbName}"
    helper.ccrJobCreate()

    assertTrue(helper.checkShowTimesOf(
        """ SHOW BACKUP WHERE SnapshotName LIKE "${prefix}%" """,
        is_backup_running, 60))
    sql "DROP TABLE IF EXISTS ${tableName}_1 FORCE"
    sql """
        CREATE TABLE if NOT EXISTS ${tableName}_1
        (
            `test` INT,
            `id` INT
        )
        ENGINE=OLAP
        UNIQUE KEY(`test`, `id`)
        PARTITION BY RANGE(`id`)
        (
            PARTITION `${opPartitonName}_0` VALUES LESS THAN ("0"),
            PARTITION `${opPartitonName}_1` VALUES LESS THAN ("1000")
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
    """
    sql "INSERT INTO ${tableName}_1 VALUES (1, 1)"
    sql "sync"

    GetDebugPoint().disableDebugPointForAllFEs("FE.PAUSE_PENDING_BACKUP_JOB")
    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}_1", 60))

    sql "INSERT INTO ${tableName}_1 VALUES (2, 2)"
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName}_1", 2, 60))
}

