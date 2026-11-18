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
suite("test_ds_tbl_create_drop") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

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

    helper.enableDbBinlog()

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

    helper.ccrJobDelete()
    helper.ccrJobCreate()

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}_1", 60))

    logger.info("=== Test 1: Check table and backup size ===")
    sql "sync"
    assertTrue(helper.checkShowTimesOf(""" SHOW TABLES LIKE "${tableName}_1" """, exist, 60, "target"))

    // save the backup num of source cluster
    def show_backup_result = sql "SHOW BACKUP"
    def backup_num = show_backup_result.size()
    logger.info("backups before drop partition: ${show_backup_result}")

    logger.info("=== Test 2: Pause and create new table ===")

    helper.ccrJobPause()

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

    sql "sync"

    logger.info("=== Test 3: Resume and check new table ===")

    helper.ccrJobResume()

    sql "sync"
    assertTrue(helper.checkShowTimesOf("""
                                SHOW TABLES LIKE "${tableName}_2"
                                """,
                                exist, 60, "target"))

    logger.info("=== Test 4: Pause and drop old table ===")

    helper.ccrJobPause()

    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${tableName}_1 VALUES (${test_num}, ${index})
            """
    }

    sql """
    DROP TABLE ${tableName}_1 FORCE
    """

    assertTrue(helper.checkShowTimesOf("""
                                SHOW TABLES LIKE "${tableName}_1"
                                """,
                                notExist, 60, "sql"))

    logger.info("=== Test 5: Resume and verify no new backups are triggered ===")
    helper.ccrJobResume()

    assertTrue(helper.checkShowTimesOf("""
                                SHOW TABLES LIKE "${tableName}_1"
                                """,
                                notExist, 60, "target"))

    show_backup_result = sql "SHOW BACKUP"
    logger.info("backups after drop old table: ${show_backup_result}")
    assertTrue(show_backup_result.size() == backup_num)
}
