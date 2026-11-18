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

suite("test_ds_part_drop") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def tableName = "tbl_" + helper.randomSuffix()
    def test_num = 0
    def insert_num = 90  // insert into last partition
    def opPartitonName = "less"

    def exist = { res -> Boolean
        return res.size() != 0
    }
    def notExist = { res -> Boolean
        return res.size() == 0
    }

    helper.enableDbBinlog()

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
            PARTITION `${opPartitonName}_0` VALUES LESS THAN ("0"),
            PARTITION `${opPartitonName}_1` VALUES LESS THAN ("10"),
            PARTITION `${opPartitonName}_2` VALUES LESS THAN ("20"),
            PARTITION `${opPartitonName}_3` VALUES LESS THAN ("30"),
            PARTITION `${opPartitonName}_4` VALUES LESS THAN ("40"),
            PARTITION `${opPartitonName}_5` VALUES LESS THAN ("50"),
            PARTITION `${opPartitonName}_6` VALUES LESS THAN ("60"),
            PARTITION `${opPartitonName}_7` VALUES LESS THAN ("70"),
            PARTITION `${opPartitonName}_8` VALUES LESS THAN ("80"),
            PARTITION `${opPartitonName}_9` VALUES LESS THAN ("90")
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
    """

    helper.ccrJobDelete()
    helper.ccrJobCreate()

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 30))


    logger.info("=== Test 1: Check partitions in src before sync case ===")
    assertTrue(helper.checkShowTimesOf("""
                                SHOW PARTITIONS
                                FROM TEST_${context.dbName}.${tableName}
                                WHERE PartitionName = \"${opPartitonName}_9\"
                                """,
                                exist, 30, "target"))
    assertTrue(helper.checkShowTimesOf("""
                                SHOW PARTITIONS
                                FROM TEST_${context.dbName}.${tableName}
                                WHERE PartitionName = \"${opPartitonName}_8\"
                                """,
                                exist, 30, "target"))

    // save the backup num of source cluster
    def show_backup_result = sql "SHOW BACKUP"
    def backup_num = show_backup_result.size()
    logger.info("backups before drop partition: ${show_backup_result}")

    logger.info("=== Test 2: Insert data in valid partitions case ===")
    test_num = 3
    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${tableName} VALUES (${test_num}, ${index})
            """
    }
    sql "sync"

    logger.info("=== Test 3: pause ===")

    helper.ccrJobPause()

    test_num = 4
    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${tableName} VALUES (${test_num}, ${index})
            """
    }

    sql "sync"

    logger.info("=== Test 4: Drop partitions case ===")
    sql """
        ALTER TABLE ${tableName}
        DROP PARTITION IF EXISTS ${opPartitonName}_9
    """

    sql """
        ALTER TABLE ${tableName}
        DROP PARTITION IF EXISTS ${opPartitonName}_8
    """
    sql "sync"

    logger.info("=== Test 5: pause and verify ===")

    helper.ccrJobResume()

    assertTrue(helper.checkShowTimesOf("""
                                SHOW PARTITIONS
                                FROM TEST_${context.dbName}.${tableName}
                                WHERE PartitionName = \"${opPartitonName}_9\"
                                """,
                                notExist, 30, "target"))
    assertTrue(helper.checkShowTimesOf("""
                                SHOW PARTITIONS
                                FROM TEST_${context.dbName}.${tableName}
                                WHERE PartitionName = \"${opPartitonName}_8\"
                                """,
                                notExist, 30, "target"))

    show_backup_result = sql "SHOW BACKUP"
    logger.info("backups after drop partition: ${show_backup_result}")
    assertTrue(show_backup_result.size() == backup_num)
}


