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

suite("test_cds_part_add_drop") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def baseTableName = "test_" + helper.randomSuffix()
    def test_num = 0
    def insert_num = 5
    def opPartitonName = "less0"

    def exist = { res -> Boolean
        return res.size() != 0
    }
    def notExist = { res -> Boolean
        return res.size() == 0
    }

    logger.info("=== Test 1: Add range partition ===")
    def tableName = "${baseTableName}_range"
    sql """
        CREATE TABLE if NOT EXISTS ${tableName}
        (
            `test` INT,
            `id` INT NOT NULL
        )
        ENGINE=OLAP
        UNIQUE KEY(`test`, `id`)
        PARTITION BY RANGE(`id`)
        (
            PARTITION `p1` VALUES LESS THAN ("0"),
            PARTITION `p2` VALUES LESS THAN ("100")
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

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 60))

    logger.info("Add partition and drop")

    def first_job_progress = helper.get_job_progress()

    sql """
        ALTER TABLE ${tableName} ADD PARTITION p3 VALUES LESS THAN ("200")
        """

    assertTrue(helper.checkShowTimesOf("""
                                SHOW PARTITIONS
                                FROM ${tableName}
                                WHERE PartitionName = "p3"
                                """,
                                exist, 60, "sql"))

    sql "INSERT INTO ${tableName} VALUES (1, 150)"

    sql """
        ALTER TABLE ${tableName} DROP PARTITION p3
        """

    sql "INSERT INTO ${tableName} VALUES (2, 10)"

    helper.ccrJobResume()

    assertTrue(helper.checkSelectTimesOf("""
                                SELECT * FROM ${tableName}
                                WHERE id = 150
                                """,
                                0, 60))
    assertTrue(helper.checkSelectTimesOf("""
                                SELECT * FROM ${tableName}
                                WHERE id = 10
                                """,
                                1, 60))

    // no fullsync are triggered
    def last_job_progress = helper.get_job_progress()
    assertTrue(last_job_progress.full_sync_start_at == first_job_progress.full_sync_start_at)
}
