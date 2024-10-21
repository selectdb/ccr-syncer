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

suite("test_ts_part_add") {
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

    helper.ccrJobCreate(tableName)

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 60))

    sql """
        ALTER TABLE ${tableName} ADD PARTITION p3 VALUES LESS THAN ("200")
        """

    assertTrue(helper.checkShowTimesOf("""
                                SHOW PARTITIONS
                                FROM ${tableName}
                                WHERE PartitionName = "p3"
                                """,
                                exist, 60, "target"))

    def show_result = target_sql """SHOW PARTITIONS FROM ${tableName} WHERE PartitionName = "p3" """
    logger.info("show partition: ${show_result}")
    // columns Range
    assertTrue(show_result[0][6].contains("100"))
    assertTrue(show_result[0][6].contains("200"))

    logger.info("=== Test 2: Add list partition ===")
    tableName = "${baseTableName}_list"
    sql """
        CREATE TABLE if NOT EXISTS ${tableName}
        (
            `test` INT,
            `id` INT NOT NULL
        )
        ENGINE=OLAP
        UNIQUE KEY(`test`, `id`)
        PARTITION BY LIST(`id`)
        (
            PARTITION `p1` VALUES IN ("0", "1", "2"),
            PARTITION `p2` VALUES IN ("100", "200", "300")
        )
        DISTRIBUTED BY HASH(id) BUCKETS AUTO
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
    """

    helper.ccrJobCreate(tableName)
    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 60))

    sql """
        ALTER TABLE ${tableName} ADD PARTITION p3 VALUES IN ("500", "600", "700")
        """

    assertTrue(helper.checkShowTimesOf("""
                                SHOW PARTITIONS
                                FROM ${tableName}
                                WHERE PartitionName = "p3"
                                """,
                                exist, 60, "target"))
    show_result = target_sql """SHOW PARTITIONS FROM ${tableName} WHERE PartitionName = "p3" """
    logger.info("show partition: ${show_result}")
    // columns Range
    assertTrue(show_result[0][6].contains("500"))
    assertTrue(show_result[0][6].contains("600"))
    assertTrue(show_result[0][6].contains("700"))

    // NOTE: ccr synder does not support syncing temp partition now.
    // logger.info("=== Test 3: Add temp partition ===")
    // tableName = "${baseTableName}_temp_range"
    // sql """
    //     CREATE TABLE if NOT EXISTS ${tableName}
    //     (
    //         `test` INT,
    //         `id` INT
    //     )
    //     ENGINE=OLAP
    //     UNIQUE KEY(`test`, `id`)
    //     PARTITION BY RANGE(`id`)
    //     (
    //         PARTITION `p1` VALUES LESS THAN ("0"),
    //         PARTITION `p2` VALUES LESS THAN ("100")
    //     )
    //     DISTRIBUTED BY HASH(id) BUCKETS AUTO
    //     PROPERTIES (
    //         "replication_allocation" = "tag.location.default: 1",
    //         "binlog.enable" = "true"
    //     )
    // """

    // helper.ccrJobCreate(tableName)

    // assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 60))

    // sql """
    //     ALTER TABLE ${tableName} ADD TEMPORARY PARTITION p3 VALUES LESS THAN ("200")
    //     """

    // assertTrue(helper.checkShowTimesOf("""
    //                             SHOW TEMPORARY PARTITIONS
    //                             FROM ${tableName}
    //                             WHERE PartitionName = "p3"
    //                             """,
    //                             exist, 60, "target"))

    // sql "INSERT INTO ${tableName} TEMPORARY PARTITION (p3) VALUES (1, 150)"

    // assertTrue(helper.checkShowTimesOf("""
    //                             SELECT *
    //                             FROM ${tableName}
    //                             TEMPORARY PARTITION (p3)
    //                             WHERE id = 150
    //                             """,
    //                             exist, 60, "target"))

    logger.info("=== Test 4: Add unpartitioned partition ===")
    tableName = "${baseTableName}_unpart"
    sql """
        CREATE TABLE if NOT EXISTS ${tableName}
        (
            `test` INT,
            `id` INT NOT NULL
        )
        ENGINE=OLAP
        UNIQUE KEY(`test`, `id`)
        DISTRIBUTED BY HASH(id) BUCKETS AUTO
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
    """

    helper.ccrJobCreate(tableName)

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 60))

    def versions = sql_return_maparray "show variables like 'version_comment'"
    if (versions[0].Value.contains('doris-2.0.')) {
        logger.info("2.0 not support INSERT OVERWRITE yet, current version is: ${versions[0].Value}")
        return
    }

    sql """
        INSERT OVERWRITE TABLE ${tableName} VALUES (1, 100);
       """

    assertTrue(helper.checkShowTimesOf("""
                                SELECT * FROM ${tableName}
                                WHERE id = 100
                                """,
                                exist, 60, "target"))
}
