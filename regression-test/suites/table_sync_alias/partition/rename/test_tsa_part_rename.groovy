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

suite("test_tsa_part_rename") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    // only works on 3.0.4/2.1.8/2.0.16
    if (!helper.is_version_supported([30004, 20108, 20016])) {
        // at least doris 3.0.3, 2.1.8 and doris 2.0.16
        def version = helper.upstream_version()
        logger.info("skip this suite because version is not supported, upstream version ${version}")
        return
    }

    def tableName = "tbl_" + helper.randomSuffix()
    def aliasTableName = "alias_tbl_" + helper.randomSuffix()
    def test_num = 0
    def insert_num = 5
    def opPartitonNameOrigin = "partitionName_1"
    def opPartitonNameNew = "partitionName_2"

    helper.set_alias(aliasTableName)

    def exist = { res -> Boolean
        return res.size() != 0
    }

    def notExist = { res -> Boolean
        return res.size() == 0
    }

    helper.enableDbBinlog()

    sql "DROP TABLE IF EXISTS ${context.dbName}.${tableName}"
    target_sql "DROP TABLE IF EXISTS TEST_${context.dbName}.${aliasTableName}"

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
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
    """

    helper.ccrJobDelete(tableName)
    helper.ccrJobCreate(tableName)

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 30))

    logger.info("=== Test 1: Add partitions case ===")

    sql """
        ALTER TABLE ${tableName}
        ADD PARTITION ${opPartitonNameOrigin}
        VALUES [('0'), ('5'))
    """

    assertTrue(helper.checkShowTimesOf("""
                                SHOW PARTITIONS
                                FROM ${tableName}
                                WHERE PartitionName = \"${opPartitonNameOrigin}\"
                                """,
                                exist, 30, "sql"))

    assertTrue(helper.checkShowTimesOf("""
                                SHOW PARTITIONS
                                FROM ${aliasTableName}
                                WHERE PartitionName = \"${opPartitonNameNew}\"
                                """,
                                notExist, 30, "target"))

    logger.info("=== Test 2: Check new partitions not exist ===")

    assertTrue(helper.checkShowTimesOf("""
                                SHOW PARTITIONS
                                FROM ${tableName}
                                WHERE PartitionName = \"${opPartitonNameNew}\"
                                """,
                                notExist, 30, "sql"))

    assertTrue(helper.checkShowTimesOf("""
                                SHOW PARTITIONS
                                FROM ${aliasTableName}
                                WHERE PartitionName = \"${opPartitonNameNew}\"
                                """,
                                notExist, 30, "target"))
                            
    logger.info("=== Test 3: Rename partitions name ===")

    sql """
        ALTER TABLE ${tableName} RENAME PARTITION ${opPartitonNameOrigin} ${opPartitonNameNew}    
        """

    logger.info("=== Test 4: Check new partitions exist and origin partition not exist ===")

    assertTrue(helper.checkShowTimesOf("""
                                SHOW PARTITIONS
                                FROM ${tableName}
                                WHERE PartitionName = \"${opPartitonNameOrigin}\"
                                """,
                                notExist, 30, "sql"))

    assertTrue(helper.checkShowTimesOf("""
                                SHOW PARTITIONS
                                FROM ${tableName}
                                WHERE PartitionName = \"${opPartitonNameNew}\"
                                """,
                                exist, 30, "sql"))

    assertTrue(helper.checkShowTimesOf("""
                                SHOW PARTITIONS
                                FROM ${aliasTableName}
                                WHERE PartitionName = \"${opPartitonNameOrigin}\"
                                """,
                                notExist, 30, "target"))

    assertTrue(helper.checkShowTimesOf("""
                                SHOW PARTITIONS
                                FROM ${aliasTableName}
                                WHERE PartitionName = \"${opPartitonNameNew}\"
                                """,
                                exist, 30, "target"))

    logger.info("=== Test 5: Check new partitions key and range ===")

    show_result = target_sql_return_maparray """SHOW PARTITIONS FROM TEST_${context.dbName}.${aliasTableName} WHERE PartitionName = \"${opPartitonNameNew}\" """

    assertEquals(show_result[0].Range, "[types: [INT]; keys: [0]; ..types: [INT]; keys: [5]; )")
}
