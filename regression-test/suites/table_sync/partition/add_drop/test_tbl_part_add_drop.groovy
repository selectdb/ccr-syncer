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

suite("test_tbl_part_add_drop") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def tableName = "tbl_" + helper.randomSuffix()
    def test_num = 0
    def insert_num = 5
    def opPartitonName = "less0"

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
            PARTITION `${opPartitonName}` VALUES LESS THAN ("0")
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
    """

    helper.ccrJobCreate(tableName)

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 30))


    logger.info("=== Test 1: Check partitions in src before sync case ===")
    assertTrue(helper.checkShowTimesOf("""
                                SHOW PARTITIONS
                                FROM TEST_${context.dbName}.${tableName}
                                WHERE PartitionName = \"${opPartitonName}\"
                                """,
                                exist, 30, "target"))

    logger.info("=== Test 2: Add partitions case ===")
    opPartitonName = "one_to_five"
    sql """
        ALTER TABLE ${tableName}
        ADD PARTITION ${opPartitonName}
        VALUES [('0'), ('5'))
    """

    // add partition use bucket number 
    opBucketNumberPartitonName = "bucket_number_partition"
    sql """
        ALTER TABLE ${tableName}
        ADD PARTITION ${opBucketNumberPartitonName}
        VALUES [(5), (6)) DISTRIBUTED BY HASH(id) BUCKETS 2;
    """
    opDifferentBucketNumberPartitonName = "different_bucket_number_partition"
    sql """
        ALTER TABLE ${tableName}
        ADD PARTITION ${opDifferentBucketNumberPartitonName}
        VALUES [(6), (7)) DISTRIBUTED BY HASH(id) BUCKETS 3;
    """


    assertTrue(helper.checkShowTimesOf("""
                                SHOW PARTITIONS
                                FROM TEST_${context.dbName}.${tableName}
                                WHERE PartitionName = \"${opPartitonName}\"
                                """,
                                exist, 30, "target"))
    assertTrue(helper.checkShowTimesOf("""
                                SHOW PARTITIONS
                                FROM TEST_${context.dbName}.${tableName}
                                WHERE PartitionName = \"${opBucketNumberPartitonName}\"
                                """,
                                exist, 30, "target"))   
    assertTrue(helper.checkShowTimesOf("""
                                SHOW PARTITIONS
                                FROM TEST_${context.dbName}.${tableName}
                                WHERE PartitionName = \"${opDifferentBucketNumberPartitonName}\"
                                """,
                                exist, 30, "target"))


    logger.info("=== Test 3: Insert data in valid partitions case ===")
    test_num = 3
    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${tableName} VALUES (${test_num}, ${index})
            """
    }
    sql "sync"
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName} WHERE test=${test_num}",
                                  insert_num, 30))



    logger.info("=== Test 4: Drop partitions case ===")
    sql """
        ALTER TABLE ${tableName}
        DROP PARTITION IF EXISTS ${opPartitonName}
    """

    assertTrue(helper.checkShowTimesOf("""
                                SHOW PARTITIONS
                                FROM TEST_${context.dbName}.${tableName}
                                WHERE PartitionName = \"${opPartitonName}\"
                                """,
                                notExist, 30, "target"))
    def resSql = target_sql "SELECT * FROM ${tableName} WHERE test=3"
    assertTrue(resSql.size() == 0)
}
