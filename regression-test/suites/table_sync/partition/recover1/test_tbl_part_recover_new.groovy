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

suite("test_tbl_part_recover_new") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    if (!helper.is_version_supported([30099, 20199, 20099])) {
        // not support doris 3.0/2.1/2.0
        def version = helper.upstream_version()
        logger.info("skip this suite because version is not supported, upstream version ${version}")
        return
    }

    def tableName = "tbl_" + helper.randomSuffix()
    def test_num = 0
    def insert_num = 3
    def opPartitonName = "part"

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
            PARTITION `${opPartitonName}_1` VALUES LESS THAN ("10"),
            PARTITION `${opPartitonName}_2` VALUES LESS THAN ("100")            
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
                                WHERE PartitionName = \"${opPartitonName}_1\"
                                """,
                                exist, 30, "target"))

    assertTrue(helper.checkShowTimesOf("""
                                SHOW PARTITIONS
                                FROM TEST_${context.dbName}.${tableName}
                                WHERE PartitionName = \"${opPartitonName}_2\"
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
        DROP PARTITION IF EXISTS ${opPartitonName}_1
    """
    sql """
        ALTER TABLE ${tableName}
        DROP PARTITION IF EXISTS ${opPartitonName}_2
    """
    assertTrue(helper.checkShowTimesOf("""
                                SHOW PARTITIONS
                                FROM TEST_${context.dbName}.${tableName}
                                WHERE PartitionName = \"${opPartitonName}_1\"
                                """,
                                notExist, 30, "target"))
    assertTrue(helper.checkShowTimesOf("""
                                SHOW PARTITIONS
                                FROM TEST_${context.dbName}.${tableName}
                                WHERE PartitionName = \"${opPartitonName}_2\"
                                """,
                                notExist, 30, "target"))                                

    logger.info("=== Test 4: recover partitions case ===")
    sql """
        RECOVER PARTITION  ${opPartitonName}_1 as ${opPartitonName}_11 from ${tableName}
    """    

    assertTrue(helper.checkShowTimesOf("""
                                SHOW PARTITIONS
                                FROM TEST_${context.dbName}.${tableName}
                                WHERE PartitionName = \"${opPartitonName}_11\"
                                """,
                                exist, 30, "target"))
    sql """
        RECOVER PARTITION  ${opPartitonName}_2 as ${opPartitonName}_22 from ${tableName}
    """        
    assertTrue(helper.checkShowTimesOf("""
                                SHOW PARTITIONS
                                FROM TEST_${context.dbName}.${tableName}
                                WHERE PartitionName = \"${opPartitonName}_22\"
                                """,
                                exist, 30, "target"))                                

    test_num = 5
    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${tableName} VALUES (${test_num}, ${index})
            """
    }
    sql "sync"
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName} WHERE test=${test_num}",
                                  insert_num, 30))    

    order_qt_target_sql_content("SELECT * FROM ${tableName}")
    order_qt_sql_source_content("SELECT * FROM ${tableName}")
}
