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
suite("test_ds_absorb_tbl_create_alt_dy_part") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def dbName = context.dbName
    def tableName = "tbl_" + helper.randomSuffix()
    def test_num = 0
    def insert_num = 10

    def exist = { res -> Boolean
        return res.size() != 0
    }
    def notExist = { res -> Boolean
        return res.size() == 0
    }
    def checkShowResult = { target_res, property -> Boolean
        if(!target_res[0][1].contains(property)){
            logger.info("don't contains {}", property)
            return false
        }
            return true 
    }
    
    def existNewPartitionProperty = { target_res -> Boolean
        Boolean result = checkShowResult(target_res, "\"dynamic_partition.time_unit\" = \"WEEK\"") &&
        checkShowResult(target_res, "\"dynamic_partition.start\" = \"-3\"") &&
        checkShowResult(target_res, "\"dynamic_partition.end\" = \"3\"") &&
        checkShowResult(target_res, "\"dynamic_partition.prefix\" = \"pp\"") &&
        checkShowResult(target_res, "\"dynamic_partition.buckets\" = \"64\"") &&
        checkShowResult(target_res, "\"dynamic_partition.history_partition_num\" = \"1\"") &&
        checkShowResult(target_res, "\"dynamic_partition.create_history_partition\" = \"false\"") &&
        checkShowResult(target_res, "\"dynamic_partition.reserved_history_periods\" = \"[2023-01-01,2023-12-31],[2024-01-01,2024-12-31]\"")
        return result
    }

    def existOldPartitionProperty = { res -> Boolean
        Boolean result = checkShowResult(res, "\"dynamic_partition.time_unit\" = \"DAY\"") &&
        checkShowResult(res, "\"dynamic_partition.start\" = \"-2\"") &&
        checkShowResult(res, "\"dynamic_partition.end\" = \"2\"") &&
        checkShowResult(res, "\"dynamic_partition.prefix\" = \"p\"") &&
        checkShowResult(res, "\"dynamic_partition.buckets\" = \"32\"") &&
        checkShowResult(res, "\"dynamic_partition.history_partition_num\" = \"2\"") &&
        checkShowResult(res, "\"dynamic_partition.create_history_partition\" = \"true\"") &&
        checkShowResult(res, "\"dynamic_partition.reserved_history_periods\" = \"[2024-01-01,2024-12-31],[2025-01-01,2025-12-31]\"")
        return result
    }

    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}_1"
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}_2"

    sql """
        CREATE TABLE if NOT EXISTS ${tableName}_1
        (
            `id` INT,
            `test` INT,
            `create_dt`  DATE
        )
        ENGINE=OLAP
        DUPLICATE KEY(`id`)
        PARTITION BY RANGE(create_dt) ()
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true",
            "dynamic_partition.enable" = "true",
            "dynamic_partition.time_unit" = "DAY",
            "dynamic_partition.time_zone" = "Asia/Shanghai",
            "dynamic_partition.start" = "-2",
            "dynamic_partition.end" = "2",
            "dynamic_partition.prefix" = "p",
            "dynamic_partition.buckets" = "32",
            "dynamic_partition.create_history_partition" = "true",
            "dynamic_partition.history_partition_num" = "2",
            "dynamic_partition.reserved_history_periods" = "[2024-01-01,2024-12-31],[2025-01-01,2025-12-31]",
            "dynamic_partition.replication_allocation" = "tag.location.default: 1"
        )
    """

    helper.enableDbBinlog()
    helper.ccrJobDelete()
    helper.ccrJobCreate()

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}_1", 180))
    assertTrue(helper.checkShowTimesOf(""" SHOW TABLES LIKE "${tableName}_1" """, exist, 60, "sql"))
    assertTrue(helper.checkShowTimesOf(""" SHOW TABLES LIKE "${tableName}_1" """, exist, 60, "target"))

    // 1. Pause ccr job
    helper.ccrJobPause()

    // 2. Insert N data
    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${tableName}_1 VALUES (${test_num}, ${index}, curdate())
            """
    }

    // 3. Do operation & wait it finishes upstream
    assertTrue(helper.checkShowTimesOf("SHOW CREATE TABLE ${tableName}_1", existOldPartitionProperty, 60, "sql"))
    assertTrue(helper.checkShowTimesOf("SHOW CREATE TABLE ${tableName}_1", existOldPartitionProperty, 60, "target"))

    sql """
        CREATE TABLE if NOT EXISTS ${tableName}_2
        (
            `id` INT,
            `test` INT,
            `create_dt`  DATE
        )
        ENGINE=OLAP
        DUPLICATE KEY(`id`)
        PARTITION BY RANGE(create_dt) ()
        DISTRIBUTED BY HASH(create_dt) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true",
            "dynamic_partition.enable" = "true",
            "dynamic_partition.time_unit" = "DAY",
            "dynamic_partition.time_zone" = "Asia/Shanghai",
            "dynamic_partition.start" = "-2",
            "dynamic_partition.end" = "2",
            "dynamic_partition.prefix" = "p",
            "dynamic_partition.buckets" = "32",
            "dynamic_partition.create_history_partition" = "true",
            "dynamic_partition.history_partition_num" = "2",
            "dynamic_partition.reserved_history_periods" = "[2024-01-01,2024-12-31],[2025-01-01,2025-12-31]",
            "dynamic_partition.replication_allocation" = "tag.location.default: 1"
        )
    """
    sql """
        ALTER TABLE ${tableName}_1 SET ("dynamic_partition.enable" = "true")
        """
    sql """
        ALTER TABLE ${tableName}_1 SET ("dynamic_partition.time_unit" = "WEEK")
        """
    sql """
        ALTER TABLE ${tableName}_1 SET ("dynamic_partition.start" = "-3")
        """
    sql """
        ALTER TABLE ${tableName}_1 SET ("dynamic_partition.end" = "3")
        """
    sql """
        ALTER TABLE ${tableName}_1 SET ("dynamic_partition.prefix" = "pp")
        """
    sql """
        ALTER TABLE ${tableName}_1 SET ("dynamic_partition.create_history_partition" = "false")
        """
    sql """
        ALTER TABLE ${tableName}_1 SET ("dynamic_partition.buckets" = "64")
        """
    sql """
        ALTER TABLE ${tableName}_1 SET ("dynamic_partition.history_partition_num" = "1")
        """
    sql """
        ALTER TABLE ${tableName}_1 SET ("dynamic_partition.reserved_history_periods" = "[2023-01-01,2023-12-31],[2024-01-01,2024-12-31]")
        """
    assertTrue(helper.checkShowTimesOf(""" SHOW TABLES LIKE "${tableName}_2" """, exist, 60, "sql"))
    assertTrue(helper.checkShowTimesOf(""" SHOW TABLES LIKE "${tableName}_2" """, notExist, 60, "target"))

    // 4. Insert N data
    for (int index = insert_num; index < insert_num * 2; index++) {
        sql """
            INSERT INTO ${tableName}_1 VALUES (${test_num}, ${index}, curdate())
            """
    }
    for (int index = 0; index < insert_num * 2; index++) {
        sql """
            INSERT INTO ${tableName}_2 VALUES (${test_num}, ${index}, curdate())
            """
    }

    assertTrue(helper.checkShowTimesOf(""" select * from ${tableName}_1 """, { r -> r.size() == insert_num * 2}, 60, "sql"))
    assertTrue(helper.checkShowTimesOf(""" select * from ${tableName}_2 """, { r -> r.size() == insert_num * 2}, 60, "sql"))

    assertTrue(helper.checkShowTimesOf("SHOW CREATE TABLE ${tableName}_1", existNewPartitionProperty, 60, "sql"))
    assertTrue(helper.checkShowTimesOf("SHOW CREATE TABLE ${tableName}_1", existOldPartitionProperty, 60, "target"))

    // 5. Force trigger fullsnapshot
    helper.force_fullsync()

    // 6. Resume ccr job
    helper.ccrJobResume()
  
    // 7. Verify data and operation are synced downstream
    assertTrue(helper.checkShowTimesOf(""" select * from ${tableName}_1 """, { r -> r.size() == insert_num * 2}, 60, "target"))
    assertTrue(helper.checkShowTimesOf(""" select * from ${tableName}_2 """, { r -> r.size() == insert_num * 2}, 60, "target"))
    assertTrue(helper.checkShowTimesOf("SHOW CREATE TABLE ${tableName}_1", existNewPartitionProperty, 60, "sql"))
    assertTrue(helper.checkShowTimesOf("SHOW CREATE TABLE ${tableName}_1", existNewPartitionProperty, 60, "target"))
}