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

suite("test_ts_alt_prop_dy_pary") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def dbName = context.dbName
    def tableName = "tbl_" + helper.randomSuffix()

    def exist = { res -> Boolean
        return res.size() != 0
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

    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
    target_sql "DROP TABLE IF EXISTS TEST_${dbName}.${tableName}"

    helper.enableDbBinlog()

    sql """
        CREATE TABLE if NOT EXISTS ${tableName}
        (
            `test` INT,
            `id` INT
        )
        ENGINE=OLAP
        AGGREGATE KEY(`test`, `id`)
        PARTITION BY RANGE(`id`)
        (
        )
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

    helper.ccrJobDelete(tableName)
    helper.ccrJobCreate(tableName)

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 30))

    logger.info("=== Test 1: check property not exist ===")

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}\"", exist, 60, "sql"))

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}\"", exist, 60, "target"))

    assertTrue(helper.checkShowTimesOf("SHOW CREATE TABLE ${tableName}", existOldPartitionProperty, 60, "sql"))

    logger.info("=== Test 2: alter table set property dynamic partition ===")

    sql """
        ALTER TABLE ${tableName} SET ("dynamic_partition.time_unit" = "WEEK")
        """
    sql """
        ALTER TABLE ${tableName} SET ("dynamic_partition.start" = "-3")
        """
    sql """
        ALTER TABLE ${tableName} SET ("dynamic_partition.end" = "3")
        """
    sql """
        ALTER TABLE ${tableName} SET ("dynamic_partition.prefix" = "pp")
        """
    sql """
        ALTER TABLE ${tableName} SET ("dynamic_partition.create_history_partition" = "false")
        """
    sql """
        ALTER TABLE ${tableName} SET ("dynamic_partition.buckets" = "64")
        """
    sql """
        ALTER TABLE ${tableName} SET ("dynamic_partition.history_partition_num" = "1")
        """
    sql """
        ALTER TABLE ${tableName} SET ("dynamic_partition.reserved_history_periods" = "[2023-01-01,2023-12-31],[2024-01-01,2024-12-31]")
        """

    logger.info("=== Test 3: check property exist ===")

    assertTrue(helper.checkShowTimesOf("SHOW CREATE TABLE ${tableName}", existNewPartitionProperty, 60, "sql"))

    // don't sync
    assertTrue(helper.checkShowTimesOf("SHOW CREATE TABLE ${tableName}", existOldPartitionProperty, 60, "target"))
}