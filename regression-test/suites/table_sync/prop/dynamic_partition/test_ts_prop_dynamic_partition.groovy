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

suite("test_ts_prop_dynamic_partition") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def dbName = context.dbName
    def tableName = "tbl_" + helper.randomSuffix()
    def test_num = 0
    def insert_num = 5

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

    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}_range_by_day"
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}_range_by_week"
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}_range_by_month"
    target_sql "DROP TABLE IF EXISTS TEST_${dbName}.${tableName}_range_by_day"
    target_sql "DROP TABLE IF EXISTS TEST_${dbName}.${tableName}_range_by_week"
    target_sql "DROP TABLE IF EXISTS TEST_${dbName}.${tableName}_range_by_month"


    helper.enableDbBinlog()

    sql """
        CREATE TABLE if NOT EXISTS ${tableName}_range_by_day
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

    sql """
        CREATE TABLE if NOT EXISTS ${tableName}_range_by_week
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
            "dynamic_partition.time_unit" = "WEEK",
            "dynamic_partition.time_zone" = "Asia/Shanghai",
            "dynamic_partition.start" = "-2",
            "dynamic_partition.end" = "2",
            "dynamic_partition.prefix" = "p",
            "dynamic_partition.buckets" = "32",
            "dynamic_partition.create_history_partition" = "true",
            "dynamic_partition.start_day_of_week" = "2",
            "dynamic_partition.history_partition_num" = "2",
            "dynamic_partition.reserved_history_periods" = "[2024-01-01,2024-12-31],[2025-01-01,2025-12-31]",
            "dynamic_partition.replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        CREATE TABLE if NOT EXISTS ${tableName}_range_by_month
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
            "dynamic_partition.time_unit" = "MONTH",
            "dynamic_partition.time_zone" = "Asia/Shanghai",
            "dynamic_partition.start" = "-2",
            "dynamic_partition.end" = "2",
            "dynamic_partition.prefix" = "p",
            "dynamic_partition.buckets" = "32",
            "dynamic_partition.create_history_partition" = "true",
            "dynamic_partition.history_partition_num" = "2",
            "dynamic_partition.start_day_of_month" = "1",
            "dynamic_partition.reserved_history_periods" = "[2024-01-01,2024-12-31],[2025-01-01,2025-12-31]",
            "dynamic_partition.replication_allocation" = "tag.location.default: 1"
        )
    """

    helper.ccrJobDelete(tableName + "_range_by_day")
    helper.ccrJobDelete(tableName + "_range_by_week")
    helper.ccrJobDelete(tableName + "_range_by_month")
    helper.ccrJobCreate(tableName + "_range_by_day")
    helper.ccrJobCreate(tableName + "_range_by_week")
    helper.ccrJobCreate(tableName + "_range_by_month")

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}_range_by_day", 30))
    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}_range_by_week", 30))
    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}_range_by_month", 30))

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}_range_by_day\"", exist, 60, "sql"))
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}_range_by_week\"", exist, 60, "sql"))
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}_range_by_month\"", exist, 60, "sql"))

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}_range_by_day\"", exist, 60, "target"))
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}_range_by_week\"", exist, 60, "target"))
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}_range_by_month\"", exist, 60, "target"))

    def target_res = target_sql "SHOW CREATE TABLE ${tableName}_range_by_day"

    assertTrue(checkShowResult(target_res, "\"dynamic_partition.enable\" = \"false\""))
    assertTrue(checkShowResult(target_res, "\"dynamic_partition.time_unit\" = \"DAY\""))
    assertTrue(checkShowResult(target_res, "\"dynamic_partition.time_zone\" = \"Asia/Shanghai\""))
    assertTrue(checkShowResult(target_res, "\"dynamic_partition.start\" = \"-2\""))
    assertTrue(checkShowResult(target_res, "\"dynamic_partition.end\" = \"2\""))
    assertTrue(checkShowResult(target_res, "\"dynamic_partition.prefix\" = \"p\""))
    assertTrue(checkShowResult(target_res, "\"dynamic_partition.buckets\" = \"32\""))
    assertTrue(checkShowResult(target_res, "\"dynamic_partition.history_partition_num\" = \"2\""))
    assertTrue(checkShowResult(target_res, "\"dynamic_partition.create_history_partition\" = \"true\""))
    assertTrue(checkShowResult(target_res, "\"dynamic_partition.reserved_history_periods\" = \"[2024-01-01,2024-12-31],[2025-01-01,2025-12-31]\""))
    assertTrue(checkShowResult(target_res, "\"dynamic_partition.replication_allocation\" = \"tag.location.default: 1\""))

    target_res = target_sql "SHOW CREATE TABLE ${tableName}_range_by_week"

    assertTrue(checkShowResult(target_res, "\"dynamic_partition.enable\" = \"false\""))
    assertTrue(checkShowResult(target_res, "\"dynamic_partition.time_unit\" = \"WEEK\""))
    assertTrue(checkShowResult(target_res, "\"dynamic_partition.time_zone\" = \"Asia/Shanghai\""))
    assertTrue(checkShowResult(target_res, "\"dynamic_partition.start\" = \"-2\""))
    assertTrue(checkShowResult(target_res, "\"dynamic_partition.end\" = \"2\""))
    assertTrue(checkShowResult(target_res, "\"dynamic_partition.prefix\" = \"p\""))
    assertTrue(checkShowResult(target_res, "\"dynamic_partition.buckets\" = \"32\""))
    assertTrue(checkShowResult(target_res, "\"dynamic_partition.history_partition_num\" = \"2\""))
    assertTrue(checkShowResult(target_res, "\"dynamic_partition.start_day_of_week\" = \"2\""))
    assertTrue(checkShowResult(target_res, "\"dynamic_partition.create_history_partition\" = \"true\""))
    assertTrue(checkShowResult(target_res, "\"dynamic_partition.reserved_history_periods\" = \"[2024-01-01,2024-12-31],[2025-01-01,2025-12-31]\""))
    assertTrue(checkShowResult(target_res, "\"dynamic_partition.replication_allocation\" = \"tag.location.default: 1\""))

    target_res = target_sql "SHOW CREATE TABLE ${tableName}_range_by_month"

    assertTrue(checkShowResult(target_res, "\"dynamic_partition.enable\" = \"false\""))
    assertTrue(checkShowResult(target_res, "\"dynamic_partition.time_unit\" = \"MONTH\""))
    assertTrue(checkShowResult(target_res, "\"dynamic_partition.time_zone\" = \"Asia/Shanghai\""))
    assertTrue(checkShowResult(target_res, "\"dynamic_partition.start\" = \"-2\""))
    assertTrue(checkShowResult(target_res, "\"dynamic_partition.end\" = \"2\""))
    assertTrue(checkShowResult(target_res, "\"dynamic_partition.prefix\" = \"p\""))
    assertTrue(checkShowResult(target_res, "\"dynamic_partition.buckets\" = \"32\""))
    assertTrue(checkShowResult(target_res, "\"dynamic_partition.history_partition_num\" = \"2\""))
    assertTrue(checkShowResult(target_res, "\"dynamic_partition.start_day_of_month\" = \"1\""))
    assertTrue(checkShowResult(target_res, "\"dynamic_partition.create_history_partition\" = \"true\""))
    assertTrue(checkShowResult(target_res, "\"dynamic_partition.reserved_history_periods\" = \"[2024-01-01,2024-12-31],[2025-01-01,2025-12-31]\""))
    assertTrue(checkShowResult(target_res, "\"dynamic_partition.replication_allocation\" = \"tag.location.default: 1\""))
}