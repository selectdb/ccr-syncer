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

suite('test_ds_prop_incrsync_dynamic_partition') {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", 'helper.groovy'))

    def dbName = context.dbName
    def tableNameFull = 'tbl_full_' + helper.randomSuffix()
    def tableNameIncrement = 'tbl_incr_' + helper.randomSuffix()
    def policyName = 'policy_' + helper.randomSuffix()
    def resourceName = 'resource_' + helper.randomSuffix()

    def exist = { res -> Boolean
        return res.size() != 0
    }

    def checkShowResult = { target_res, property -> Boolean
        if (!target_res[0][1].contains(property)) {
            logger.info("don't contains {}", property)
            return false
        }
        return true
    }

    sql "DROP TABLE IF EXISTS ${dbName}.${tableNameFull}_range_by_day"
    sql "DROP TABLE IF EXISTS ${dbName}.${tableNameFull}_range_by_week"
    sql "DROP TABLE IF EXISTS ${dbName}.${tableNameFull}_range_by_month"
    target_sql "DROP TABLE IF EXISTS TEST_${dbName}.${tableNameFull}_range_by_day"
    target_sql "DROP TABLE IF EXISTS TEST_${dbName}.${tableNameFull}_range_by_week"
    target_sql "DROP TABLE IF EXISTS TEST_${dbName}.${tableNameFull}_range_by_month"

    sql "DROP TABLE IF EXISTS ${dbName}.${tableNameIncrement}_range_by_day"
    sql "DROP TABLE IF EXISTS ${dbName}.${tableNameIncrement}_range_by_week"
    sql "DROP TABLE IF EXISTS ${dbName}.${tableNameIncrement}_range_by_month"
    target_sql "DROP TABLE IF EXISTS TEST_${dbName}.${tableNameIncrement}_range_by_day"
    target_sql "DROP TABLE IF EXISTS TEST_${dbName}.${tableNameIncrement}_range_by_week"
    target_sql "DROP TABLE IF EXISTS TEST_${dbName}.${tableNameIncrement}_range_by_month"

    helper.enableDbBinlog()
    helper.ccrJobDelete()
    helper.ccrJobCreate()

    sql """
        CREATE TABLE if NOT EXISTS ${tableNameFull}_range_by_day
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
        CREATE TABLE if NOT EXISTS ${tableNameFull}_range_by_week
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
        CREATE TABLE if NOT EXISTS ${tableNameFull}_range_by_month
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

    assertTrue(helper.checkRestoreFinishTimesOf("${tableNameFull}_range_by_day", 30))
    assertTrue(helper.checkRestoreFinishTimesOf("${tableNameFull}_range_by_week", 30))
    assertTrue(helper.checkRestoreFinishTimesOf("${tableNameFull}_range_by_month", 30))

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableNameFull}_range_by_day\"", exist, 60, 'sql'))
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableNameFull}_range_by_week\"", exist, 60, 'sql'))
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableNameFull}_range_by_month\"", exist, 60, 'sql'))

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableNameFull}_range_by_day\"", exist, 60, 'target'))
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableNameFull}_range_by_week\"", exist, 60, 'target'))
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableNameFull}_range_by_month\"", exist, 60, 'target'))

    def target_res = target_sql "SHOW CREATE TABLE ${tableNameFull}_range_by_day"

    assertTrue(checkShowResult(target_res, '\"dynamic_partition.enable\" = \"false\"'))
    assertTrue(checkShowResult(target_res, '\"dynamic_partition.time_unit\" = \"DAY\"'))
    assertTrue(checkShowResult(target_res, '\"dynamic_partition.time_zone\" = \"Asia/Shanghai\"'))
    assertTrue(checkShowResult(target_res, '\"dynamic_partition.start\" = \"-2\"'))
    assertTrue(checkShowResult(target_res, '\"dynamic_partition.end\" = \"2\"'))
    assertTrue(checkShowResult(target_res, '\"dynamic_partition.prefix\" = \"p\"'))
    assertTrue(checkShowResult(target_res, '\"dynamic_partition.buckets\" = \"32\"'))
    assertTrue(checkShowResult(target_res, '\"dynamic_partition.history_partition_num\" = \"2\"'))
    assertTrue(checkShowResult(target_res, '\"dynamic_partition.create_history_partition\" = \"true\"'))
    assertTrue(checkShowResult(target_res, '\"dynamic_partition.reserved_history_periods\" = \"[2024-01-01,2024-12-31],[2025-01-01,2025-12-31]\"'))
    assertTrue(checkShowResult(target_res, '\"dynamic_partition.replication_allocation\" = \"tag.location.default: 1\"'))

    target_res = target_sql "SHOW CREATE TABLE ${tableNameFull}_range_by_week"

    assertTrue(checkShowResult(target_res, '\"dynamic_partition.enable\" = \"false\"'))
    assertTrue(checkShowResult(target_res, '\"dynamic_partition.time_unit\" = \"WEEK\"'))
    assertTrue(checkShowResult(target_res, '\"dynamic_partition.time_zone\" = \"Asia/Shanghai\"'))
    assertTrue(checkShowResult(target_res, '\"dynamic_partition.start\" = \"-2\"'))
    assertTrue(checkShowResult(target_res, '\"dynamic_partition.end\" = \"2\"'))
    assertTrue(checkShowResult(target_res, '\"dynamic_partition.prefix\" = \"p\"'))
    assertTrue(checkShowResult(target_res, '\"dynamic_partition.buckets\" = \"32\"'))
    assertTrue(checkShowResult(target_res, '\"dynamic_partition.history_partition_num\" = \"2\"'))
    assertTrue(checkShowResult(target_res, '\"dynamic_partition.start_day_of_week\" = \"2\"'))
    assertTrue(checkShowResult(target_res, '\"dynamic_partition.create_history_partition\" = \"true\"'))
    assertTrue(checkShowResult(target_res, '\"dynamic_partition.reserved_history_periods\" = \"[2024-01-01,2024-12-31],[2025-01-01,2025-12-31]\"'))
    assertTrue(checkShowResult(target_res, '\"dynamic_partition.replication_allocation\" = \"tag.location.default: 1\"'))

    target_res = target_sql "SHOW CREATE TABLE ${tableNameFull}_range_by_month"

    assertTrue(checkShowResult(target_res, '\"dynamic_partition.enable\" = \"false\"'))
    assertTrue(checkShowResult(target_res, '\"dynamic_partition.time_unit\" = \"MONTH\"'))
    assertTrue(checkShowResult(target_res, '\"dynamic_partition.time_zone\" = \"Asia/Shanghai\"'))
    assertTrue(checkShowResult(target_res, '\"dynamic_partition.start\" = \"-2\"'))
    assertTrue(checkShowResult(target_res, '\"dynamic_partition.end\" = \"2\"'))
    assertTrue(checkShowResult(target_res, '\"dynamic_partition.prefix\" = \"p\"'))
    assertTrue(checkShowResult(target_res, '\"dynamic_partition.buckets\" = \"32\"'))
    assertTrue(checkShowResult(target_res, '\"dynamic_partition.history_partition_num\" = \"2\"'))
    assertTrue(checkShowResult(target_res, '\"dynamic_partition.start_day_of_month\" = \"1\"'))
    assertTrue(checkShowResult(target_res, '\"dynamic_partition.create_history_partition\" = \"true\"'))
    assertTrue(checkShowResult(target_res, '\"dynamic_partition.reserved_history_periods\" = \"[2024-01-01,2024-12-31],[2025-01-01,2025-12-31]\"'))
    assertTrue(checkShowResult(target_res, '\"dynamic_partition.replication_allocation\" = \"tag.location.default: 1\"'))

    sql """ CREATE RESOURCE "${resourceName}"
        PROPERTIES
        (
            "type" = "s3",
            "s3.endpoint" = "bj.s3.com",
            "s3.region" = "bj",
            "s3.access_key" = "bbb",
            "s3.secret_key" = "aaaa",
            "s3.bucket" = "test",
            "s3_validity_check" = "false",
            "s3.root.path" = "s3://test/"
        )
        """
    sql """ CREATE STORAGE POLICY ${policyName} PROPERTIES ( "storage_resource" = "${resourceName}", "cooldown_ttl" = "5000" ) """
    sql """
        CREATE TABLE if NOT EXISTS ${tableNameIncrement}_range_by_day
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
            "dynamic_partition.replication_allocation" = "tag.location.default: 1",
            "dynamic_partition.storage_policy" = "${policyName}"
        )
    """

    sql """
        CREATE TABLE if NOT EXISTS ${tableNameIncrement}_range_by_week
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
            "dynamic_partition.replication_allocation" = "tag.location.default: 1",
            "dynamic_partition.storage_policy" = "${policyName}"
        );
    """

    sql """
        CREATE TABLE if NOT EXISTS ${tableNameIncrement}_range_by_month
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
            "dynamic_partition.replication_allocation" = "tag.location.default: 1",
            "dynamic_partition.storage_policy" = "${policyName}"
        )
    """

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableNameIncrement}_range_by_day\"", exist, 60, 'sql'))
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableNameIncrement}_range_by_week\"", exist, 60, 'sql'))
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableNameIncrement}_range_by_month\"", exist, 60, 'sql'))

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableNameIncrement}_range_by_day\"", exist, 60, 'target'))
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableNameIncrement}_range_by_week\"", exist, 60, 'target'))
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableNameIncrement}_range_by_month\"", exist, 60, 'target'))

    target_res = target_sql "SHOW CREATE TABLE ${tableNameIncrement}_range_by_day"

    assertTrue(checkShowResult(target_res, '\"dynamic_partition.enable\" = \"true\"'))
    assertTrue(checkShowResult(target_res, '\"dynamic_partition.time_unit\" = \"DAY\"'))
    assertTrue(checkShowResult(target_res, '\"dynamic_partition.time_zone\" = \"Asia/Shanghai\"'))
    assertTrue(checkShowResult(target_res, '\"dynamic_partition.start\" = \"-2\"'))
    assertTrue(checkShowResult(target_res, '\"dynamic_partition.end\" = \"2\"'))
    assertTrue(checkShowResult(target_res, '\"dynamic_partition.prefix\" = \"p\"'))
    assertTrue(checkShowResult(target_res, '\"dynamic_partition.buckets\" = \"32\"'))
    assertTrue(checkShowResult(target_res, '\"dynamic_partition.history_partition_num\" = \"2\"'))
    assertTrue(checkShowResult(target_res, '\"dynamic_partition.create_history_partition\" = \"true\"'))
    assertTrue(checkShowResult(target_res, '\"dynamic_partition.reserved_history_periods\" = \"[2024-01-01,2024-12-31],[2025-01-01,2025-12-31]\"'))
    assertTrue(checkShowResult(target_res, '\"dynamic_partition.replication_allocation\" = \"tag.location.default: 1\"'))
    assertFalse(checkShowResult(target_res, "\"dynamic_partition.storage_policy\" = \"${policyName}\""))  // don't sync storage policy

    target_res = target_sql "SHOW CREATE TABLE ${tableNameIncrement}_range_by_week"

    assertTrue(checkShowResult(target_res, '\"dynamic_partition.enable\" = \"true\"'))
    assertTrue(checkShowResult(target_res, '\"dynamic_partition.time_unit\" = \"WEEK\"'))
    assertTrue(checkShowResult(target_res, '\"dynamic_partition.time_zone\" = \"Asia/Shanghai\"'))
    assertTrue(checkShowResult(target_res, '\"dynamic_partition.start\" = \"-2\"'))
    assertTrue(checkShowResult(target_res, '\"dynamic_partition.end\" = \"2\"'))
    assertTrue(checkShowResult(target_res, '\"dynamic_partition.prefix\" = \"p\"'))
    assertTrue(checkShowResult(target_res, '\"dynamic_partition.buckets\" = \"32\"'))
    assertTrue(checkShowResult(target_res, '\"dynamic_partition.history_partition_num\" = \"2\"'))
    assertTrue(checkShowResult(target_res, '\"dynamic_partition.start_day_of_week\" = \"2\"'))
    assertTrue(checkShowResult(target_res, '\"dynamic_partition.create_history_partition\" = \"true\"'))
    assertTrue(checkShowResult(target_res, '\"dynamic_partition.reserved_history_periods\" = \"[2024-01-01,2024-12-31],[2025-01-01,2025-12-31]\"'))
    assertTrue(checkShowResult(target_res, '\"dynamic_partition.replication_allocation\" = \"tag.location.default: 1\"'))
    assertFalse(checkShowResult(target_res, "\"dynamic_partition.storage_policy\" = \"${policyName}\""))  // don't sync storage policy

    target_res = target_sql "SHOW CREATE TABLE ${tableNameIncrement}_range_by_month"

    assertTrue(checkShowResult(target_res, '\"dynamic_partition.enable\" = \"true\"'))
    assertTrue(checkShowResult(target_res, '\"dynamic_partition.time_unit\" = \"MONTH\"'))
    assertTrue(checkShowResult(target_res, '\"dynamic_partition.time_zone\" = \"Asia/Shanghai\"'))
    assertTrue(checkShowResult(target_res, '\"dynamic_partition.start\" = \"-2\"'))
    assertTrue(checkShowResult(target_res, '\"dynamic_partition.end\" = \"2\"'))
    assertTrue(checkShowResult(target_res, '\"dynamic_partition.prefix\" = \"p\"'))
    assertTrue(checkShowResult(target_res, '\"dynamic_partition.buckets\" = \"32\"'))
    assertTrue(checkShowResult(target_res, '\"dynamic_partition.history_partition_num\" = \"2\"'))
    assertTrue(checkShowResult(target_res, '\"dynamic_partition.start_day_of_month\" = \"1\"'))
    assertTrue(checkShowResult(target_res, '\"dynamic_partition.create_history_partition\" = \"true\"'))
    assertTrue(checkShowResult(target_res, '\"dynamic_partition.reserved_history_periods\" = \"[2024-01-01,2024-12-31],[2025-01-01,2025-12-31]\"'))
    assertTrue(checkShowResult(target_res, '\"dynamic_partition.replication_allocation\" = \"tag.location.default: 1\"'))
    assertFalse(checkShowResult(target_res, "\"dynamic_partition.storage_policy\" = \"${policyName}\""))  // don't sync storage policy
}
