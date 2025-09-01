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

suite('test_ds_prop_incrsync_storage_policy') {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", 'helper.groovy'))

    def dbName = context.dbName
    def tableNameFull = 'tbl_full_' + helper.randomSuffix()
    def tableNameIncrement = 'tbl_incr_' + helper.randomSuffix()

    def exist = { res -> Boolean
        return res.size() != 0
    }

    sql "DROP TABLE IF EXISTS ${dbName}.${tableNameFull}"
    target_sql "DROP TABLE IF EXISTS TEST_${dbName}.${tableNameFull}"
    sql "DROP TABLE IF EXISTS ${dbName}.${tableNameIncrement}"
    target_sql "DROP TABLE IF EXISTS TEST_${dbName}.${tableNameIncrement}"

    def resource_name = "res_" + helper.randomSuffix()
    def policy_name= "policy_" + helper.randomSuffix()

    def check_storage_policy_exist = { name->
        def polices = sql'''
        show storage policy;
        '''
        for (p in polices) {
            if (name == p[0]) {
                return true
            }
        }
        return false
    }

    def has_resouce = sql """
        SHOW RESOURCES WHERE NAME = "${resource_name}";
    """

    if (check_storage_policy_exist(policy_name)) {
        sql """
            DROP STORAGE POLICY ${policy_name}
        """
    }

    if (has_resouce.size() > 0) {
        sql """
            DROP RESOURCE ${resource_name}
        """
    }

    sql """
        CREATE RESOURCE IF NOT EXISTS "${resource_name}"
        PROPERTIES(
            "type"="s3",
            "AWS_ENDPOINT" = "${getS3Endpoint()}",
            "AWS_REGION" = "${getS3Region()}",
            "AWS_ROOT_PATH" = "regression/cooldown",
            "AWS_ACCESS_KEY" = "${getS3AK()}",
            "AWS_SECRET_KEY" = "${getS3SK()}",
            "AWS_MAX_CONNECTIONS" = "50",
            "AWS_REQUEST_TIMEOUT_MS" = "3000",
            "AWS_CONNECTION_TIMEOUT_MS" = "1000",
            "AWS_BUCKET" = "${getS3BucketName()}",
            "s3_validity_check" = "true"
        );
    """

    sql """
        CREATE STORAGE POLICY IF NOT EXISTS ${policy_name}
        PROPERTIES(
            "storage_resource" = "${resource_name}",
            "cooldown_ttl" = "300"
        )
    """

    helper.enableDbBinlog()

    sql """
        CREATE TABLE if NOT EXISTS ${tableNameFull}
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
            "storage_policy" = "${policy_name}"
        )
    """

    helper.ccrJobDelete()
    helper.ccrJobCreate()

    assertTrue(helper.checkRestoreFinishTimesOf("${tableNameFull}", 30))
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableNameFull}\"", exist, 60, 'sql'))
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableNameFull}\"", exist, 60, 'target'))

    // storage_policy should't be synchronized
    def res = sql "SHOW CREATE TABLE ${tableNameFull}"
    def target_res = target_sql "SHOW CREATE TABLE ${tableNameFull}"
    assertTrue(res[0][1].contains("\"storage_policy\" = \"${policy_name}\""))
    assertTrue(!target_res[0][1].contains("\"storage_policy\" = \"${policy_name}\""))

    sql """
        CREATE TABLE if NOT EXISTS ${tableNameIncrement}
        (
            `test` INT,
            `id` INT
        )
        ENGINE=OLAP
        AGGREGATE KEY(`test`, `id`)
        PARTITION BY RANGE(`id`)
        (
            PARTITION p0 VALUES LESS THAN (100) ("storage_policy" = "${policy_name}"),
            PARTITION p1 VALUES LESS THAN (200) ("storage_policy" = "${policy_name}"),
            PARTITION p2 VALUES LESS THAN (300)
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true",
            "storage_policy" = "${policy_name}"
        )
    """

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableNameIncrement}\"", exist, 60, 'sql'))
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableNameIncrement}\"", exist, 60, 'target'))

    // storage_policy should't be synchronized
    res = sql "SHOW CREATE TABLE ${tableNameIncrement}"
    target_res = target_sql "SHOW CREATE TABLE ${tableNameIncrement}"
    logger.info("SHOW CREATE TABLE ${tableNameIncrement} : ${res[0][1]}")
    logger.info("Target: SHOW CREATE TABLE ${tableNameIncrement} : ${target_res[0][1]}")
    assertTrue(res[0][1].contains("\"storage_policy\" = \"${policy_name}\""))
    assertTrue(!target_res[0][1].contains("\"storage_policy\" = \"${policy_name}\""))
}
