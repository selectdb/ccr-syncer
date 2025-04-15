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

suite('test_ds_alt_prop_dy_part_sp') {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", 'helper.groovy'))

    def dbName = context.dbName
    def tableName = 'tbl_' + helper.randomSuffix()
    def policyName = 'policy_' + helper.randomSuffix()
    def resourceName = 'resource_' + helper.randomSuffix()
	
	String ak = getS3AK()
    String sk = getS3SK()
    String s3_endpoint = getS3Endpoint()
    String region = getS3Region()
    String bucket = context.config.otherConfigs.get("s3BucketName");
	
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

    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
    target_sql "DROP TABLE IF EXISTS TEST_${dbName}.${tableName}"

    helper.enableDbBinlog()

    sql """ CREATE RESOURCE "${resourceName}"
		PROPERTIES
        (
            "type" = "s3",
            "s3.endpoint" = "${s3_endpoint}",
            "s3.region" = "${region}",
            "s3.access_key"= "${ak}",
            "s3.secret_key" = "${sk}",
            "s3.bucket" = "${bucket}",
			"s3_validity_check" = "false",
            "s3.root.path" = "s3://regression/"
        );
        """
    sql """ CREATE STORAGE POLICY ${policyName} PROPERTIES ( "storage_resource" = "${resourceName}", "cooldown_ttl" = "5000" ) """
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

    helper.ccrJobDelete()
    helper.ccrJobCreate()

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 180))
    
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}\"", exist, 60, 'sql'))

    // alter op will delay to the downstream, so modify one more prop to ensure the op has been operated to the downstream
    sql """ ALTER TABLE ${tableName} set ("dynamic_partition.storage_policy" = "${policyName}") """
    sql """ ALTER TABLE ${tableName} set ("dynamic_partition.end" = "3") """
    assertTrue(helper.checkShowTimesOf("SHOW CREATE TABLE ${tableName}", { res -> return res[0][1].contains("\"dynamic_partition.storage_policy\" = \"${policyName}\"")}, 60, 'sql'))
    assertTrue(helper.checkShowTimesOf("SHOW CREATE TABLE ${tableName}", { res -> return res[0][1].contains("\"dynamic_partition.end\" = \"3\"")}, 60, 'sql'))

    assertTrue(helper.checkShowTimesOf("SHOW CREATE TABLE ${tableName}", { res -> return res[0][1].contains("\"dynamic_partition.end\" = \"3\"")}, 60, 'target'))
    assertTrue(helper.checkShowTimesOf("SHOW CREATE TABLE ${tableName}", { res -> return !res[0][1].contains("\"dynamic_partition.storage_policy\" = \"${policyName}\"")}, 60, 'target'))
}
