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
suite("test_ds_absorb_tbl_alt_prop_dynamic_partition_sp") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def dbName = context.dbName
    def tableName = "tbl_" + helper.randomSuffix()
    def policyName = 'policy_' + helper.randomSuffix()
    def resourceName = 'resource_' + helper.randomSuffix()
    def test_num = 0
    def insert_num = 10
	
	String ak = getS3AK()
    String sk = getS3SK()
    String s3_endpoint = getS3Endpoint()
    String region = getS3Region()
    String bucket = context.config.otherConfigs.get("s3BucketName");
	
    def exist = { res -> Boolean
        return res.size() != 0
    }

    def notExist = { res -> Boolean
        return res.size() == 0
    }

    def checkShowResult = { target_res, property -> Boolean
        if (!target_res[0][1].contains(property)) {
            logger.info("don't contains {}", property)
            return false
        }
        return true
    }

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

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 180))
    assertTrue(helper.checkShowTimesOf(""" SHOW TABLES LIKE "${tableName}" """, exist, 60, "sql"))
    assertTrue(helper.checkShowTimesOf(""" SHOW TABLES LIKE "${tableName}" """, exist, 60, "target"))

    // 0. Insert N data
    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${tableName} VALUES (${test_num}, ${index}, curdate())
            """
    }
    assertTrue(helper.checkShowTimesOf(""" select * from ${tableName} """, { r -> r.size() == insert_num}, 60, "sql"))
    assertTrue(helper.checkShowTimesOf(""" select * from ${tableName} """, { r -> r.size() == insert_num}, 60, "target"))
    // 1. Pause ccr job
    helper.ccrJobPause()

    // 2. Insert N data
    for (int index = insert_num; index < insert_num * 2; index++) {
        sql """
            INSERT INTO ${tableName} VALUES (${test_num}, ${index}, curdate())
            """
    }

    // 3. Do operation & wait it finishes upstream
    sql """ ALTER TABLE ${tableName} set ("dynamic_partition.storage_policy" = "${policyName}") """
    sql """ ALTER TABLE ${tableName} set ("dynamic_partition.end" = "3") """

    // 4. Insert N data
    for (int index = insert_num * 2; index < insert_num * 3; index++) {
        sql """
            INSERT INTO ${tableName} VALUES (${test_num}, ${index}, curdate())
            """
    }

    assertTrue(helper.checkShowTimesOf(""" select * from ${tableName} """, { r -> r.size() == insert_num * 3}, 60, "sql"))

    assertTrue(helper.checkShowTimesOf("SHOW CREATE TABLE ${tableName}", { res -> return res[0][1].contains("\"dynamic_partition.end\" = \"3\"")}, 60, 'sql'))
    assertTrue(helper.checkShowTimesOf("SHOW CREATE TABLE ${tableName}", { res -> return res[0][1].contains("\"dynamic_partition.storage_policy\" = \"${policyName}\"")}, 60, 'sql'))
    // 5. Force trigger fullsnapshot
    helper.force_fullsync()

    // 6. Resume ccr job
    helper.ccrJobResume()
  
    // 7. Verify data and operation are synced downstream
    assertTrue(helper.checkShowTimesOf(""" select * from ${tableName} """, { r -> r.size() == insert_num * 3}, 60, "target"))

    assertTrue(helper.checkShowTimesOf("SHOW CREATE TABLE ${tableName}", { res -> return res[0][1].contains("\"dynamic_partition.end\" = \"3\"")}, 60, 'target'))
    assertTrue(helper.checkShowTimesOf("SHOW CREATE TABLE ${tableName}", { res -> return !res[0][1].contains("\"dynamic_partition.storage_policy\" = \"${policyName}\"")}, 60, 'target'))

}