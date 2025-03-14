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
suite("test_ds_absorb_tbl_alt_prop_storage_policy") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def dbName = context.dbName
    def tableName = "tbl" + helper.randomSuffix()
    def test_num = 0
    def insert_num = 5

    def exist = { res -> Boolean
        return res.size() != 0
    }

    def notExist = { res -> Boolean
        return res.size() == 0
    }
    def existPolicy = { res -> Boolean
        return res[0][1].contains("\"storage_policy\" = \"test_policy\"")
    }

    def notExistPolicy = { res -> Boolean
        return !res[0][1].contains("\"storage_policy\" = \"test_policy\"")
    }

    def resource_name = "test_ds_absorb_tbl_alt_prop_storage_policy_resource"
    def policy_name= "test_policy" + helper.randomSuffix()

    def check_storage_policy_exist = { name->
        def polices = sql"""
        show storage policy;
        """
        for (p in polices) {
            if (name == p[0]) {
                return true;
            }
        }
        return false;
    }

    if (check_storage_policy_exist(policy_name)) {
        sql """
            DROP STORAGE POLICY ${policy_name}
        """
    }

    def has_resouce = sql """
        SHOW RESOURCES WHERE NAME = "${resource_name}";
    """

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

    sql """
        CREATE TABLE if NOT EXISTS ${tableName}
        (
            `test` INT,
            `id` INT
        )
        ENGINE=OLAP
        AGGREGATE KEY(`test`, `id`)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
    """

    helper.enableDbBinlog()
    helper.ccrJobDelete()
    helper.ccrJobCreate()

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 180))
    assertTrue(helper.checkShowTimesOf(""" SHOW TABLES LIKE "${tableName}" """, exist, 60, "sql"))
    assertTrue(helper.checkShowTimesOf(""" SHOW TABLES LIKE "${tableName}" """, exist, 60, "target"))
    assertTrue(helper.checkShowTimesOf("SHOW CREATE TABLE ${tableName}", notExistPolicy, 60, "sql"))
    assertTrue(helper.checkShowTimesOf("SHOW CREATE TABLE ${tableName}", notExistPolicy, 60, "target"))

    // 1. Pause ccr job
    helper.ccrJobPause()

    // 2. Insert N data
    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${tableName} VALUES (${test_num}, ${index})
            """
    }

    // 3. Do operation & wait it finishes upstream
    sql """
        ALTER TABLE ${tableName} set ("storage_policy" = "${policy_name}");
        """
    // 4. Insert N data
    for (int index = insert_num; index < insert_num * 2; index++) {
        sql """
            INSERT INTO ${tableName} VALUES (${test_num}, ${index})
            """
    }

    assertTrue(helper.checkShowTimesOf(""" select * from ${tableName} """, { r -> r.size() == insert_num * 2}, 60, "sql"))
    assertTrue(helper.checkShowTimesOf("SHOW CREATE TABLE ${tableName}", existPolicy, 60, "sql"))
    // don't synced
    assertTrue(helper.checkShowTimesOf("SHOW CREATE TABLE ${tableName}", notExistPolicy, 60, "target"))
    // 5. Force trigger fullsnapshot
    helper.force_fullsync()

    // 6. Resume ccr job
    helper.ccrJobResume()
  
    // 7. Verify data and operation are synced downstream
    assertTrue(helper.checkShowTimesOf(""" select * from ${tableName} """, { r -> r.size() == insert_num * 2}, 60, "target"))
    assertTrue(helper.checkShowTimesOf("SHOW CREATE TABLE ${tableName}", existPolicy, 60, "sql"))
    assertTrue(helper.checkShowTimesOf("SHOW CREATE TABLE ${tableName}", notExistPolicy, 60, "target"))
}