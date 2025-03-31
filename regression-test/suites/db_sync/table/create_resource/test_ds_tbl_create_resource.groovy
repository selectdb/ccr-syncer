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

suite("test_ds_tbl_create_resource") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def dbName = context.dbName
    def viewName = "view_" + helper.randomSuffix()
    def tableName = "tbl_" + helper.randomSuffix()

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

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql "DROP VIEW IF EXISTS ${viewName}"
    target_sql "DROP TABLE IF EXISTS ${tableName}"
    target_sql "DROP TABLE IF EXISTS ${viewName}"

    def resource_name = "test_ds_tbl_create_resource_resource"

    sql """ DROP RESOURCE IF EXISTS '${resource_name}' """
    sql """
        CREATE RESOURCE "${resource_name}"
        PROPERTIES
        (
            "type" = "s3",
            "s3.endpoint" = "${s3_endpoint}",
            "s3.region" = "${region}",
            "s3.access_key"= "${ak}",
            "s3.secret_key" = "${sk}",
            "s3.bucket" = "${bucket}"
        );
        """


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
            "binlog.enable" = "true"
        )
    """

    helper.ccrJobDelete()
    helper.ccrJobCreate()

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 30))

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}\"", exist, 60, "sql"))

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}\"", exist, 60, "target"))

    sql """
        create view ${viewName} as
        SELECT * FROM S3 (
                           "uri" = "https://${bucket}.${s3_endpoint}/regression/tvf/test_hive_text.text",
                           "format" = "hive_text",
                           "csv_schema"="k1:int;k2:string;k3:double",
                           "resource" = "${resource_name}"
                       )  where k1 > 100  order by k3,k2,k1;
        """

    assertTrue(helper.checkShowTimesOf("SHOW VIEWS LIKE \"${viewName}\"", exist, 60, "sql"))

    assertTrue(helper.checkShowTimesOf("SHOW VIEWS LIKE \"${viewName}\"", notExist, 60, "target"))
}