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

suite("test_ds_view_create_func") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def dbName = context.dbName
    def viewName = "test_view_with_function"
    def tableName = "tbl_" + helper.randomSuffix()

    def exist = { res -> Boolean
        return res.size() != 0
    }

    def notExist = { res -> Boolean
        return res.size() == 0
    }

    sql "DROP VIEW IF EXISTS ${viewName}"
    sql "DROP FUNCTION IF EXISTS view_function(INT)"
    sql "DROP TABLE IF EXISTS ${tableName}"
    target_sql "DROP TABLE IF EXISTS ${tableName}"
    target_sql "DROP VIEW IF EXISTS ${viewName}"
    target_sql "DROP FUNCTION IF EXISTS view_function(INT)"

    helper.enableDbBinlog()

    sql """
        CREATE TABLE ${tableName} 
        (
            id INT,
            value1 INT,
            value2 VARCHAR(50)
        ) 
        distributed by hash(id) buckets 10 
        properties
        (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        );
    """

    helper.ccrJobDelete()
    helper.ccrJobCreate()

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 30))

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}\"", exist, 60, "sql"))

    logger.info("=== Test 1: create function and check ===")

    sql "CREATE ALIAS FUNCTION view_function(INT) WITH PARAMETER(id) AS CONCAT(LEFT(id, 3), '****', RIGHT(id, 4));"

    assertTrue(helper.checkShowTimesOf("SHOW FULL FUNCTIONS LIKE 'view_function'", exist, 60, "sql"))

    assertTrue(helper.checkShowTimesOf("SHOW FULL FUNCTIONS LIKE 'view_function'", notExist, 60, "target"))

    logger.info("=== Test 2: create view with function ===")

    sql "CREATE VIEW ${viewName} AS (select view_function(id) as c1,abs(id) from ${tableName});"

    logger.info("=== Test 3: check origin view exist and target not exist ===")

    assertTrue(helper.checkShowTimesOf("SHOW VIEW FROM ${tableName}", exist, 60, "sql"))

    assertTrue(helper.checkShowTimesOf("SHOW VIEW FROM ${tableName}", notExist, 60, "target"))
}