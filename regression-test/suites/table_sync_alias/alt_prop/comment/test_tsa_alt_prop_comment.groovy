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

suite("test_tsa_alt_prop_comment") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def dbName = context.dbName
    def tableName = "tbl_" + helper.randomSuffix()
    def aliasTableName = "tbl_alias_" + helper.randomSuffix()
    helper.set_alias(aliasTableName)

    def exist = { res -> Boolean
        return res.size() != 0
    }
    def existComment = { res -> Boolean
        return res[0][1].contains("COMMENT 'test_comment'")
    }

    def notExistComment = { res -> Boolean
        return !res[0][1].contains("COMMENT 'test_comment'")
    }

    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
    target_sql "DROP TABLE IF EXISTS TEST_${dbName}.${aliasTableName}"

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

    helper.ccrJobDelete(tableName)
    helper.ccrJobCreate(tableName)

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 30))

    logger.info("=== Test 1: check property not exist ===")

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}\"", exist, 60, "sql"))

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${aliasTableName}\"", exist, 60, "target"))

    assertTrue(helper.checkShowTimesOf("SHOW CREATE TABLE ${tableName}", notExistComment, 60, "sql"))

    assertTrue(helper.checkShowTimesOf("SHOW CREATE TABLE ${aliasTableName}", notExistComment, 60, "target"))

    logger.info("=== Test 2: alter table set property colocate_with ===")

    sql """
        ALTER TABLE ${tableName} MODIFY COMMENT "test_comment"
        """

    logger.info("=== Test 3: check property exist ===")

    assertTrue(helper.checkShowTimesOf("SHOW CREATE TABLE ${tableName}", existComment, 60, "sql"))

    assertTrue(helper.checkShowTimesOf("SHOW CREATE TABLE ${aliasTableName}", existComment, 60, "target"))
}