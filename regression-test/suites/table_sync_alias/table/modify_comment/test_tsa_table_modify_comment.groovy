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

suite("test_tsa_table_modify_comment") {

    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    if (!helper.is_version_supported([20108, 20017, 30004])) {
        def version = helper.upstream_version()
        logger.info("Skip the test case because the version is not supported. current version ${version}")
    }

    def tableName = "tbl_" + helper.randomSuffix()
    def aliasName = "alias_" + helper.randomSuffix()

    def checkTableCommentTimesOf = { checkTable, expectedComment, times -> Boolean
        def expected = "COMMENT '${expectedComment}'"
        def res = target_sql "SHOW CREATE TABLE ${checkTable}"
        while (times > 0) {
            if (res.size() > 0 && (res[0][1] as String).contains(expected)) {
                return true
            }
            if (--times > 0) {
                sleep(helper.sync_gap_time)
                res = target_sql "SHOW CREATE TABLE ${checkTable}"
            }
        }
        return false
    }

    helper.set_alias(aliasName)

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName}
        (
            `test` INT,
            `id` INT
        )
        ENGINE=OLAP
        UNIQUE KEY(`test`, `id`)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
        """

    helper.ccrJobDelete(tableName)
    helper.ccrJobCreate(tableName)

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 30))

    logger.info("=== Test 1: modify table comment case ===")
    sql """
        ALTER TABLE ${tableName}
        MODIFY COMMENT "this is a test table"
        """
    assertTrue(checkTableCommentTimesOf(aliasName, "this is a test table", 30))
}
