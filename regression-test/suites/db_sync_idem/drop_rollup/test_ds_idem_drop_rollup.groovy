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

suite('test_ds_idem_drop_rollup') {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", 'helper.groovy'))

    if (!helper.has_feature("feature_idempotent_ddl")) {
        logger.info("this case only works with feature_idempotent_ddl")
        return
    }

    def notExist = { res -> Boolean
        return res.size() == 0
    }

    def suffix = helper.randomSuffix()
    def tableName = 'tbl_' + suffix

    helper.enableDbBinlog()
    sql """
        CREATE TABLE if NOT EXISTS ${tableName}
        (
            `test` INT,
            `id` INT
        )
        ENGINE=OLAP
        AGGREGATE KEY(`test`, `id`)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        ROLLUP (
            rollupIndex(`test`)
        )
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true",
            "binlog.ttl_seconds" = "180"
        );
    """
    helper.ccrJobDelete()
    helper.ccrJobCreate()

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 60))

    // The first failpoint is used to simulate the failure occurred before committed.
    // The second failpoint is used to simulate the failure of committed but rpc failed.
    helper.addFailpoint('handle_binlog_idempotent:before', 'DROP_ROLLUP')
    helper.addFailpoint('handle_binlog_idempotent:after', 'DROP_ROLLUP')

    logger.info('create a new table')
    sql """
        ALTER TABLE ${tableName} DROP ROLLUP `rollupIndex`
    """

    assertTrue(helper.check_table_describe_times(tableName, 30))

    sql "INSERT INTO ${tableName} VALUES (2, 10)"

    assertTrue(helper.checkSelectTimesOf("""
                            SELECT * FROM ${tableName}
                            WHERE id = 10
                            """,
                            1, 60))
}
