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

suite('test_ds_idem_rename_table') {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", 'helper.groovy'))

    if (!helper.has_feature("feature_idempotent_ddl")) {
        logger.info("this case only works with feature_idempotent_ddl")
        return
    }

    def notExist = { res -> Boolean
        return res.size() == 0
    }
    def exist = { res -> Boolean
        return res.size() == 0
    }
    def dbName = context.dbName
    def suffix = helper.randomSuffix()
    def tableName = 'tbl_' + suffix
    def newTableName = 'tbl_new_' + suffix

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql "DROP TABLE IF EXISTS ${newTableName}"
    target_sql "DROP TABLE IF EXISTS ${tableName}"
    target_sql "DROP TABLE IF EXISTS ${newTableName}"


    helper.enableDbBinlog()
    sql """
        CREATE TABLE if NOT EXISTS ${tableName}
        (
            `test` INT,
            `id` INT
        )
        ENGINE=OLAP
        UNIQUE KEY(`test`, `id`)
        PARTITION BY RANGE(id)
        (
            PARTITION `p1` VALUES LESS THAN ("100"),
            PARTITION `p2` VALUES LESS THAN ("200")
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true",
            "binlog.ttl_seconds" = "180"
        )
    """
    helper.ccrJobDelete()
    helper.ccrJobCreate()

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 60))

    helper.addFailpoint('handle_binlog_idempotent:before', 'RENAME_TABLE')
    helper.addFailpoint('handle_binlog_idempotent:after', 'RENAME_TABLE')

    sql """
        ALTER TABLE ${tableName} RENAME ${newTableName}
    """

    assertTrue(helper.checkShowTimesOf(""" SHOW TABLES LIKE "${newTableName}" """, exist, 30, "target"))

    assertTrue(helper.checkShowTimesOf(""" SHOW TABLES LIKE "${tableName}" """, notExist, 30, "target"))

    sql "INSERT INTO ${newTableName} VALUES (2, 10)"

    assertTrue(helper.checkSelectTimesOf("""
                            SELECT * FROM ${newTableName}
                            WHERE id = 10
                            """,
                            1, 60))
}
