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
suite('test_ds_dml_insert') {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", 'helper.groovy'))

    def tableName = 'tbl_' + helper.randomSuffix()

    def exist = { res -> Boolean
        return res.size() != 0
    }

    helper.enableDbBinlog()
    helper.ccrJobDelete()
    helper.ccrJobCreate()

    // Create a aggregate key table
    sql """
        CREATE TABLE if NOT EXISTS ${tableName}
        (
            `user_id` INT,
            `value` INT SUM,
        )
        ENGINE=OLAP
        AGGREGATE KEY(`user_id`)
        PARTITION BY RANGE(user_id)
        (
            PARTITION `p1` VALUES LESS THAN ("100"),
            PARTITION `p2` VALUES LESS THAN ("200"),
            PARTITION `p3` VALUES LESS THAN ("300"),
            PARTITION `p4` VALUES LESS THAN ("400"),
            PARTITION `p5` VALUES LESS THAN ("500"),
            PARTITION `p6` VALUES LESS THAN ("600"),
            PARTITION `p7` VALUES LESS THAN MAXVALUE
        )
        DISTRIBUTED BY HASH(user_id) BUCKETS 2
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true",
            "binlog.ttl_seconds" = "180"
        )
    """

    Integer total = 1000
    for (int i = 0; i < total; i++) {
        sql """ INSERT INTO ${tableName} VALUES (${i}, ${i}) """
    }
    sql 'sync'

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 60))
    assertTrue(helper.checkShowTimesOf("SELECT * FROM ${tableName}", exist, 60, 'sql'))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName}", total, 60))

    def checkDataConsistency = { ->
        def up = sql_return_maparray """ SELECT * FROM ${tableName} ORDER BY user_id """
        def down = target_sql_return_maparray """ SELECT * FROM ${tableName} ORDER BY user_id """
        assertTrue(up.size() == down.size())
        for (int i = 0; i < up.size(); i++) {
            assertTrue(up[i].user_id == down[i].user_id)
            assertTrue(up[i].value == down[i].value)
        }
    }
    checkDataConsistency()

    // Insert again
    for (int i = 0; i < total; i++) {
        sql """ INSERT INTO ${tableName} VALUES (${i}, ${i}) """
    }
    sql """ INSERT INTO ${tableName} VALUES (1000, 1000) """
    sql 'sync'
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName}", total + 1, 60))

    checkDataConsistency()

    // Insert again when the job is paused
    helper.ccrJobPause()
    for (int i = 0; i < total; i++) {
        sql """ INSERT INTO ${tableName} VALUES (${i}, ${i}) """
    }
    sql """ INSERT INTO ${tableName} VALUES (1001, 1001) """
    sql 'sync'
    helper.ccrJobResume()
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName}", total + 2, 60))

    checkDataConsistency()
}
