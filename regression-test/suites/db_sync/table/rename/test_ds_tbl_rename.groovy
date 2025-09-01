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

suite("test_ds_tbl_rename") {
    def versions = sql_return_maparray "show variables like 'version_comment'"
    if (versions[0].Value.contains('doris-2.0.') || versions[0].Value.contains('doris-2.1.')) {
        logger.info("2.0/2.1 not support this case, current version is: ${versions[0].Value}")
        return
    }

    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def tableName = "tbl_" + helper.randomSuffix()
    def test_num = 0
    def insert_num = 10
    def opPartitonName = "less"
    def new_rollup_name = "rn_new"

    def exist = { res -> Boolean
        return res.size() != 0
    }
    def notExist = { res -> Boolean
        return res.size() == 0
    }

    def hasRollupFull = { res -> Boolean
        for (List<Object> row : res) {
            if ((row[0] as String) == "${new_rollup_name}") {
                return true
            }
        }
        return false
    }

    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} 
        (
            `id` int, 
            `no` int,
            `name` varchar(10)
        ) ENGINE = olap 
        UNIQUE KEY(`id`, `no`) 
        DISTRIBUTED BY HASH(`id`) BUCKETS 2  
        PROPERTIES (
            "replication_num" = "1", 
            "binlog.enable" = "true", 
            "enable_unique_key_merge_on_write" = "false"
        );
    """
    sql """ INSERT INTO ${tableName} VALUES (2, 1, 'b') """
    sql """ ALTER TABLE ${tableName} ADD ROLLUP rn (no, id) """

    sql """
        CREATE TABLE IF NOT EXISTS ${tableName}_1
        (
        `user_id` LARGEINT NOT NULL COMMENT "用户id",
        `date` DATE NOT NULL COMMENT "数据灌入日期时间",
        `cost` BIGINT SUM DEFAULT "0" COMMENT "用户总消费"
        ) ENGINE = olap
        AGGREGATE KEY(`user_id`, `date`)
        PARTITION BY RANGE (`date`)
        (
         PARTITION `p201701` VALUES LESS THAN ("2017-02-01"),
         PARTITION `p201702` VALUES LESS THAN ("2017-03-01"),
         PARTITION `p201703` VALUES LESS THAN ("2017-04-01")
        )
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 2
        PROPERTIES ("replication_num" = "1", "binlog.enable" = "true");
    """
    helper.enableDbBinlog()
    sql """ INSERT INTO ${tableName}_1 VALUES (1, '2017-03-30', 1), (2, '2017-03-29', 2), (3, '2017-03-28', 1) """

    helper.ccrJobDelete()
    helper.ccrJobCreate()

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 60))
    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}_1", 60))

    logger.info("=== Test 0: Db sync ===")
    sql "sync"
    assertTrue(helper.checkShowTimesOf("SELECT * FROM ${tableName} ", exist, 60, "target"))
    assertTrue(helper.checkShowTimesOf("SELECT * FROM ${tableName}_1 ", exist, 60, "target"))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName}", 1, 30))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName}_1", 3, 30))

    logger.info("=== Test 1: Rename rollup case ===")
    sql "ALTER TABLE ${tableName} RENAME ROLLUP rn ${new_rollup_name}; "
    sql "sync"
    assertTrue(helper.checkShowTimesOf("SELECT * FROM ${tableName} ", exist, 60, "target"))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName} ", 1, 30))
    assertTrue(helper.checkShowTimesOf("""desc ${tableName} all """, hasRollupFull, 60, "target"))

    logger.info("=== Test 2: Rename partition case ===")
    sql "ALTER TABLE ${tableName}_1 RENAME PARTITION p201702 p201702_new "
    sql "sync"
    assertTrue(helper.checkShowTimesOf("SELECT * FROM ${tableName}_1 ", exist, 60, "target"))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName}_1 p201702_new ", 3, 30))

    logger.info("=== Test 3: Rename table case ===")
    def newTableName = "NEW_${tableName}"
    sql "ALTER TABLE ${tableName} RENAME ${newTableName}"
    sql "sync"
    assertTrue(helper.checkShowTimesOf("SELECT * FROM ${newTableName} ", exist, 60, "target"))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${newTableName} WHERE id = 2", 1, 30))
}

