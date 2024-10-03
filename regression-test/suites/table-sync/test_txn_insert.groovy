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

suite("test_txn_insert") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def tableName1 = "tbl_txn_insert_1_" + helper.randomSuffix()
    def tableName2 = "tbl_txn_insert_2_" + helper.randomSuffix()
    def test_num = 0
    def insert_num = 10

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
        CREATE TABLE IF NOT EXISTS ${tableName1}
        (
          `user_id` LARGEINT NOT NULL COMMENT "用户id",
          `date` DATE NOT NULL COMMENT "数据灌入日期时间",
          `city` VARCHAR(20) COMMENT "用户所在城市"
        ) ENGINE = olap
        unique KEY(`user_id`, `date`, `city`)
        PARTITION BY RANGE (`date`)
        (
            PARTITION `p201701` VALUES LESS THAN ("2017-02-01"),
            PARTITION `p201702` VALUES LESS THAN ("2017-03-01"),
            PARTITION `p201703` VALUES LESS THAN ("2017-04-01")
        )
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
        PROPERTIES ("replication_num" = "1", "binlog.enable" = "true","enable_unique_key_merge_on_write" = "false");
    """

    sql """
    CREATE TABLE IF NOT EXISTS ${tableName2} (`id` int)
       ENGINE = olap unique KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 2
        PROPERTIES
        ("replication_allocation" = "tag.location.default: 1", "binlog.enable" = "true", "enable_unique_key_merge_on_write" = "false");
    """

    sql """ insert into ${tableName2} values (3),(4),(5); """
    sql """ insert into ${tableName1} values (1, '2017-03-31', 'a'), (2, '2017-02-28', 'b'); """

    helper.ccrJobDelete(tableName1)
    helper.ccrJobCreate(tableName1)

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName1}", 60))


    logger.info("=== Test 0: Table sync ===")
    sql "sync"
    assertTrue(helper.checkShowTimesOf("SELECT * FROM ${tableName1} ", exist, 60, "target"))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName1}", 2, 30))

    logger.info("=== Test 1: Txn insert, but insert only, no update and delete ===")
    sql """ 
        begin; 
        insert into ${tableName1}  select id, '2017-02-28', 'y1' from ${tableName2} where id = 3;
        insert into ${tableName1}  select id, '2017-02-28', 'y1' from ${tableName2} where id = 5;
        insert into ${tableName1}  select id, '2017-03-31', 'x' from ${tableName2} where id = 4;
        commit;
        """
    assertTrue(helper.checkShowTimesOf("SELECT * FROM ${tableName1} ", exist, 60, "target"))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName1} ", 5, 30))

}



