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

suite("test_txn_insert_db") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    if (!helper.has_feature("feature_txn_insert")) {
        logger.info("Skip the test because the feature is not supported.")
        return
    }

    def tableName1 = "t1_" + helper.randomSuffix()
    def tableName2 = "t2_" + helper.randomSuffix()
    def tableName3 = "t3_" + helper.randomSuffix()
    def tableName4 = "t4_" + helper.randomSuffix()
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

    helper.enableDbBinlog()

    sql """
        CREATE TABLE IF NOT EXISTS ${tableName1}
        (
          `user_id` LARGEINT NOT NULL COMMENT "用户id",
          `date` DATE NOT NULL COMMENT "数据灌入日期时间",
          `city` VARCHAR(20) COMMENT "用户所在城市"
        ) ENGINE = olap
        unique KEY(`user_id`, `date`)
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

    sql """
    CREATE TABLE IF NOT EXISTS ${tableName3}
        (
            id int,
            name varchar(20)
        ) ENGINE = olap
        UNIQUE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 3
        PROPERTIES (
        "binlog.enable" = "true", "enable_unique_key_merge_on_write" = "false","replication_allocation" = "tag.location.default: 1");
    """

    sql """
    CREATE TABLE IF NOT EXISTS ${tableName4}
        (
            id int,
            name varchar(20)
        ) ENGINE = olap
        UNIQUE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 3
        PROPERTIES (
        "binlog.enable" = "true", "enable_unique_key_merge_on_write" = "false","replication_allocation" = "tag.location.default: 1");
    """

    sql """ insert into ${tableName1} values (1, '2017-03-31', 'a'), (2, '2017-02-28', 'b'), (3, '2017-02-28', 'c'); """
    sql """ insert into ${tableName2} values (3),(4),(5); """
    sql """ insert into ${tableName3} values (111, 'aa'),(222, 'bb'),(333, 'cc'); """
    sql """ insert into ${tableName4} values (12, 'xx'),(23, 'yy'),(34, 'aa'), (45, 'bb'), (56, 'cc'), (67, 'cc') """

    helper.ccrJobDelete()
    helper.ccrJobCreate()

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName1}", 60))
    assertTrue(helper.checkRestoreFinishTimesOf("${tableName2}", 60))
    assertTrue(helper.checkRestoreFinishTimesOf("${tableName3}", 60))
    assertTrue(helper.checkRestoreFinishTimesOf("${tableName4}", 60))


    logger.info("=== Test 0: Db sync ===")
    sql "sync"
    assertTrue(helper.checkShowTimesOf("SELECT * FROM ${tableName1} ", exist, 60, "target"))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName1}", 3, 30))
    assertTrue(helper.checkShowTimesOf("SELECT * FROM ${tableName2} ", exist, 60, "target"))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName2}", 3, 30))
    assertTrue(helper.checkShowTimesOf("SELECT * FROM ${tableName3} ", exist, 60, "target"))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName3}", 3, 30))
    assertTrue(helper.checkShowTimesOf("SELECT * FROM ${tableName4} ", exist, 60, "target"))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName4}", 6, 30))

    logger.info("=== Test 1: insert only ===")
    sql """ 
        begin;
        insert into ${tableName1} select id, '2017-02-28', 'y1' from ${tableName4} where id = 23;
        insert into ${tableName2} select id from ${tableName4} where id = 12;
        commit;
        """
    assertTrue(helper.checkShowTimesOf("SELECT * FROM ${tableName1} ", exist, 60, "target"))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName1} ", 4, 30))
    assertTrue(helper.checkShowTimesOf("SELECT * FROM ${tableName2} ", exist, 60, "target"))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName2} ", 4, 30))


    logger.info("=== Test 2: insert A + delete B ===")
    sql """
        set delete_without_partition = true;
        begin;
        insert into ${tableName2} select id from ${tableName4} where id = 23;
        delete from ${tableName1} where user_id = 1;
        commit;
    """
    assertTrue(helper.checkShowTimesOf("SELECT * FROM ${tableName1} ", exist, 60, "target"))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName1} ", 3, 30))
    assertTrue(helper.checkShowTimesOf("SELECT * FROM ${tableName2} ", exist, 60, "target"))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName2} ", 5, 30))


    logger.info("=== Test 3: insert A + delete B + update B ===")
    sql """
        set delete_without_partition = true;
        begin;
        insert into ${tableName2} select id from ${tableName4} where id = 34;
        delete from ${tableName1} where user_id = 2;
        update ${tableName1} set city = 'new' where user_id = 3;
        commit;
    """
    assertTrue(helper.checkShowTimesOf("SELECT * FROM ${tableName1} ", exist, 60, "target"))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName1} where city = 'new'", 1, 30))
    assertTrue(helper.checkShowTimesOf("SELECT * FROM ${tableName2} ", exist, 60, "target"))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName2} where id = 34", 1, 30))



    logger.info("=== Test 4: insert A + update B + delete C ===")
    sql """
        begin;
        insert into ${tableName1} select id,'2017-03-01','xyz' from ${tableName4}  where id = 45;
        delete from ${tableName2} where id = 34;
        update ${tableName3} set name = 'new' where id = 111;
        commit;
    """
    assertTrue(helper.checkShowTimesOf("SELECT * FROM ${tableName1} ", exist, 60, "target"))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName1} where city = 'xyz' and date = '2017-03-01'", 1, 30))
    assertTrue(helper.checkShowTimesOf("SELECT * FROM ${tableName2} ", exist, 60, "target"))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName2}", 5, 30))
    assertTrue(helper.checkShowTimesOf("SELECT * FROM ${tableName3} ", exist, 60, "target"))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName3} where name = 'new'", 1, 30))
}


