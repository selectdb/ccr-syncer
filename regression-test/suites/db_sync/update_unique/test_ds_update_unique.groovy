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

suite("test_ds_update_unique") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def tableName = "tbl_" + helper.randomSuffix()
    def test_num = 0
    def insert_num = 10

    def exist = { res -> Boolean
        return res.size() != 0
    }
    def notExist = { res -> Boolean
        return res.size() == 0
    }

    helper.enableDbBinlog()

    sql """
        CREATE TABLE IF NOT EXISTS ${tableName}
        (
          `user_id` LARGEINT NOT NULL COMMENT "用户id",
          `date` DATE NOT NULL COMMENT "数据灌入日期时间",
          `city` VARCHAR(20) COMMENT "用户所在城市"
        ) ENGINE = olap
        unique KEY(`user_id`, `date`)
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
        PROPERTIES ("replication_num" = "1", "binlog.enable" = "true");
    """

    sql """ insert into ${tableName} values (1, '2017-03-31', 'a'), (2, '2017-02-28', 'b'), (3, '2017-02-28', 'c'); """

    helper.ccrJobDelete()
    helper.ccrJobCreate()

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 60))


    logger.info("=== Test 0: Db sync ===")
    assertTrue(helper.checkShowTimesOf("SELECT * FROM ${tableName} ", exist, 60, "target"))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName}", 3, 30))

    logger.info("=== Test 1: update data ===")
    sql """ update ${tableName} set city = 'd' where user_id = 1; """

    logger.info("=== Test 2: check update data ===")
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName} where city = 'd'", 1, 30))
}
