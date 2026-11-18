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
suite("test_ts_auto_incer_update_val") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def dbName = context.dbName
    def dbNameTarget = "TEST_" + context.dbName
    def tableName = "tbl_" + helper.randomSuffix()
    def test_num = 0
    def insert_num = 100

    def exist = { res -> Boolean
        return res.size() != 0
    }

    def existAutoIncrement = { res -> Boolean
        return res[0][1].contains("`aid` bigint NOT NULL AUTO_INCREMENT(1)")
    }

    helper.enableDbBinlog()
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
    target_sql "DROP TABLE IF EXISTS ${dbNameTarget}.${tableName}"

    sql """
        CREATE TABLE if NOT EXISTS ${tableName}
        (
            `uuid` BIGINT NOT NULL ,
            `user_name` VARCHAR(128) NOT NULL,
            `experience` INT NOT NULL,
            `aid` BIGINT NOT NULL AUTO_INCREMENT
        )
        ENGINE=OLAP
        UNIQUE KEY(`uuid`)
        DISTRIBUTED BY HASH(uuid) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "enable_unique_key_merge_on_write" = "true",
            "binlog.enable" = "true"
        )
    """

    helper.ccrJobDelete(tableName)
    helper.ccrJobCreate(tableName)

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 30))
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}\"", exist, 30, "sql"))
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}\"", exist, 30, "target"))

    logger.info("=== Test 1: check table exist auto increment column ===")
    assertTrue(helper.checkShowTimesOf("SHOW CREATE TABLE ${tableName}", existAutoIncrement, 30, "target"))

    logger.info("=== Test 2: insert some data ===")
    sql """
        insert into ${tableName} (uuid, user_name, experience) values (1, 'Alice', 1), (2, 'Bob', 2), (3, 'Carve', 3), (4, 'Colo', 4)
        """
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName}", 4, 30))

    logger.info("=== Test 3: set variables ===")
    sql """
        set enable_unique_key_partial_update=true;
        set enable_insert_strict=false;
        """
    logger.info("=== Test 4: update some column without increment column ===")
    sql """
        insert into ${tableName} (uuid, user_name) values (4, 'Darvin')
        """
    
    logger.info("=== Test 5: check value update ===")
    helper.checkSelectTimesOf("select * from ${tableName} where user_name = 'Darvin' and uuid = 4", 1, 30)
    
    logger.info("=== Test 6: update some column with increment column ===")
    sql """
        insert into ${tableName} (uuid, aid) values (4, 100)
        """
    
    logger.info("=== Test 7: check value update ===")
    helper.checkSelectTimesOf("select * from ${tableName} where aid = 100 and uuid = 4", 1, 30)
}

