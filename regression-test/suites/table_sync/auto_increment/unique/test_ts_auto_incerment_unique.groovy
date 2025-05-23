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
suite("test_ts_auto_incerment_unique") {
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
        return res[0][1].contains("`uuid` bigint NOT NULL AUTO_INCREMENT(1)")
    }

    helper.enableDbBinlog()
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
    target_sql "DROP TABLE IF EXISTS ${dbNameTarget}.${tableName}"

    sql """
        CREATE TABLE if NOT EXISTS ${tableName}
        (
            `uuid` BIGINT NOT NULL AUTO_INCREMENT,
            `user_name` VARCHAR(128) NOT NULL,
            `experience` INT NOT NULL
        )
        ENGINE=OLAP
        UNIQUE KEY(`uuid`)
        DISTRIBUTED BY HASH(uuid) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
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
    for (int i = 0; i < insert_num; i++) {
        sql """
            insert into ${tableName} (user_name, experience) values ('A', ${i})
            """
    }
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName}", insert_num, 30))

    logger.info("=== Test 3: check auto increment column consistency ===")
    def ures = sql " select * from ${tableName} order by experience "
    def dres = target_sql " select * from ${tableName} order by experience "
    for (int i = 0; i < insert_num; i++) {
        assertEquals(ures[i][0], dres[i][0])
        assertEquals(ures[i][1], dres[i][1])
        assertEquals(ures[i][2], dres[i][2])
    }
}

