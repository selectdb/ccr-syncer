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
suite("test_ds_absorb_auto_incre_dup") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def dbName = context.dbName
    def dbNameTarget = "TEST_" + context.dbName
    def tableName = "tbl_" + helper.randomSuffix()
    def tableName2 = "tbl_" + helper.randomSuffix()
    def test_num = 0
    def insert_num = 30

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
        DUPLICATE KEY(`uuid`)
        DISTRIBUTED BY HASH(uuid) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
    """

    helper.ccrJobDelete()
    helper.ccrJobCreate()

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 30))
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}\"", exist, 30, "sql"))
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}\"", exist, 30, "target"))
    assertTrue(helper.checkShowTimesOf("SHOW CREATE TABLE ${tableName}", existAutoIncrement, 30, "target"))

    // 0. Insert N data
    for (int index = 0; index < insert_num; index++) {
        sql """
            insert into ${tableName} (user_name, experience) values ('A', ${index})
            """
    }
    assertTrue(helper.checkShowTimesOf(""" select * from ${tableName} """, { r -> r.size() == insert_num}, 60, "sql"))
    assertTrue(helper.checkShowTimesOf(""" select * from ${tableName} """, { r -> r.size() == insert_num}, 60, "target"))
    // 1. Pause ccr job
    helper.ccrJobPause()

    // 2. Insert N data
    for (int index = insert_num; index < insert_num * 2; index++) {
        sql """
            insert into ${tableName} (user_name, experience) values ('A', ${index})
            """
    }

    assertTrue(helper.checkShowTimesOf(""" select * from ${tableName} """, { r -> r.size() == insert_num * 2}, 60, "sql"))

    // 3. Do operation & wait it finishes upstream
    sql """
        CREATE TABLE if NOT EXISTS ${tableName2}
        (
            `uuid` BIGINT NOT NULL AUTO_INCREMENT,
            `user_name` VARCHAR(128) NOT NULL,
            `experience` INT NOT NULL
        )
        ENGINE=OLAP
        DUPLICATE KEY(`uuid`)
        DISTRIBUTED BY HASH(uuid) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
        """


    // 4. Insert N data
    for (int index = insert_num * 2; index < insert_num * 3; index++) {
        sql """
            insert into ${tableName} (user_name, experience) values ('A', ${index})
            """
    }
    for (int index = 0; index < insert_num * 3; index++) {
        sql """
            insert into ${tableName2} (user_name, experience) values ('A', ${index})
            """
    }

    assertTrue(helper.checkShowTimesOf(""" select * from ${tableName} """, { r -> r.size() == insert_num * 3 }, 60, "sql"))

    // 5. Force trigger fullsnapshot
    helper.force_fullsync()

    // 6. Resume ccr job
    helper.ccrJobResume()
  
    // 7. Verify data and operation are synced downstream
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName}", insert_num * 3, 30))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName2}", insert_num * 3, 30))
    assertTrue(helper.checkShowTimesOf("SHOW CREATE TABLE ${tableName2}", existAutoIncrement, 30, "target"))
    def ures = sql " select * from ${tableName} order by experience "
    def dres = target_sql " select * from ${tableName} order by experience "
    for (int i = 0; i < insert_num; i++) {
        assertEquals(ures[i][0], dres[i][0])
        assertEquals(ures[i][1], dres[i][1])
        assertEquals(ures[i][2], dres[i][2])
    }
    ures = sql " select * from ${tableName2} order by experience "
    dres = target_sql " select * from ${tableName2} order by experience "
    for (int i = 0; i < insert_num; i++) {
        assertEquals(ures[i][0], dres[i][0])
        assertEquals(ures[i][1], dres[i][1])
        assertEquals(ures[i][2], dres[i][2])
    }
}

