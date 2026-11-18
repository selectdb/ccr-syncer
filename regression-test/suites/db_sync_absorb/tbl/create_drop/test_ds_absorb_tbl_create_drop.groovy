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
suite("test_ds_absorb_tbl_create_drop") {

    logger.info("Snapshot drop operations are not synchronized downstream so this test is not useful for the moment")
    return

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

    sql """
        CREATE TABLE if NOT EXISTS ${tableName}_1
        (
            `test` INT,
            `id` INT
        )
        ENGINE=OLAP
        UNIQUE KEY(`test`, `id`)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
    """

    helper.enableDbBinlog()
    helper.ccrJobDelete()
    helper.ccrJobCreate()

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}_1", 180))
    assertTrue(helper.checkShowTimesOf(""" SHOW TABLES LIKE "${tableName}_1" """, exist, 60, "sql"))

    // 0. Insert N data
    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${tableName}_1 VALUES (${test_num}, ${index})
            """
    }
    assertTrue(helper.checkShowTimesOf(""" select * from ${tableName}_1 """, { r -> r.size() == insert_num}, 60, "sql"))
    assertTrue(helper.checkShowTimesOf(""" select * from ${tableName}_1 """, { r -> r.size() == insert_num}, 60, "target"))
    // 1. Pause ccr job
    helper.ccrJobPause()

    // 2. Insert N data
    for (int index = insert_num; index < insert_num * 2; index++) {
        sql """
            INSERT INTO ${tableName}_1 VALUES (${test_num}, ${index})
            """
    }

    // 3. Do operation & wait it finishes upstream
    sql """
        CREATE TABLE if NOT EXISTS ${tableName}_2
        (
            `test` INT,
            `id` INT
        )
        ENGINE=OLAP
        UNIQUE KEY(`test`, `id`)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
    """
    sql "DROP TABLE ${tableName}_1 FORCE"
    assertTrue(helper.checkShowTimesOf(""" SHOW TABLES LIKE "${tableName}_1" """, notExist, 180, "sql"))

    // 4. Insert N data
    for (int index = insert_num; index < insert_num * 2; index++) {
        sql """
            INSERT INTO ${tableName}_2 VALUES (${test_num}, ${index})
            """
    }
    assertTrue(helper.checkShowTimesOf(""" select * from ${tableName}_2 """, { r -> r.size() == insert_num }, 180, "sql"))

    // 5. Force trigger fullsnapshot
    helper.force_fullsync()

    // 6. Resume ccr job
    helper.ccrJobResume()
  
    // 7. Verify data and operation are synced downstream
    assertTrue(helper.checkShowTimesOf(""" SHOW TABLES LIKE "${tableName}_2" """, exist, 60, "target"))
    assertTrue(helper.checkShowTimesOf(""" select * from ${tableName}_2 """, { r -> r.size() == insert_num }, 180, "target"))
    assertTrue(helper.checkShowTimesOf(""" SHOW TABLES LIKE "${tableName}_1" """, notExist, 180, "target"))
}