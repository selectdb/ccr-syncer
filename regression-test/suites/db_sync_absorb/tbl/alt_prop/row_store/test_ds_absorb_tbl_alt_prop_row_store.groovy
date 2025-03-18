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
suite("test_ds_absorb_tbl_alt_prop_row_store") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    if (!helper.is_version_supported([30004, 20199, 20099])) {
        // disable in 2.1/2.0
        def version = helper.upstream_version()
        logger.info("skip this suite because version is not supported, upstream version ${version}")
        return
    }

    def tableName = "tbl_" + helper.randomSuffix()
    def test_num = 0
    def insert_num = 10

    def exist = { res -> Boolean
        return res.size() != 0
    }

    def notExist = { res -> Boolean
        return res.size() == 0
    }

    def has_count = { count ->
        return { res -> Boolean
            res.size() == count
        }
    }

    def checkShowResult = { target_res, property -> Boolean
        if(!target_res[0][1].contains(property)){
            logger.info("don't contains {}", property)
            return false
        }
        return true 
    }

    def existRowStore = { res -> Boolean
        if(!checkShowResult(res, "\"row_store_columns\" = \"test,id\"")) {
            return false
        }
        if(!checkShowResult(res, "\"row_store_page_size\" = \"16384\"")) {
            return false
        }
        return true
    }

    def notExistRowStore = { res -> Boolean
        if(!checkShowResult(res, "\"row_store_columns\" = \"test,id\"")) {
            return true;
        }
        if(!checkShowResult(res, "\"row_store_page_size\" = \"16384\"")) {
            return true;
        }
        return false
    }

    sql """
        CREATE TABLE if NOT EXISTS ${tableName}
        (
            `test` INT,
            `id` INT
        )
        ENGINE=OLAP
        DUPLICATE KEY(`test`, `id`)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
    """

    helper.enableDbBinlog()
    helper.ccrJobDelete()
    helper.ccrJobCreate()

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 180))
    assertTrue(helper.checkShowTimesOf(""" SHOW TABLES LIKE "${tableName}" """, exist, 60, "sql"))
    assertTrue(helper.checkShowTimesOf(""" SHOW TABLES LIKE "${tableName}" """, exist, 60, "target"))
    assertTrue(helper.checkShowTimesOf("SHOW CREATE TABLE ${tableName}", notExistRowStore, 60, "sql"))
    assertTrue(helper.checkShowTimesOf("SHOW CREATE TABLE ${tableName}", notExistRowStore, 60, "target"))

    // 0. Insert N data
    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${tableName} VALUES (${test_num}, ${index})
            """
    }
    assertTrue(helper.checkShowTimesOf(""" select * from ${tableName} """, { r -> r.size() == insert_num}, 60, "sql"))
    assertTrue(helper.checkShowTimesOf(""" select * from ${tableName} """, { r -> r.size() == insert_num}, 60, "target"))
    // 1. Pause ccr job
    helper.ccrJobPause()

    // 2. Insert N data
    for (int index = insert_num; index < insert_num * 2; index++) {
        sql """
            INSERT INTO ${tableName} VALUES (${test_num}, ${index})
            """
    }

    // 3. Do operation & wait it finishes upstream

    def state = sql """ SHOW ALTER TABLE COLUMN FROM ${context.dbName} WHERE TableName = "${tableName}" AND State = "FINISHED" """
    sql """
        ALTER TABLE ${tableName} SET ("store_row_column" = "true")
        """
    assertTrue(helper.checkShowTimesOf("""
                            SHOW ALTER TABLE COLUMN
                            FROM ${context.dbName}
                            WHERE TableName = "${tableName}" AND State = "FINISHED"
                            """,
                            has_count(state.size() + 1), 30))
    sql """
        ALTER TABLE ${tableName} SET ("row_store_columns" = "test,id")
        """
    assertTrue(helper.checkShowTimesOf("""
                            SHOW ALTER TABLE COLUMN
                            FROM ${context.dbName}
                            WHERE TableName = "${tableName}" AND State = "FINISHED"
                            """,
                            has_count(state.size() + 2), 30))
    // 4. Insert N data
    for (int index = insert_num * 2; index < insert_num * 3; index++) {
        sql """
            INSERT INTO ${tableName} VALUES (${test_num}, ${index})
            """
    }

    assertTrue(helper.checkShowTimesOf(""" select * from ${tableName} """, { r -> r.size() == insert_num * 3}, 60, "sql"))
    assertTrue(helper.checkShowTimesOf("SHOW CREATE TABLE ${tableName}", existRowStore, 60, "sql"))
    assertTrue(helper.checkShowTimesOf("SHOW CREATE TABLE ${tableName}", notExistRowStore, 60, "target"))
    // 5. Force trigger fullsnapshot
    helper.force_fullsync()

    // 6. Resume ccr job
    helper.ccrJobResume()
  
    // 7. Verify data and operation are synced downstream
    assertTrue(helper.checkShowTimesOf(""" select * from ${tableName} """, { r -> r.size() == insert_num * 3}, 60, "target"))
    assertTrue(helper.checkShowTimesOf("SHOW CREATE TABLE ${tableName}", existRowStore, 60, "sql"))
    assertTrue(helper.checkShowTimesOf("SHOW CREATE TABLE ${tableName}", existRowStore, 60, "target"))
}