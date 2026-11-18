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
suite("test_tsa_absorb_alt_prop_seq") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def dbName = context.dbName
    def tableName = "tbl_" + helper.randomSuffix()
    def aliasTableName = "alias_" + helper.randomSuffix()
    def test_num = 0
    def insert_num = 5
    helper.set_alias(aliasTableName)

    def exist = { res -> Boolean
        return res.size() != 0
    }

    def notExist = { res -> Boolean
        return res.size() == 0
    }
    
    def checkShowResult = { target_res, property -> Boolean
        if(!target_res[0][1].contains(property)){
            logger.info("don't contains {}", property)
            return false
        }
            return true 
    }

    def existST = { res -> Boolean
        return checkShowResult(res, "\"function_column.sequence_type\" = \"datetimev2(0)\"")
    }

    def notExistST = { res -> Boolean
        return !checkShowResult(res, "\"function_column.sequence_type\" = \"datetimev2(0)\"")
    }
    sql """
        CREATE TABLE ${tableName}
        (
            user_id bigint,
            date date,
            modify_date datetimev2,
            user_name VARCHAR(128)
        )
        UNIQUE KEY(user_id, date)
        DISTRIBUTED BY HASH (user_id) BUCKETS 32
        PROPERTIES(
            "replication_num" = "1",
            "binlog.enable" = "true"
        )
    """

    helper.enableDbBinlog()
    helper.ccrJobDelete(tableName)
    helper.ccrJobCreate(tableName)

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 180))
    assertTrue(helper.checkShowTimesOf(""" SHOW TABLES LIKE "${tableName}" """, exist, 60, "sql"))
    assertTrue(helper.checkShowTimesOf(""" SHOW TABLES LIKE "${aliasTableName}" """, exist, 60, "target"))
    assertTrue(helper.checkShowTimesOf(""" show create table ${tableName} """, notExistST, 60, "sql"))
    assertTrue(helper.checkShowTimesOf(""" show create table ${aliasTableName} """, notExistST, 60, "target"))

    // 0. Insert N data
    for (int index = 0; index < insert_num; index++) {
        def str = helper.randomSuffix()
        sql """
            INSERT INTO ${tableName} VALUES (${index}, now(), now(), ${str})
            """
    }
    assertTrue(helper.checkShowTimesOf(""" select * from ${tableName} """, { r -> r.size() == insert_num}, 60, "sql"))
    assertTrue(helper.checkShowTimesOf(""" select * from ${aliasTableName} """, { r -> r.size() == insert_num}, 60, "target"))
    // 1. Pause ccr job
    helper.ccrJobPause(tableName)

    // 2. Insert N data
    for (int index = insert_num; index < insert_num * 2; index++) {
        def str = helper.randomSuffix()
        sql """
            INSERT INTO ${tableName} VALUES (${index}, now(), now(), ${str})
            """
    }
    assertTrue(helper.checkShowTimesOf(""" select * from ${tableName} """, { r -> r.size() == insert_num * 2}, 60, "sql"))
    // 3. Do operation & wait it finishes upstream
    sql """
        ALTER TABLE ${tableName} ENABLE FEATURE "SEQUENCE_LOAD" WITH PROPERTIES ("function_column.sequence_type" = "datetimev2")
        """

    // 4. Insert N data
    for (int index = insert_num * 2; index < insert_num * 3; index++) {
        def str = helper.randomSuffix()
        sql """
            INSERT INTO ${tableName}  (user_id, date, modify_date, user_name, __DORIS_SEQUENCE_COL__)
            VALUES (${index}, now(), now(), ${str}, now())
            """
    }

    assertTrue(helper.checkShowTimesOf(""" select * from ${tableName} """, { r -> r.size() == insert_num * 3}, 60, "sql"))
    // 5. Force trigger fullsnapshot
    helper.force_fullsync(tableName)

    // 6. Resume ccr job
    helper.ccrJobResume(tableName)
  
    // 7. Verify data and operation are synced downstream
    assertTrue(helper.checkShowTimesOf(""" select * from ${aliasTableName} """, { r -> r.size() == insert_num * 3}, 60, "target"))
    assertTrue(helper.checkShowTimesOf(""" show create table ${tableName} """, existST, 60, "sql"))
    assertTrue(helper.checkShowTimesOf(""" show create table ${aliasTableName} """, existST, 60, "target"))
}