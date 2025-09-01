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

suite("test_ts_alt_prop_seq") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def dbName = context.dbName
    def tableName = "tbl_" + helper.randomSuffix()

    def exist = { res -> Boolean
        return res.size() != 0
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

    def has_count = { count ->
        return { res -> Boolean
            res.size() == count
        }
    }

    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
    target_sql "DROP TABLE IF EXISTS TEST_${dbName}.${tableName}"

    helper.enableDbBinlog()

    sql """
        CREATE TABLE if NOT EXISTS ${tableName}
        (
            `test` INT,
            `id` INT
        )
        ENGINE=OLAP
        UNIQUE KEY(test)
        DISTRIBUTED BY HASH (test) BUCKETS 32
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
    """

    helper.ccrJobDelete(tableName)
    helper.ccrJobCreate(tableName)

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 30))

    logger.info("=== Test 1: check property not exist ===")

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}\"", exist, 60, "sql"))

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}\"", exist, 60, "target"))

    assertTrue(helper.checkShowTimesOf("SHOW CREATE TABLE ${tableName}", notExistST, 60, "sql"))

    assertTrue(helper.checkShowTimesOf("SHOW CREATE TABLE ${tableName}", notExistST, 60, "target"))

    logger.info("=== Test 2: alter table set property sequence type ===")

    def state = sql """ SHOW ALTER TABLE COLUMN FROM ${context.dbName} WHERE TableName = "${tableName}" AND State = "FINISHED" """

    sql """
        ALTER TABLE ${tableName} ENABLE FEATURE "SEQUENCE_LOAD" WITH PROPERTIES ("function_column.sequence_type" = "datetimev2")
        """

    assertTrue(helper.checkShowTimesOf("""
                                SHOW ALTER TABLE COLUMN
                                FROM ${context.dbName}
                                WHERE TableName = "${tableName}" AND State = "FINISHED"
                                """,
                                has_count(state.size() + 1), 30))

    logger.info("=== Test 3: check property exist ===")

    assertTrue(helper.checkShowTimesOf("SHOW CREATE TABLE ${tableName}", existST, 60, "sql"))

    assertTrue(helper.checkShowTimesOf("SHOW CREATE TABLE ${tableName}", existST, 60, "target"))
}