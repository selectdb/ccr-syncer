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

suite("test_tsa_alt_prop_bloom_filter") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def dbName = context.dbName
    def tableName = "tbl_" + helper.randomSuffix()
    def aliasTableName = "tbl_alias_" + helper.randomSuffix()

    def exist = { res -> Boolean
        return res.size() != 0
    }
    
    helper.set_alias(aliasTableName)

    def checkShowResult = { target_res, property -> Boolean
        if(!target_res[0][1].contains(property)){
            logger.info("don't contains {}", property)
            return false
        }
            return true 
    }

    def existBF = { res -> Boolean
        return checkShowResult(res, "\"bloom_filter_columns\" = \"test, id\"")
    }

    def notExistBF = { res -> Boolean
        return !checkShowResult(res, "\"bloom_filter_columns\" = \"test, id\"")
    }

    def has_count = { count ->
        return { res -> Boolean
            res.size() == count
        }
    }

    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
    target_sql "DROP TABLE IF EXISTS TEST_${dbName}.${aliasTableName}"

    helper.enableDbBinlog()

    sql """
        CREATE TABLE if NOT EXISTS ${tableName}
        (
            `test` INT,
            `id` INT
        )
        ENGINE=OLAP
        AGGREGATE KEY(`test`, `id`)
        PARTITION BY RANGE(`id`)
        (
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
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

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${aliasTableName}\"", exist, 60, "target"))

    assertTrue(helper.checkShowTimesOf("SHOW CREATE TABLE ${tableName}", notExistBF, 60, "sql"))

    assertTrue(helper.checkShowTimesOf("SHOW CREATE TABLE ${aliasTableName}", notExistBF, 60, "target"))

    logger.info("=== Test 2: alter table set property bloom filter columns ===")

    def state = sql """ SHOW ALTER TABLE COLUMN FROM ${context.dbName} WHERE TableName = "${tableName}" AND State = "FINISHED" """

    sql """
        ALTER TABLE ${tableName} SET ("bloom_filter_columns" = "test, id");
        """

    assertTrue(helper.checkShowTimesOf("""
                                SHOW ALTER TABLE COLUMN
                                FROM ${context.dbName}
                                WHERE TableName = "${tableName}" AND State = "FINISHED"
                                """,
                                has_count(state.size() + 1), 30))

    logger.info("=== Test 3: check property exist ===")

    assertTrue(helper.checkShowTimesOf("SHOW CREATE TABLE ${tableName}", existBF, 60, "sql"))

    assertTrue(helper.checkShowTimesOf("SHOW CREATE TABLE ${aliasTableName}", existBF, 60, "target"))
}