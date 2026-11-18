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

suite("test_ds_tbl_create_rollup") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def dbName = context.dbName
    def tableNameOrigin = "tbl_" + helper.randomSuffix()
    def tableNameLike = "tbl_" + helper.randomSuffix()
    def indedxName = "rollupIndex" 

    def exist = { res -> Boolean
        return res.size() != 0
    }

    def has_count = { count ->
        return { res -> Boolean
            res.size() == count
        }
    }

    def existIndex = { res -> Boolean
        List<Map<String, Object>> map_res = res
        logger.info(map_res[0])
        return map_res[0].IndexName.contains("${indedxName}")
    }

    sql "DROP TABLE IF EXISTS ${dbName}.${tableNameOrigin}"
    target_sql "DROP TABLE IF EXISTS TEST_${dbName}.${tableNameOrigin}"
    sql "DROP TABLE IF EXISTS ${dbName}.${tableNameLike}"
    target_sql "DROP TABLE IF EXISTS TEST_${dbName}.${tableNameLike}"

    helper.enableDbBinlog()
    helper.ccrJobDelete()
    helper.ccrJobCreate()

    logger.info("=== Test 1: create table full sync ===")

    sql """
        CREATE TABLE if NOT EXISTS ${tableNameOrigin}
        (
            `id` LARGEINT NOT NULL,
            `test` INT NOT NULL,
            `value` INT
        )
        ENGINE=OLAP
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true",
            "storage_medium" = "ssd"
        )
    """

    assertTrue(helper.checkRestoreFinishTimesOf("${tableNameOrigin}", 30))

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableNameOrigin}\"", exist, 60, "target"))

    logger.info("=== Test 2: add rollup index ===")

    sql """
        ALTER TABLE ${tableNameOrigin} add rollup ${indedxName}(test)
    """

    assertTrue(helper.checkShowTimesOf("""
                                SHOW ALTER TABLE ROLLUP
                                FROM ${context.dbName}
                                WHERE TableName = "${tableNameOrigin}" AND State = "FINISHED"
                                """,
                                has_count(1), 30))

    logger.info("=== Test 3: create table like origin with rollup ===")

    sql """
        CREATE TABLE if NOT EXISTS ${tableNameLike} LIKE ${tableNameOrigin} WITH ROLLUP
    """

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableNameLike}\"", exist, 60, "target"))

    assertTrue(helper.check_table_describe_times(tableNameLike, 30))
}