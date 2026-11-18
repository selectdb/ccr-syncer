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

suite("test_tsa_idx_bf_fpp") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def tableName = "tbl_" + helper.randomSuffix()
    def aliasTableName = "alias_tbl_" + helper.randomSuffix()
    def test_num = 0
    def insert_num = 5

    helper.enableDbBinlog()

    helper.set_alias(aliasTableName)

    sql "DROP TABLE IF EXISTS ${tableName}"
    target_sql "DROP TABLE IF EXISTS ${aliasTableName}"

    sql """
        CREATE TABLE if NOT EXISTS ${tableName}
        (
            `test` INT,
            `id` INT,
            `username` varchar(32) NULL DEFAULT "",
            `only4test` varchar(32) NULL DEFAULT ""
        )
        ENGINE=OLAP
        DUPLICATE KEY(`test`, `id`)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "bloom_filter_columns" = "username",
            "binlog.enable" = "true"
        )
    """
    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${tableName} VALUES (${test_num}, ${index}, "test_${index}", "${index}_test")
            """
    }
    sql "sync"

    logger.info("=== Test 1: full update bloom filter ===")
    helper.ccrJobDelete(tableName)
    helper.ccrJobCreate(tableName)

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 30))
    def checkBloomFilter = { inputRes -> Boolean
        for (List<Object> row : inputRes) {
            if ((row[1] as String).contains("\"bloom_filter_columns\" = \"username\"")) {
                return true
            }
        }
        return false
    }
    assertTrue(helper.checkShowTimesOf("""
                                SHOW CREATE TABLE TEST_${context.dbName}.${aliasTableName}
                                """,
                                checkBloomFilter, 30, "target"))

    logger.info("=== Test 2: update bloom filter fpp property ===")
    sql """
        ALTER TABLE ${tableName}
        SET ("bloom_filter_fpp" = "0.01")
        """
    def checkBloomFilterFPP = { inputRes -> Boolean
        for (List<Object> row : inputRes) {
            if ((row[1] as String).contains("\"bloom_filter_fpp\" = \"0.01\"")) {
                return true
            }
        }
        return false
    }
    assertTrue(helper.checkShowTimesOf("""
                                SHOW CREATE TABLE TEST_${context.dbName}.${aliasTableName}
                                """,
                                checkBloomFilter, 30, "target"))
}

