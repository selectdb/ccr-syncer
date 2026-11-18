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

suite("test_tbl_idx_ngbf_add_drop") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def tableName = "tbl_" + helper.randomSuffix()
    def test_num = 0
    def insert_num = 5

    sql """
        CREATE TABLE if NOT EXISTS ${tableName}
        (
            `test` INT,
            `id` INT,
            `username` varchar(32) NULL DEFAULT "",
            `only4test` varchar(32) NULL DEFAULT "",
            INDEX idx_ngrambf (`username`) USING NGRAM_BF PROPERTIES("gram_size"="3", "bf_size"="256")
        )
        ENGINE=OLAP
        DUPLICATE KEY(`test`, `id`)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "bloom_filter_columns" = "id",
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
    helper.ccrJobCreate(tableName)

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 30))
    def checkNgramBf = { inputRes -> Boolean
        for (List<Object> row : inputRes) {
            if (row[2] == "idx_ngrambf" && row[10] == "NGRAM_BF") {
                return true
            }
        }
        return false
    }
    assertTrue(helper.checkShowTimesOf("""
                                SHOW INDEXES FROM TEST_${context.dbName}.${tableName}
                                """,
                                checkNgramBf, 30, "target"))
    def checkBloomFilter = { inputRes -> Boolean
        for (List<Object> row : inputRes) {
            if ((row[1] as String).contains("\"bloom_filter_columns\" = \"id\"")) {
                return true
            }
        }
        return false
    }
    assertTrue(helper.checkShowTimesOf("""
                                SHOW CREATE TABLE TEST_${context.dbName}.${tableName}
                                """,
                                checkBloomFilter, 30, "target"))

    logger.info("=== Test 2: incremental update Ngram bloom filter ===")
    sql """
        ALTER TABLE ${tableName}
        ADD INDEX idx_only4test(`only4test`) USING NGRAM_BF PROPERTIES("gram_size"="3", "bf_size"="256")
        """
    def checkNgramBf1 = { inputRes -> Boolean
        for (List<Object> row : inputRes) {
            if (row[2] == "idx_only4test" && row[10] == "NGRAM_BF") {
                return true
            }
        }
        return false
    }
    assertTrue(helper.checkShowTimesOf("""
                                SHOW INDEXES FROM ${context.dbName}.${tableName}
                                """,
                                checkNgramBf1, 30, "sql"))
    assertTrue(helper.checkShowTimesOf("""
                                SHOW INDEXES FROM TEST_${context.dbName}.${tableName}
                                """,
                                checkNgramBf1, 30, "target"))

    logger.info("=== Test 3: drop bloom filter ===")
    sql """
        ALTER TABLE ${tableName}
        DROP INDEX idx_ngrambf
        """
    def checkNgramBf1NotExists = { inputRes -> Boolean
        for (List<Object> row : inputRes) {
            if (row[2] == "idx_ngrambf" && row[10] == "NGRAM_BF") {
                return false
            }
        }
        return true
    }
    assertTrue(helper.checkShowTimesOf("""
                                SHOW INDEXES FROM ${context.dbName}.${tableName}
                                """,
                                checkNgramBf1NotExists, 30, "sql"))
    assertTrue(helper.checkShowTimesOf("""
                                SHOW INDEXES FROM TEST_${context.dbName}.${tableName}
                                """,
                                checkNgramBf1NotExists, 30, "target"))

    sql "INSERT INTO ${tableName} VALUES (1, 1, '1', '1')"
    assertTrue(helper.checkSelectTimesOf(
        """ SELECT * FROM ${tableName} """, insert_num + 1, 30))
}
