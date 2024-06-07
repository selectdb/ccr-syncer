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

suite("test_bloomfilter_index") {

    def tableName = "tbl_bloomfilter_index_" + UUID.randomUUID().toString().replace("-", "")
    def syncerAddress = "127.0.0.1:9190"
    def test_num = 0
    def insert_num = 5
    def sync_gap_time = 5000
    String response

    def checkShowTimesOf = { sqlString, myClosure, times, func = "sql" -> Boolean
        Boolean ret = false
        List<List<Object>> res
        while (times > 0) {
            try {
                if (func == "sql") {
                    res = sql "${sqlString}"
                } else {
                    res = target_sql "${sqlString}"
                }
                logger.info("res: ${res}")
                if (myClosure.call(res)) {
                    ret = true
                }
            } catch (Exception e) {}

            if (ret) {
                break
            } else if (--times > 0) {
                sleep(sync_gap_time)
            }
        }

        return ret
    }

    def checkRestoreFinishTimesOf = { checkTable, times -> Boolean
        Boolean ret = false
        while (times > 0) {
            def sqlInfo = target_sql "SHOW RESTORE FROM TEST_${context.dbName}"
            for (List<Object> row : sqlInfo) {
                if ((row[10] as String).contains(checkTable)) {
                    ret = row[4] == "FINISHED"
                }
            }

            if (ret) {
                break
            } else if (--times > 0) {
                sleep(sync_gap_time)
            }
        }

        return ret
    }

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
            "bloom_filter_columns" = "id"
        )
    """
    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${tableName} VALUES (${test_num}, ${index}, "test_${index}", "${index}_test")
            """
    }
    sql """ALTER TABLE ${tableName} set ("binlog.enable" = "true")"""
    sql "sync"

    logger.info("=== Test 1: full update bloom filter ===")
    httpTest {
        uri "/create_ccr"
        endpoint syncerAddress
        def bodyJson = get_ccr_body "${tableName}"
        body "${bodyJson}"
        op "post"
        result response
    }

    assertTrue(checkRestoreFinishTimesOf("${tableName}", 30))
    def checkNgramBf = { inputRes -> Boolean
        for (List<Object> row : inputRes) {
            if (row[2] == "idx_ngrambf" && row[10] == "NGRAM_BF") {
                return true
            }
        }
        return false
    }
    assertTrue(checkShowTimesOf("""
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
    assertTrue(checkShowTimesOf("""
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
    assertTrue(checkShowTimesOf("""
                                SHOW INDEXES FROM ${context.dbName}.${tableName}
                                """, 
                                checkNgramBf1, 30, "sql"))
    assertTrue(checkShowTimesOf("""
                                SHOW INDEXES FROM TEST_${context.dbName}.${tableName}
                                """, 
                                checkNgramBf1, 30, "target"))
}
