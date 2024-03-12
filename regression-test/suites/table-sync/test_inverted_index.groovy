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

suite("test_inverted_index") {

    def tableName = "tbl_inverted_index_dup_" + UUID.randomUUID().toString().replace("-", "")
    def syncerAddress = "127.0.0.1:9190"
    def test_num = 0
    def insert_num = 5
    def sync_gap_time = 5000
    String response

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

    def checkSyncFinishTimesOf = { count, times -> Boolean
        Boolean ret = false
        while (times > 0) {
            def sqlInfo = target_sql "SELECT COUNT() FROM TEST_${context.dbName}.${tableName}"
            if ((sqlInfo[0][0] as Integer) == count) {
                ret = true
                break
            } else if (--times > 0) {
                sleep(sync_gap_time)
            }
        }

        return ret
    }

    def insert_data = { -> 
        sql """ INSERT INTO ${tableName} VALUES (1, "andy", "andy love apple", 100); """
        sql """ INSERT INTO ${tableName} VALUES (1, "bason", "bason hate pear", 100); """
        sql """ INSERT INTO ${tableName} VALUES (2, "andy", "andy love apple", 100); """
        sql """ INSERT INTO ${tableName} VALUES (2, "bason", "bason hate pear", 98); """
        sql """ INSERT INTO ${tableName} VALUES (3, "andy", "andy love apple", 100); """
        sql """ INSERT INTO ${tableName} VALUES (3, "bason", "bason hate pear", 99); """
        sql """ INSERT INTO ${tableName} VALUES (4, "bason", "bason hate pear", 99); """
        sql """ INSERT INTO ${tableName} VALUES (4, "andy", "andy love apple", 100); """
    }

    def insert_data2 = { -> 
        sql """ INSERT INTO ${tableName} VALUES (5, "bason", "bason hate pear", 99); """
        sql """ INSERT INTO ${tableName} VALUES (5, "andy", "andy love apple", 100); """
        sql """ INSERT INTO ${tableName} VALUES (6, "bason", "bason hate pear", 99); """
        sql """ INSERT INTO ${tableName} VALUES (6, "andy", "andy love apple", 98); """
    }

    def run_sql = { String db -> 
        qt_sql """ select * from ${db}.${tableName} order by id, name, hobbies, score """
        qt_sql """ select * from ${db}.${tableName} where name match "andy" order by id, name, hobbies, score """
        qt_sql """ select * from ${db}.${tableName} where hobbies match "pear" order by id, name, hobbies, score """
        qt_sql """ select * from ${db}.${tableName} where score < 99 order by id, name, hobbies, score """
    }

    def run_test = { ->
        insert_data.call()
        
        sql """ALTER TABLE ${tableName} set ("binlog.enable" = "true")"""
        sql "sync"

        httpTest {
            uri "/create_ccr"
            endpoint syncerAddress
            def bodyJson = get_ccr_body "${tableName}"
            body "${bodyJson}"
            op "post"
            result response
        }

        assertTrue(checkRestoreFinishTimesOf("${tableName}", 30))

        def show_result = target_sql "SHOW INDEXES FROM TEST_${context.dbName}.${tableName}"
        logger.info("show index from TEST_${context.dbName}.${tableName} result: " + show_result)
        assertEquals(show_result.size(), 3)
        assertEquals(show_result[0][2], "index_name")
        assertEquals(show_result[1][2], "index_hobbies")
        assertEquals(show_result[2][2], "index_score")

        run_sql.call("${context.dbName}")
        run_sql.call("TEST_${context.dbName}")

        insert_data2.call()
        sql "sync"

        if (("${tableName}" as String).contains("tbl_inverted_index_dup")) {
            assertTrue(checkSyncFinishTimesOf(12, 30))
        } else {
            assertTrue(checkSyncFinishTimesOf(6, 30))
        }

        run_sql.call("${context.dbName}")
        run_sql.call("TEST_${context.dbName}")
    }

    /**
    * test for duplicated key table
    */
    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """
        CREATE TABLE ${tableName} (
            `id` int(11) NULL,
            `name` varchar(255) NULL,
            `hobbies` text NULL,
            `score` int(11) NULL,
            index index_name (name) using inverted,
            index index_hobbies (hobbies) using inverted properties("parser"="english"),
            index index_score (score) using inverted
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ( "replication_num" = "1");
    """

    run_test.call()

    /**
    * test for unique key table with mow
    */
    tableName = "tbl_inverted_index_unique_mow_" + UUID.randomUUID().toString().replace("-", "")

    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """
        CREATE TABLE ${tableName} (
            `id` int(11) NULL,
            `name` varchar(255) NULL,
            `hobbies` text NULL,
            `score` int(11) NULL,
            index index_name (name) using inverted,
            index index_hobbies (hobbies) using inverted properties("parser"="english"),
            index index_score (score) using inverted
        ) ENGINE=OLAP
        UNIQUE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ( 
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    run_test.call()

    /**
    * test for unique key table with mor
    */
    tableName = "tbl_inverted_index_unique_mor_" + UUID.randomUUID().toString().replace("-", "")

    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """
        CREATE TABLE ${tableName} (
            `id` int(11) NULL,
            `name` varchar(255) NULL,
            `hobbies` text NULL,
            `score` int(11) NULL,
            index index_name (name) using inverted,
            index index_hobbies (hobbies) using inverted properties("parser"="english"),
            index index_score (score) using inverted
        ) ENGINE=OLAP
        UNIQUE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ( 
            "replication_num" = "1"
        );
    """

    run_test.call()
}
