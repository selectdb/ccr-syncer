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

suite("test_tsd_index_drop_bf") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def tableName = "tbl_" + helper.randomSuffix()
    def insert_num = 5

    def exist = { res -> Boolean
        return res.size() != 0
    }

    def has_count = { count ->
        return { res -> Boolean
            res.size() == count
        }
    }

    sql "DROP TABLE IF EXISTS ${tableName}"
    target_sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE if NOT EXISTS ${tableName}
        (
            `test` INT,
            `id` INT,
            `username` String,
            `value` String
        )
        ENGINE=OLAP
        UNIQUE KEY(`test`, `id`)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true",
            "bloom_filter_columns" = "username"
        )
    """

    helper.enableDbBinlog()
    helper.ccrJobDelete(tableName)
    helper.ccrJobCreate(tableName)

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 30))
    assertTrue(helper.checkShowTimesOf(""" SHOW TABLES LIKE "${tableName}" """, exist, 60, "target"))
    
    // Define checks for bloom filter presence and absence
    def hasBloomFilter = { inputRes -> Boolean
        for (List<Object> row : inputRes) {
            if ((row[1] as String).contains('\"bloom_filter_columns\" = \"username\"')) {
                return true
            }
        }
        return false
    }
    
    def noBloomFilter = { inputRes -> Boolean
        for (List<Object> row : inputRes) {
            if ((row[1] as String).contains('\"bloom_filter_columns\"')) {
                return false
            }
        }
        return true
    }
    
    // Verify bloom filter exists initially in both source and target
    assertTrue(helper.checkShowTimesOf("""
        SHOW CREATE TABLE ${context.dbName}.${tableName}
        """, 
        hasBloomFilter, 30, "sql"))
        
    assertTrue(helper.checkShowTimesOf("""
        SHOW CREATE TABLE TEST_${context.dbName}.${tableName}
        """, 
        hasBloomFilter, 30, "target"))
    
    // 1. Pause ccr job
    helper.ccrJobPause(tableName)

    // 2. Insert initial data
    def values = [];
    for (int index = 0; index < insert_num; index++) {
        values.add("(0, ${index}, 'user_${index}', 'data_${index}')")
    }
    sql """
        INSERT INTO ${tableName} VALUES ${values.join(",")}
    """
    sql "sync"
    assertTrue(helper.checkShowTimesOf(""" select * from ${tableName} """, has_count(insert_num), 60, "sql"))

    // 3. Do operation - drop bloom filter
    sql """
        ALTER TABLE ${tableName}
        SET ("bloom_filter_columns" = "")
    """

    // Insert more data in parallel, to create the shadow index.
    def num_values = insert_num
    insert_num *= 2
    for (int index = num_values; index < insert_num; index++) {
        sql """
            INSERT INTO ${tableName} VALUES (1, ${index}, 'user_${index}', 'data_${index}')
        """
    }
    sql "sync"

    // Verify bloom filter was removed upstream
    assertTrue(helper.checkShowTimesOf("""
        SHOW CREATE TABLE ${context.dbName}.${tableName}
        """,
        noBloomFilter, 30, "sql"))

    // 4. Insert more data after dropping bloom filter
    sql """
        INSERT INTO ${tableName} VALUES
        (2, 100, "post_drop_user_1", "extra_data_1"),
        (2, 101, "post_drop_user_2", "extra_data_2"),
        (2, 102, "post_drop_unique", "extra_data_3")
    """
    sql "sync"
    insert_num += 3

    // Verify query still works without bloom filter
    assertTrue(helper.checkShowTimesOf(
        """ select * from ${tableName} where username = 'post_drop_unique' """, 
        has_count(1), 30, "sql"))

    // 5. Resume ccr job
    helper.ccrJobResume(tableName)

    // 6. Verify all operations are synced downstream
    // Check all data is synced
    assertTrue(helper.checkShowTimesOf(
        """ select * from ${tableName} """,
        has_count(insert_num), 60, "target"))
    
    // Check bloom filter is removed downstream
    assertTrue(helper.checkShowTimesOf("""
        SHOW CREATE TABLE TEST_${context.dbName}.${tableName}
        """,
        noBloomFilter, 30, "target"))

    // Verify query works downstream
    assertTrue(helper.checkShowTimesOf(
        """ select * from ${tableName} where username = 'post_drop_unique' """,
        has_count(1), 30, "target"))
        
    // Insert some more data to verify downstream is still functional
    sql """
        INSERT INTO ${tableName} VALUES
        (3, 200, "final_user_1", "final_data_1"),
        (3, 201, "final_user_2", "final_data_2")
    """
    assertTrue(helper.checkShowTimesOf(
        """ select * from ${tableName} """,
        has_count(insert_num + 2), 60, "target"))
}