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

suite("test_tsad_index_add_bf") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def tableName = "tbl_" + helper.randomSuffix()
    def aliasTableName = "alias_tbl_" + helper.randomSuffix()
    def insert_num = 5
    helper.set_alias(aliasTableName)

    def exist = { res -> Boolean
        return res.size() != 0
    }

    def has_count = { count ->
        return { res -> Boolean
            res.size() == count
        }
    }

    sql "DROP TABLE IF EXISTS ${tableName}"
    target_sql "DROP TABLE IF EXISTS ${aliasTableName}"
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
            "binlog.enable" = "true"
        )
    """

    helper.enableDbBinlog()
    helper.ccrJobDelete(tableName)
    helper.ccrJobCreate(tableName)

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 30))
    assertTrue(helper.checkShowTimesOf(""" SHOW TABLES LIKE "${aliasTableName}" """, exist, 60, "target"))
    
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

    // 3. Do operation - add bloom filter index
    sql """
        ALTER TABLE ${tableName}
        SET ("bloom_filter_columns" = "username")
    """
    sql "sync"

    // Verify bloom filter was added upstream
    def checkBloomFilter = { inputRes -> Boolean
        for (List<Object> row : inputRes) {
            if ((row[1] as String).contains('\"bloom_filter_columns\" = \"username\"')) {
                return true
            }
        }
        return false
    }
    
    assertTrue(helper.checkShowTimesOf("""
        SHOW CREATE TABLE ${context.dbName}.${tableName}
        """, 
        checkBloomFilter, 30, "sql"))

    // 4. Insert more data after adding bloom filter
    sql """
        INSERT INTO ${tableName} VALUES
        (1, 100, "specific_user_1", "extra_data_1"),
        (1, 101, "specific_user_2", "extra_data_2"),
        (1, 102, "unique_username", "extra_data_3")
    """
    sql "sync"

    // Verify query with username works (potentially using bloom filter)
    assertTrue(helper.checkShowTimesOf(
        """ select * from ${tableName} where username = 'unique_username' """, 
        has_count(1), 30, "sql"))

    // 5. Resume ccr job
    helper.ccrJobResume(tableName)

    // 6. Verify all operations are synced downstream
    // Check all data is synced
    assertTrue(helper.checkShowTimesOf(
        """ select * from ${aliasTableName} """,
        has_count(insert_num + 3), 60, "target"))
    
    // Check bloom filter is synced
    assertTrue(helper.checkShowTimesOf("""
        SHOW CREATE TABLE TEST_${context.dbName}.${aliasTableName}
        """,
        checkBloomFilter, 30, "target"))

    // Verify query works downstream
    assertTrue(helper.checkShowTimesOf(
        """ select * from ${aliasTableName} where username = 'unique_username' """,
        has_count(1), 30, "target"))
}