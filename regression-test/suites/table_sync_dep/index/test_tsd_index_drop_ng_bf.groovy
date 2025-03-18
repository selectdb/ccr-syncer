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

suite("test_tsd_index_drop_ng_bf") {
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
            "binlog.enable" = "true"
        )
    """

    // Create the NGRAM_BF index that we'll drop later
    sql """
        ALTER TABLE ${tableName}
        ADD INDEX idx_ngbf_value (value) USING NGRAM_BF 
        PROPERTIES ("gram_size"="3", "bf_size"="1024")
    """

    helper.enableDbBinlog()
    helper.ccrJobDelete(tableName)
    helper.ccrJobCreate(tableName)

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 30))
    assertTrue(helper.checkShowTimesOf(""" SHOW TABLES LIKE "${tableName}" """, exist, 60, "target"))
    
    // Define checks for NGRAM_BF presence and absence
    def hasNGBFIndex = { inputRes -> Boolean
        for (List<Object> row : inputRes) {
            if ((row[1] as String).contains('NGRAM_BF')) {
                return true
            }
        }
        return false
    }
    
    def noNGBFIndex = { inputRes -> Boolean
        for (List<Object> row : inputRes) {
            if ((row[1] as String).contains('NGRAM_BF')) {
                return false
            }
        }
        return true
    }
    
    // Verify NGRAM_BF exists in both source and target
    assertTrue(helper.checkShowTimesOf("""
        SHOW CREATE TABLE ${context.dbName}.${tableName}
        """, 
        hasNGBFIndex, 30, "sql"))

    assertTrue(helper.checkShowTimesOf("""
        SHOW CREATE TABLE TEST_${context.dbName}.${tableName}
        """, 
        hasNGBFIndex, 30, "target"))
    
    // 1. Pause ccr job
    helper.ccrJobPause(tableName)

    // 2. Insert initial data
    def values = [];
    for (int index = 0; index < insert_num; index++) {
        values.add("(0, ${index}, 'user_${index}', 'text_data_${index}')")
    }
    sql """
        INSERT INTO ${tableName} VALUES ${values.join(",")}
    """
    sql "sync"
    assertTrue(helper.checkShowTimesOf(""" select * from ${tableName} """, has_count(insert_num), 60, "sql"))

    // Verify search with NGRAM_BF works
    assertTrue(helper.checkShowTimesOf(
        """ select * from ${tableName} where value like '%data_1%' """, 
        has_count(1), 30, "sql"))

    // 3. Do operation - drop NGRAM_BF index
    sql """
        ALTER TABLE ${tableName}
        DROP INDEX idx_ngbf_value
    """
    sql "sync"

    // Verify NGRAM_BF index was removed upstream
    assertTrue(helper.checkShowTimesOf("""
        SHOW CREATE TABLE ${context.dbName}.${tableName}
        """, 
        noNGBFIndex, 30, "sql"))

    // 4. Insert more data after dropping NGRAM_BF index
    sql """
        INSERT INTO ${tableName} VALUES
        (1, 100, "post_drop_user_1", "post_drop_text_1"),
        (1, 101, "post_drop_user_2", "post_drop_text_2"),
        (1, 102, "post_drop_user_3", "unique search phrase")
    """
    sql "sync"

    // Verify query still works without NGRAM_BF index
    assertTrue(helper.checkShowTimesOf(
        """ select * from ${tableName} where value like '%unique%' """, 
        has_count(1), 30, "sql"))

    // 5. Resume ccr job
    helper.ccrJobResume(tableName)

    // 6. Verify all operations are synced downstream
    // Check all data is synced
    assertTrue(helper.checkShowTimesOf(
        """ select * from ${tableName} """,
        has_count(insert_num + 3), 60, "target"))
    
    // Check NGRAM_BF index is removed downstream
    assertTrue(helper.checkShowTimesOf("""
        SHOW CREATE TABLE TEST_${context.dbName}.${tableName}
        """,
        noNGBFIndex, 30, "target"))

    // Verify query works downstream
    assertTrue(helper.checkShowTimesOf(
        """ select * from ${tableName} where value like '%unique%' """,
        has_count(1), 30, "target"))
}