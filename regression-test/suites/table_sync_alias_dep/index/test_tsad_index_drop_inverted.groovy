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

suite("test_tsad_index_drop_inverted") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def tableName = "tbl_" + helper.randomSuffix()
    def aliasTableName = "alias_tbl_" + helper.randomSuffix()
    def insert_num = 5
    helper.set_alias(aliasTableName)

    def exist = { res -> Boolean
        return res.size() != 0
    }

    def notExist = { res -> Boolean
        return res.size() == 0
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
            `value` String,
            `value1` String
        )
        ENGINE=OLAP
        UNIQUE KEY(`test`, `id`)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
    """

    // Create the inverted index we will drop later
    sql """
        ALTER TABLE ${tableName}
        ADD INDEX idx_inverted(value) USING INVERTED
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
        values.add("(0, ${index}, 'value_${index}', 'data_${index}')")
    }
    sql """
        INSERT INTO ${tableName} VALUES ${values.join(",")}
    """

    // Build the index after inserting data
    sql """
        BUILD INDEX idx_inverted ON ${tableName}
    """

    assertTrue(helper.checkShowTimesOf("""
        SHOW BUILD INDEX FROM ${context.dbName}
        WHERE TableName = "${tableName}" AND State = "FINISHED"
    """, has_count(1), 30))

    // Verify index exists and works
    def show_indexes_result = sql_return_maparray "show indexes from ${tableName}"
    logger.info("show indexes upstream before drop: ${show_indexes_result}")
    assertTrue(show_indexes_result.any {
        it['Key_name'] == 'idx_inverted' && it['Index_type'] == 'INVERTED' })

    // Check query works with index
    assertTrue(helper.checkShowTimesOf(
        """ select * from ${tableName} where value = 'value_1' """, 
        has_count(1), 30, "sql"))

    // 3. Do operation - drop the inverted index
    sql """
        ALTER TABLE ${tableName}
        DROP INDEX idx_inverted
    """

    // Verify index is gone upstream
    show_indexes_result = sql_return_maparray "show indexes from ${tableName}"
    logger.info("show indexes upstream after drop: ${show_indexes_result}")
    assertFalse(show_indexes_result.any { 
        it['Key_name'] == 'idx_inverted' })

    // 4. Insert more data after dropping the index
    sql """
        INSERT INTO ${tableName} VALUES
        (1, 100, "post_drop_term_1", "extra_data_1"),
        (1, 101, "post_drop_term_2", "extra_data_2"),
        (1, 102, "unique_post_drop", "extra_data_3")
    """

    // Verify query still works without index
    assertTrue(helper.checkShowTimesOf(
        """ select * from ${tableName} where value = 'unique_post_drop' """, 
        has_count(1), 30, "sql"))

    // 5. Resume ccr job
    helper.ccrJobResume(tableName)

    // 6. Verify all operations are synced downstream
    assertTrue(helper.checkShowTimesOf(
        """ select * from ${aliasTableName} """,
        has_count(insert_num + 3), 60, "target"))

    // Check index is dropped in downstream
    show_indexes_result = target_sql_return_maparray "show indexes from ${aliasTableName}"
    logger.info("show indexes downstream after drop: ${show_indexes_result}")
    assertFalse(show_indexes_result.any { 
        it['Key_name'] == 'idx_inverted' })

    // Verify query works downstream
    assertTrue(helper.checkShowTimesOf(
        """ select * from ${aliasTableName} where value = 'unique_post_drop' """,
        has_count(1), 30, "target"))
}