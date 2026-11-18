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
suite('test_db_psi_drop_table') {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", 'helper.groovy'))

    if (!helper.has_feature('feature_schema_change_partial_sync')) {
        logger.info('this suite require feature_schema_change_partial_sync set to true')
        return
    }

    def tableName = 'tbl_' + helper.randomSuffix()
    def tableName1 = 'tbl_' + helper.randomSuffix()
    def test_num = 0
    def insert_num = 5

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

    helper.enableDbBinlog()
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql "DROP TABLE IF EXISTS ${tableName1}"
    target_sql "DROP TABLE IF EXISTS ${tableName}"
    target_sql "DROP TABLE IF EXISTS ${tableName1}"

    sql """
        CREATE TABLE if NOT EXISTS ${tableName}
        (
            `test` INT,
            `id` INT,
            `value` TEXT,
            INDEX `idx_value`(`value`) USING INVERTED PROPERTIES ("parser" = "english")
        )
        ENGINE=OLAP
        UNIQUE KEY(`test`, `id`)
        PARTITION BY RANGE(`id`)
        (
            PARTITION p1 VALUES LESS THAN ("1000"),
            PARTITION p2 VALUES LESS THAN ("2000"),
            PARTITION p3 VALUES LESS THAN ("3000")
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
    """
    sql "DROP TABLE IF EXISTS ${tableName1}"
    sql """
        CREATE TABLE if NOT EXISTS ${tableName1}
        (
            `test` INT,
            `id` INT,
            `value` TEXT,
            INDEX `idx_value`(`value`) USING INVERTED PROPERTIES ("parser" = "english")
        )
        ENGINE=OLAP
        UNIQUE KEY(`test`, `id`)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
    """

    def values = []
    for (int index = 0; index < insert_num; index++) {
        values.add("(${test_num}, ${index}, '${index}')")
    }
    sql """
        INSERT INTO ${tableName} VALUES ${values.join(',')}
        """
    sql """
        INSERT INTO ${tableName1} VALUES ${values.join(',')}
        """
    sql 'sync'

    helper.ccrJobDelete()
    helper.ccrJobCreate()

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 30))
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}\"", exist, 60, 'target_sql'))
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName1}\"", exist, 60, 'target_sql'))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName}", insert_num, 60))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName1}", insert_num, 60))

    def show_indexes_result = target_sql_return_maparray "show indexes from ${tableName}"
    assertTrue(show_indexes_result.any { it['Key_name'] == 'idx_value' && it['Index_type'] == 'INVERTED' })

    def first_job_progress = helper.get_job_progress()

    logger.info('=== pause job ===')
    helper.ccrJobPause()

    logger.info('=== add column to two tables, it will trigger partial snapshot ===')
    def column = sql " SHOW ALTER TABLE COLUMN FROM ${context.dbName} WHERE TableName = \"${tableName}\" "

    sql """
        ALTER TABLE ${tableName}
        ADD COLUMN `first` INT KEY DEFAULT "0" FIRST
        """
    sql 'sync'

    assertTrue(helper.checkShowTimesOf("""
                                SHOW ALTER TABLE COLUMN
                                FROM ${context.dbName}
                                WHERE TableName = "${tableName}" AND State = "FINISHED"
                                """,
                                has_count(column.size() + 1), 30))

    sql "INSERT INTO ${tableName} VALUES (123, 123, 123, '1')"
    sql "INSERT INTO ${tableName} VALUES (124, 124, 124, '2')"
    sql "INSERT INTO ${tableName} VALUES (125, 125, 125, '3')"

    sql """
        ALTER TABLE ${tableName1}
        ADD COLUMN `first` INT KEY DEFAULT "0" FIRST
        """
    sql 'sync'

    assertTrue(helper.checkShowTimesOf("""
                                SHOW ALTER TABLE COLUMN
                                FROM ${context.dbName}
                                WHERE TableName = "${tableName1}" AND State = "FINISHED"
                                """,
                                has_count(column.size() + 1), 30))

    logger.info('The drop index should be skipped by partial snapshot')
    sql """
        ALTER TABLE ${tableName}
        DROP INDEX idx_value
        """

    assertTrue(helper.checkShowTimesOf("""
                                SHOW ALTER TABLE COLUMN
                                FROM ${context.dbName}
                                WHERE TableName = "${tableName}" AND State = "FINISHED"
                                """,
                                has_count(column.size() + 2), 30))

    show_indexes_result = sql_return_maparray "show indexes from ${tableName}"
    assertFalse(show_indexes_result.any { it['Key_name'] == 'idx_value' && it['Index_type'] == 'INVERTED' })

    logger.info("=== drop table ${tableName1}, so the partial snapshot will failed ===")

    sql """
        DROP TABLE ${tableName1}
        """

    helper.ccrJobResume()

    def has_column_first = { res -> Boolean
        // Field == 'first' && 'Key' == 'YES'
        return res[0][0] == 'first' && (res[0][3] == 'YES' || res[0][3] == 'true')
    }

    assertTrue(helper.checkShowTimesOf("SHOW COLUMNS FROM `${tableName}`", has_column_first, 60, 'target_sql'))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName}", insert_num + 3, 60))

    sql "INSERT INTO ${tableName} VALUES (126, 126, 126, '4')"
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName}", insert_num + 4, 60))

    show_indexes_result = target_sql_return_maparray "show indexes from ${tableName}"
    assertFalse(show_indexes_result.any { it['Key_name'] == 'idx_value' && it['Index_type'] == 'INVERTED' })

    // no full sync triggered.
    def last_job_progress = helper.get_job_progress()
    assertTrue(last_job_progress.full_sync_start_at == first_job_progress.full_sync_start_at)
}
