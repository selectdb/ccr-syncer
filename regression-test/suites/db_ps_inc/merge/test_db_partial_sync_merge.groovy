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
suite("test_db_partial_sync_inc_merge") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    if (!helper.has_feature("feature_schema_change_partial_sync")) {
        logger.info("this suite require feature_schema_change_partial_sync set to true")
        return
    }

    def tableName = "tbl_" + helper.randomSuffix()
    def tableName1 = "tbl_" + helper.randomSuffix()
    def test_num = 0
    def insert_num = 5

    def exist = { res -> Boolean
        return res.size() != 0
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
            `value` INT SUM
        )
        ENGINE=OLAP
        AGGREGATE KEY(`test`, `id`)
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
            `value` INT SUM
        )
        ENGINE=OLAP
        AGGREGATE KEY(`test`, `id`)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
    """

    def values = [];
    for (int index = 0; index < insert_num; index++) {
        values.add("(${test_num}, ${index}, ${index})")
    }
    sql """ INSERT INTO ${tableName} VALUES ${values.join(",")} """
    sql """ INSERT INTO ${tableName1} VALUES ${values.join(",")} """
    sql "sync"

    helper.ccrJobDelete()
    helper.ccrJobCreate()

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 30))
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}\"", exist, 60, "target_sql"))
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName1}\"", exist, 60, "target_sql"))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName}", insert_num, 60))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName1}", insert_num, 60))

    def first_job_progress = helper.get_job_progress()

    // the change flow of the sync states:
    //
    //  db incremental sync                                         pause CCR job
    //      -> db partial sync table A                              add column to A
    //          -> db tables incremental with table A               insert some data into A
    //              -> db partial sync table B                      add column to B
    //                  -> db tables incremental with table A/B     insert some data into A/B, resume CCR job
    //                      -> db incremental sync
    helper.ccrJobPause()

    def column = sql " SHOW ALTER TABLE COLUMN FROM ${context.dbName} WHERE TableName = \"${tableName}\" "

    sql """
        ALTER TABLE ${tableName}
        ADD COLUMN `first` INT KEY DEFAULT "0" FIRST
        """
    sql "sync"

    assertTrue(helper.checkShowTimesOf("""
                                SHOW ALTER TABLE COLUMN
                                FROM ${context.dbName}
                                WHERE TableName = "${tableName}" AND State = "FINISHED"
                                """,
                                has_count(column.size() + 1), 30))

    sql "INSERT INTO ${tableName} VALUES (123, 123, 123, 1)"

    column = sql " SHOW ALTER TABLE COLUMN FROM ${context.dbName} WHERE TableName = \"${tableName1}\" "

    sql """
        ALTER TABLE ${tableName1}
        ADD COLUMN `first` INT KEY DEFAULT "0" FIRST
        """
    sql "sync"

    assertTrue(helper.checkShowTimesOf("""
                                SHOW ALTER TABLE COLUMN
                                FROM ${context.dbName}
                                WHERE TableName = "${tableName1}" AND State = "FINISHED"
                                """,
                                has_count(column.size() + 1), 30))

    sql "INSERT INTO ${tableName} VALUES (123, 123, 123, 2)"
    sql "INSERT INTO ${tableName} VALUES (123, 123, 123, 3)"
    sql "INSERT INTO ${tableName1} VALUES (123, 123, 123, 1)"
    sql "INSERT INTO ${tableName1} VALUES (123, 123, 123, 2)"
    sql "INSERT INTO ${tableName1} VALUES (123, 123, 123, 3)"

    helper.ccrJobResume()

    def has_column_first = { res -> Boolean
        // Field == 'first' && 'Key' == 'YES'
        return res[0][0] == 'first' && (res[0][3] == 'YES' || res[0][3] == 'true')
    }

    assertTrue(helper.checkShowTimesOf("SHOW COLUMNS FROM `${tableName}`", has_column_first, 60, "target_sql"))
    assertTrue(helper.checkShowTimesOf("SHOW COLUMNS FROM `${tableName1}`", has_column_first, 60, "target_sql"))

    logger.info("the aggregate keys inserted should be synced accurately")
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName}", insert_num + 1, 60))
    def last_record = target_sql "SELECT value FROM ${tableName} WHERE id = 123 AND test = 123"
    logger.info("last record is ${last_record}")
    assertTrue(last_record.size() == 1 && last_record[0][0] == 6)

    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName1}", insert_num + 1, 60))
    last_record = target_sql "SELECT value FROM ${tableName1} WHERE id = 123 AND test = 123"
    logger.info("last record of table ${tableName1} is ${last_record}")
    assertTrue(last_record.size() == 1 && last_record[0][0] == 6)

    // no full sync triggered.
    def last_job_progress = helper.get_job_progress()
    assertTrue(last_job_progress.full_sync_start_at == first_job_progress.full_sync_start_at)
}



