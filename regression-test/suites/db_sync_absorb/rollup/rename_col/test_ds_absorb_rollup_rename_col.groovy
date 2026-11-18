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
suite("test_ds_absorb_rollup_rename_col") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    if (helper.has_feature('feature_skip_rollup_binlogs')) {
        logger.info('skip this suite because feature_skip_rollup_binlogs is enabled')
        return
    }

    def dbName = context.dbName
    def tableName = "tbl_" + helper.randomSuffix()
    def test_num = 0
    def insert_num = 10

    def exist = { res -> Boolean
        return res.size() != 0
    }

    def notExist = { res -> Boolean
        return res.size() == 0
    }

    def hasRollupFull = { res -> Boolean
        for (List<Object> row : res) {
            if ((row[0] as String) == "rollup_${tableName}_full") {
                return true
            }
        }
        return false
    }

    def has_count = { count ->
        return { res -> Boolean
            res.size() == count
        }
    }
    def has_column = { column ->
        return { res -> Boolean
            res[2][0] == column
        }
    }
    sql """
        CREATE TABLE if NOT EXISTS ${tableName}
        (
            `id` INT,
            `col1` INT,
            `col2` INT,
            `col3` INT,
            `col4` INT,
        )
        ENGINE=OLAP
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
    """

    sql """
        ALTER TABLE ${tableName}
        ADD ROLLUP rollup_${tableName}_full (id, col2, col4)
        """

    helper.enableDbBinlog()
    helper.ccrJobDelete()
    helper.ccrJobCreate()

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 180))
    assertTrue(helper.checkShowTimesOf(""" SHOW TABLES LIKE "${tableName}" """, exist, 60, "sql"))
    assertTrue(helper.checkShowTimesOf(""" SHOW TABLES LIKE "${tableName}" """, exist, 60, "target"))
    assertTrue(helper.checkShowTimesOf("DESC ${context.dbName}.${tableName} ALL", hasRollupFull, 30, 'sql'))
    assertTrue(helper.checkShowTimesOf("DESC TEST_${context.dbName}.${tableName} ALL", hasRollupFull, 30, 'target'))

    // 0. Insert N data
    for (int index = insert_num; index < insert_num * 2; index++) {
        sql """
            INSERT INTO ${tableName} VALUES (${index}, ${index}, ${index}, ${index}, ${index})
            """
    }
    assertTrue(helper.checkShowTimesOf(""" select * from ${tableName} """, { r -> r.size() == insert_num}, 60, "sql"))
    assertTrue(helper.checkShowTimesOf(""" select * from ${tableName} """, { r -> r.size() == insert_num}, 60, "target"))
    // 1. Pause ccr job
    helper.ccrJobPause()

    // 2. Insert N data
    for (int index = insert_num; index < insert_num * 2; index++) {
        sql """
            INSERT INTO ${tableName} VALUES (${index}, ${index}, ${index}, ${index}, ${index})
            """
    }

    // 3. Do operation & wait it finishes upstream
    sql """
        ALTER TABLE ${tableName} RENAME COLUMN `col2` `col02`
        """

    assertTrue(helper.checkShowTimesOf("SHOW COLUMNS FROM ${tableName}", has_column("col02"), 60, "sql"))
    // 4. Insert N data
    for (int index = insert_num * 2; index < insert_num * 3; index++) {
        sql """
            INSERT INTO ${tableName} VALUES (${index}, ${index}, ${index}, ${index}, ${index})
            """
    }

    assertTrue(helper.checkShowTimesOf(""" select * from ${tableName} """, { r -> r.size() == insert_num * 3}, 60, "sql"))
    // 5. Force trigger fullsnapshot
    helper.force_fullsync()

    // 6. Resume ccr job
    helper.ccrJobResume()
  
    // 7. Verify data and operation are synced downstream
    assertTrue(helper.checkShowTimesOf(""" select * from ${tableName} """, { r -> r.size() == insert_num * 3}, 60, "target"))

    assertTrue(helper.checkShowTimesOf("SHOW COLUMNS FROM ${tableName}", has_column("col02"), 60, "target"))
}