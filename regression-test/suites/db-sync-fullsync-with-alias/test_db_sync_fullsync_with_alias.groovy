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

suite("test_db_sync_fullsync_with_alias") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    if (!helper.has_feature("feature_replace_not_matched_with_alias")) {
        logger.info("this case only works with feature_replace_not_matched_with_alias")
        return
    }

    def tableName = "tbl_" + helper.randomSuffix()
    def test_num = 0
    def insert_num = 20
    def opPartitonName = "less"

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

    logger.info("create two tables")

    sql """
        CREATE TABLE if NOT EXISTS ${tableName}
        (
            `test` INT,
            `id` INT
        )
        ENGINE=OLAP
        UNIQUE KEY(`test`, `id`)
        PARTITION BY RANGE(`id`)
        (
            PARTITION `${opPartitonName}_0` VALUES LESS THAN ("0"),
            PARTITION `${opPartitonName}_1` VALUES LESS THAN ("10"),
            PARTITION `${opPartitonName}_2` VALUES LESS THAN ("20"),
            PARTITION `${opPartitonName}_3` VALUES LESS THAN ("30"),
            PARTITION `${opPartitonName}_4` VALUES LESS THAN ("40")
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
    """
    sql """
        CREATE TABLE if NOT EXISTS ${tableName}_1
        (
            `test` INT,
            `id` INT
        )
        ENGINE=OLAP
        UNIQUE KEY(`test`, `id`)
        PARTITION BY RANGE(`id`)
        (
            PARTITION `${opPartitonName}_0` VALUES LESS THAN ("0"),
            PARTITION `${opPartitonName}_1` VALUES LESS THAN ("10"),
            PARTITION `${opPartitonName}_2` VALUES LESS THAN ("20"),
            PARTITION `${opPartitonName}_3` VALUES LESS THAN ("30"),
            PARTITION `${opPartitonName}_4` VALUES LESS THAN ("40")
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
    """

    List<String> values = []
    for (int index = 0; index < insert_num; index++) {
        values.add("(${test_num}, ${index})")
    }

    sql """ INSERT INTO ${tableName} VALUES ${values.join(",")} """
    sql """ INSERT INTO ${tableName}_1 VALUES ${values.join(",")} """
    sql "sync"

    helper.ccrJobDelete()
    helper.ccrJobCreate()

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 60))
    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}_1", 60))

    logger.info("pause ccr job, change table1 schema and trigger fullsync, then the upsert of table2 should be synced")
    helper.ccrJobPause()
    helper.force_fullsync()

    values.clear();
    for (int index = insert_num; index < insert_num * 2; index++) {
        values.add("(${test_num}, ${index})")
    }

    sql """ INSERT INTO ${tableName} VALUES ${values.join(",")} """
    sql """ INSERT INTO ${tableName}_1 VALUES ${values.join(",")} """

    sql """
        CREATE VIEW ${tableName}_view (k1, k2)
        AS
        SELECT test as k1, sum(id) as k2 FROM ${tableName}
        GROUP BY test;
        """

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
                                has_count(1), 30))

    sql "sync"

    logger.info("resume job, then the upserts both table1 and table2 will be synced to downstream")
    helper.ccrJobResume()

    def has_column_first = { res -> Boolean
        // Field == 'first' && 'Key' == 'YES'
        return res[0][0] == 'first' && (res[0][3] == 'YES' || res[0][3] == 'true')
    }

    assertTrue(helper.checkShowTimesOf("SHOW COLUMNS FROM `${tableName}`", has_column_first, 60, "target_sql"))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName}", insert_num * 2, 60))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName}_1", insert_num * 2, 60))
    def view_size = target_sql "SHOW VIEW FROM ${tableName}"
    assertTrue(view_size.size() == 1);
}



