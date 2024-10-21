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

suite("test_ts_part_replace_partial") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def baseTableName = "tbl_" + helper.randomSuffix()
    def test_num = 0
    def insert_num = 5
    def opPartitonName = "less0"

    def exist = { res -> Boolean
        return res.size() != 0
    }
    def notExist = { res -> Boolean
        return res.size() == 0
    }

    logger.info("=== Create table ===")
    tableName = "${baseTableName}"
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
            PARTITION `p1` VALUES LESS THAN ("0"),
            PARTITION `p2` VALUES LESS THAN ("100"),
            PARTITION `p3` VALUES LESS THAN ("200"),
            PARTITION `p4` VALUES LESS THAN ("300")
        )
        DISTRIBUTED BY HASH(id) BUCKETS AUTO
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
    """

    // insert into p2,p3,p4
    sql """
        INSERT INTO ${tableName} VALUES
            (1, 10),
            (1, 11),
            (1, 12),
            (1, 13),
            (1, 14),
            (2, 100),
            (2, 110),
            (2, 120),
            (2, 130),
            (2, 140),
            (3, 200),
            (3, 210),
            (3, 220),
            (3, 230),
            (3, 240)
    """
    sql "sync"

    helper.ccrJobCreate(tableName)

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 60))
    // p2,p3,p4 all has 5 rows
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName} WHERE test=1", 5, 60))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName} WHERE test=2", 5, 60))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName} WHERE test=3", 5, 60))

    logger.info("=== Add temp partition p5 ===")

    sql """
        ALTER TABLE ${tableName} ADD TEMPORARY PARTITION p5 VALUES [("0"), ("100"))
        """

    assertTrue(helper.checkShowTimesOf("""
                                SHOW TEMPORARY PARTITIONS
                                FROM ${tableName}
                                WHERE PartitionName = "p5"
                                """,
                                exist, 60, "sql"))

    sql "INSERT INTO ${tableName} TEMPORARY PARTITION (p5) VALUES (1, 50)"

    assertTrue(helper.checkShowTimesOf("""
                                SELECT *
                                FROM ${tableName}
                                TEMPORARY PARTITION (p5)
                                WHERE id = 50
                                """,
                                exist, 60, "sql"))

    logger.info("=== Replace partition p2 by p5 ===")

    assertTrue(helper.checkShowTimesOf("""
                                SELECT *
                                FROM ${tableName}
                                WHERE id = 50
                                """,
                                notExist, 60, "target"))

    sql "ALTER TABLE ${tableName} REPLACE PARTITION (p2) WITH TEMPORARY PARTITION (p5)"

    assertTrue(helper.checkShowTimesOf("""
                                SELECT *
                                FROM ${tableName}
                                WHERE id = 50
                                """,
                                exist, 60, "target"))

    // p3,p4 all has 5 rows, p2 has 1 row
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName} WHERE test=1", 1, 60))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName} WHERE test=2", 5, 60))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName} WHERE test=3", 5, 60))

    // The last restore should contains only partition p2
    def show_restore_result = target_sql "SHOW RESTORE FROM TEST_${context.dbName}"
    def restore_num = show_restore_result.size()
    def last_restore_result = show_restore_result[restore_num-1]
    def restore_objects = last_restore_result[10]   // RestoreObjs
    logger.info("The restore result: ${last_restore_result}")
    logger.info("The restore objects: ${restore_objects}")

    // {
    //  "name": "ccrp_regression_test_table_sync_test_replace_partial_p_02f747eda70e4f768afd613e074e790d_1722983645",
    //  "database": "regression_test_table_sync",
    //  "backup_time": 1722983645667,
    //  "content": "ALL",
    //  "olap_table_list": [
    //    {
    //      "name": "test_replace_partial_p_02f747eda70e4f768afd613e074e790d",
    //      "partition_names": [
    //        "p2"
    //      ]
    //    }
    //  ],
    //  "view_list": [],
    //  "odbc_table_list": [],
    //  "odbc_resource_list": []
    // }
    def jsonSlurper = new groovy.json.JsonSlurper()
    def object = jsonSlurper.parseText "${restore_objects}"
    assertTrue(object.olap_table_list[0].partition_names.size() == 1)
    assertTrue(object.olap_table_list[0].partition_names[0] == "p2")
}


