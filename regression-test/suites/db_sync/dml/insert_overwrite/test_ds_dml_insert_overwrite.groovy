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
suite("test_ds_dml_insert_overwrite") {
    def versions = sql_return_maparray "show variables like 'version_comment'"
    if (versions[0].Value.contains('doris-2.0.')) {
        logger.info("2.0 not support INSERT OVERWRITE yet, current version is: ${versions[0].Value}")
        return
    }

    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    // The doris has two kind of insert overwrite handle logic: leagcy and nereids.
    // The first will
    //  1. create temp table
    //  2. insert into temp table
    //  3. replace table
    // The second will
    //  1. create temp partitions
    //  2. insert into temp partitions
    //  3. replace overlap partitions
    def tableName = "tbl_" + helper.randomSuffix()
    def uniqueTable = "${tableName}_unique"
    def test_num = 0
    def insert_num = 5
    String response

    def exist = { res -> Boolean
        return res.size() != 0
    }
    def notExist = { res -> Boolean
        return res.size() == 0
    }

    helper.enableDbBinlog()

    sql """
        CREATE TABLE if NOT EXISTS ${uniqueTable}
        (
            `test` INT,
            `id` INT
        )
        ENGINE=OLAP
        UNIQUE KEY(`test`, `id`)
        PARTITION BY RANGE(id)
        (
            PARTITION `p1` VALUES LESS THAN ("100"),
            PARTITION `p2` VALUES LESS THAN ("200")
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true",
            "binlog.ttl_seconds" = "180"
        )
    """

    sql """
    INSERT INTO ${uniqueTable} VALUES
        (1, 0),
        (1, 1),
        (1, 2),
        (1, 3),
        (1, 4)
    """
    sql "sync"

    // test 1: target cluster follow source cluster
    logger.info("=== Test 1: backup/restore case ===")

    helper.ccrJobDelete()
    helper.ccrJobCreate()
    assertTrue(helper.checkRestoreFinishTimesOf("${uniqueTable}", 60))
    assertTrue(helper.checkShowTimesOf("SELECT * FROM ${uniqueTable} WHERE test = 1", exist, 60, "sql"))
    assertTrue(helper.checkShowTimesOf("SELECT * FROM ${uniqueTable} WHERE test = 1", exist, 60, "target"))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${uniqueTable} WHERE test = 1 ORDER BY id", 5, 60))

    logger.info("=== Test 2: dest cluster follow source cluster case ===")

    sql """
    INSERT INTO ${uniqueTable} VALUES
        (2, 0),
        (2, 1),
        (2, 2),
        (2, 3),
        (2, 4)
    """
    sql "sync"

    assertTrue(helper.checkShowTimesOf("SELECT * FROM ${uniqueTable} WHERE test=2", exist, 60, "sql"))
    assertTrue(helper.checkShowTimesOf("SELECT * FROM ${uniqueTable} WHERE test=2", exist, 60, "target"))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${uniqueTable} WHERE test=2", 5, 60))

    logger.info("=== Test 3: insert overwrite source table ===")

    sql """
    INSERT OVERWRITE TABLE ${uniqueTable} VALUES
        (3, 0),
        (3, 1),
        (3, 2),
        (3, 3),
        (3, 4)
    """
    sql "sync"

    assertTrue(helper.checkShowTimesOf("SELECT * FROM ${uniqueTable} WHERE test=3", exist, 60, "sql"))
    assertTrue(helper.checkShowTimesOf("SELECT * FROM ${uniqueTable} WHERE test=2", notExist, 60, "sql"))
    assertTrue(helper.checkShowTimesOf("SELECT * FROM ${uniqueTable} WHERE test=3", exist, 60, "target"))
    assertTrue(helper.checkShowTimesOf("SELECT * FROM ${uniqueTable} WHERE test=2", notExist, 60, "target"))

    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${uniqueTable} WHERE test=3", 5, 60))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${uniqueTable}", 5, 60))
}


