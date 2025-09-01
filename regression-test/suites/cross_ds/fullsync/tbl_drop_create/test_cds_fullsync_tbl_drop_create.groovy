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

suite("test_cds_fullsync_tbl_drop_create") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    if (!helper.has_feature("feature_replace_not_matched_with_alias")) {
        logger.info("this case only works with feature_replace_not_matched_with_alias")
        return
    }

    // Case description
    // 1. Create two tables
    // 2. Pause ccr job, drop table1, then trigger fullsync
    // 3. Resume ccr job, insert data into table1
    // 4. Create table1 again, insert data into table1
    // 5. Check data in table1 and table2

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

    logger.info("pause ccr job, drop table1, then trigger fullsync")
    helper.ccrJobPause()

    sql "DROP TABLE ${tableName}_1"
    helper.force_fullsync()

    values.clear();
    for (int index = insert_num; index < insert_num * 2; index++) {
        values.add("(${test_num}, ${index})")
    }
    sql """ INSERT INTO ${tableName} VALUES ${values.join(",")} """
    sql "sync"

    helper.ccrJobResume()

    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName}", insert_num * 2, 60))

    logger.info("create table ${tableName}_1 again")
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
    values.clear();
    for (int index = 0; index < insert_num * 2; index++) {
        values.add("(${test_num}, ${index})")
    }
    sql """ INSERT INTO ${tableName}_1 VALUES ${values.join(",")} """
    sql "sync"

    def has_expect_rows = { res ->
        return res.size() == insert_num * 2
    }
    assertTrue(helper.checkShowTimesOf("SELECT * FROM ${tableName}_1", has_expect_rows, 60))
}




