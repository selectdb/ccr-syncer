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

suite("test_ds_tbl_truncate") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def baseTableName = "tbl_truncate_" + helper.randomSuffix()
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
    def tableName = "${baseTableName}"
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
            PARTITION `p4` VALUES LESS THAN ("300"),
            PARTITION `p5` VALUES LESS THAN ("400")
        )
        DISTRIBUTED BY HASH(id) BUCKETS AUTO
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
    """

    helper.enableDbBinlog()
    helper.ccrJobDelete()
    helper.ccrJobCreate()

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 60))

    sql "INSERT INTO ${tableName} VALUES (1, 100), (100, 1), (2, 200), (200, 2)"
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName}", 4, 60))

    logger.info(" ==== truncate table ==== ")
    def first_job_progress = helper.get_job_progress()

    helper.ccrJobPause()

    sql "INSERT INTO ${tableName} VALUES (3, 300), (300, 3)"
    sql "TRUNCATE TABLE ${tableName}"
    sql "INSERT INTO ${tableName} VALUES (2, 300)"

    helper.ccrJobResume()

    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName}", 1, 60))

    logger.info(" ==== truncate partitions ==== ")

    helper.ccrJobPause()
    sql "INSERT INTO ${tableName} VALUES (3, 230)" // insert into p4
    sql "INSERT INTO ${tableName} VALUES (4, 250)" // insert into p4
    sql "INSERT INTO ${tableName} VALUES (2, 350)" // insert into p5
    sql "TRUNCATE TABLE ${tableName} PARTITIONS (p5)"
    helper.ccrJobResume()

    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName}", 2, 60))  // p5 are truncated

    // no fullsync are triggered
    def last_job_progress = helper.get_job_progress()
    if (helper.is_version_supported([20107, 20016])) {  // at least doris 2.1.7 and doris 2.0.16
        assertTrue(last_job_progress.full_sync_start_at == first_job_progress.full_sync_start_at)
    }
}
