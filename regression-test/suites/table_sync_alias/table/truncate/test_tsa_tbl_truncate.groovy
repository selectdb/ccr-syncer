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
suite("test_tsa_tbl_truncate") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def tableName = "tbl_" + helper.randomSuffix()
    def aliasName = "alias_" + helper.randomSuffix()
    def test_num = 0
    def insert_num = 5

    helper.set_alias(aliasName)

    sql """
        CREATE TABLE if NOT EXISTS ${tableName}
        (
            `test` INT,
            `id` INT,
            `num` INT
        )
        ENGINE=OLAP
        UNIQUE KEY(`test`, `id`, `num`)
        PARTITION BY RANGE(`id`)
        (
            PARTITION `ZERO` VALUES LESS THAN ("1"),
            PARTITION `ONE` VALUES LESS THAN ("2"),
            PARTITION `TWO` VALUES LESS THAN ("3"),
            PARTITION `THREE` VALUES LESS THAN ("4")
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
    """


    // test 1: target cluster follow source cluster
    logger.info("=== Test 1: backup/restore case ===")
    helper.ccrJobCreate(tableName)
    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 30))

    def first_job_progress = helper.get_job_progress(tableName)

    logger.info("=== Test 2: full partitions ===")
    test_num = 2
    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${tableName} VALUES (${test_num}, 0, ${index})
            """
    }
    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${tableName} VALUES (${test_num}, 1, ${index})
            """
    }
    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${tableName} VALUES (${test_num}, 2, ${index})
            """
    }
    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${tableName} VALUES (${test_num}, 3, ${index})
            """
    }
    sql "sync"
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${aliasName} WHERE test=${test_num} AND id=0",
                                   insert_num, 30))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${aliasName} WHERE test=${test_num} AND id=1",
                                   insert_num, 30))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${aliasName} WHERE test=${test_num} AND id=2",
                                   insert_num, 30))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${aliasName} WHERE test=${test_num} AND id=3",
                                   insert_num, 30))




    logger.info("=== Test 3: truncate partition ===")
    // TRUNCATE TABLE tbl PARTITION(p1, p2);
    sql """TRUNCATE TABLE ${tableName} PARTITION(`ONE`, `THREE`)"""

    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${aliasName} WHERE id=0",
                                   insert_num, 30))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${aliasName} WHERE id=1",
                                   0, 30))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${aliasName} WHERE id=2",
                                   insert_num, 30))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${aliasName} WHERE id=3",
                                   0, 30))




    logger.info("=== Test 4: truncate table ===")
    // TRUNCATE TABLE tbl PARTITION(p1, p2);
    sql """TRUNCATE TABLE ${tableName}"""

    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${aliasName} WHERE id=0",
                                   0, 30))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${aliasName} WHERE id=1",
                                   0, 30))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${aliasName} WHERE id=2",
                                   0, 30))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${aliasName} WHERE id=3",
                                   0, 30))

    def last_job_progress = helper.get_job_progress(tableName)
    if (helper.is_version_supported([20107, 20016])) {  // at least doris 2.1.7 and doris 2.0.16
        assertTrue(last_job_progress.full_sync_start_at == first_job_progress.full_sync_start_at)
    }
}
