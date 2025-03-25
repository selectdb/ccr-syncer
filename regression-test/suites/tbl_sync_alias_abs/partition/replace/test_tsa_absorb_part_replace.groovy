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
suite("test_tsa_absorb_part_replace") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def tableName = "tbl_" + helper.randomSuffix()
    def test_num = 3
    def insert_num = 10
    def aliasTableName = "alias_tbl_" + helper.randomSuffix()
    helper.set_alias(aliasTableName)

    def exist = { res -> Boolean
        return res.size() != 0
    }
    def notExist = { res -> Boolean
        return res.size() == 0
    }

    sql "DROP TABLE IF EXISTS ${tableName}"
    target_sql "DROP TABLE IF EXISTS ${aliasTableName}"

    sql """
        CREATE TABLE if NOT EXISTS ${tableName}
        (
            `test` INT,
            `id` INT
        )
        ENGINE=OLAP
        UNIQUE KEY(`test`, `id`)
        PARTITION BY RANGE(`id`) (
            PARTITION `p0` VALUES LESS THAN ("0"),
            PARTITION `p1` VALUES LESS THAN ("10"),
            PARTITION `p2` VALUES LESS THAN ("20"),
            PARTITION `p3` VALUES LESS THAN ("30")
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
    """

    helper.enableDbBinlog()
    helper.ccrJobDelete(tableName)
    helper.ccrJobCreate(tableName)

    assertTrue(helper.checkShowTimesOf(""" SHOW TABLES LIKE "${aliasTableName}" """, exist, 60, "target"))

    // 0. Insert N data
    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${tableName} VALUES (${test_num}, ${index})
            """
    }
    assertTrue(helper.checkShowTimesOf(""" select * from ${tableName} """, { r -> r.size() == insert_num}, 60, "sql"))
    assertTrue(helper.checkShowTimesOf(""" select * from ${aliasTableName} """, { r -> r.size() == insert_num}, 60, "target"))

    // 1. Pause ccr job
    helper.ccrJobPause(tableName)

    // 2. Insert N data
    for (int index = insert_num; index < insert_num * 2; index++) {
        sql """
            INSERT INTO ${tableName} VALUES (${test_num}, ${index})
            """
    }
    assertTrue(helper.checkShowTimesOf(""" select * from ${tableName} """, { r -> r.size() == insert_num * 2}, 60, "sql"))

    // 3. Do operation & wait it finishes upstream
    sql """
        ALTER TABLE ${tableName} ADD TEMPORARY PARTITION p5 VALUES [("10"), ("20"))
        """
    sql "ALTER TABLE ${tableName} REPLACE PARTITION (p2) WITH TEMPORARY PARTITION (p5)"
    assertTrue(helper.checkShowTimesOf(""" SHOW PARTITIONS FROM ${tableName} where PartitionName = "p2" """, exist, 60, "sql"))
    assertTrue(helper.checkShowTimesOf(""" SHOW PARTITIONS FROM ${tableName} where PartitionName = "p5" """, notExist, 60, "sql"))

    // 4. Insert N data
    for (int index = insert_num; index < insert_num * 2; index++) {
        sql """
            INSERT INTO ${tableName} VALUES (${test_num}, ${index})
            """
    }
    assertTrue(helper.checkShowTimesOf(""" select * from ${tableName} """, { r -> r.size() == insert_num * 2}, 60, "sql"))

    // 5. Force trigger fullsnapshot
    helper.force_fullsync(tableName)

    // 6. Resume ccr job
    helper.ccrJobResume(tableName)
  
    // 7. Verify data and operation are synced downstream
    assertTrue(helper.checkShowTimesOf(""" SHOW PARTITIONS FROM ${aliasTableName} where PartitionName = "p2" """, exist, 60, "target"))
    assertTrue(helper.checkShowTimesOf(""" SHOW PARTITIONS FROM ${tableName} where PartitionName = "p5" """, notExist, 60, "sql"))
    assertTrue(helper.checkShowTimesOf(""" select * from ${aliasTableName} """, { r -> r.size() == insert_num * 2}, 60, "target"))
}
