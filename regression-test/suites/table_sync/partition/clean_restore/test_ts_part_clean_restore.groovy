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

suite("test_ts_part_clean_restore") {
    // FIXME(walter) fix clean partitions.
    return

    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))
    def tableName = "tbl_" + helper.randomSuffix()
    def test_num = 0
    def insert_num = 20
    def sync_gap_time = 5000

    def exist = { res -> Boolean
        return res.size() != 0
    }
    def notExist = { res -> Boolean
        return res.size() == 0
    }

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
            PARTITION `${opPartitonName}_2` VALUES LESS THAN ("20")
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
    """

    target_sql "CREATE DATABASE IF NOT EXISTS TEST_${context.dbName}"
    target_sql """
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
            PARTITION `${opPartitonName}_2` VALUES LESS THAN ("20")
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        )
    """

    sql """
        CREATE TABLE if NOT EXISTS ${tableName}_2
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
            PARTITION `${opPartitonName}_2` VALUES LESS THAN ("20")
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
    """

    target_sql """
        CREATE TABLE if NOT EXISTS ${tableName}_2
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
            PARTITION `${opPartitonName}_2` VALUES LESS THAN ("20")
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        )
    """

    List<String> values = []
    for (int index = 0; index < insert_num; index++) {
        values.add("(${test_num}, ${index})")
    }

    sql """ INSERT INTO ${tableName}_1 VALUES ${values.join(",")} """
    sql """ INSERT INTO ${tableName}_2 VALUES ${values.join(",")} """
    target_sql """ INSERT INTO ${tableName}_1 VALUES ${values.join(",")} """
    target_sql """ INSERT INTO ${tableName}_2 VALUES ${values.join(",")} """

    def v = target_sql "SELECT * FROM ${tableName}_1"
    assertEquals(v.size(), insert_num);
    v = target_sql "SELECT * FROM ${tableName}_2"
    assertEquals(v.size(), insert_num);

    sql "ALTER TABLE ${tableName}_2 DROP PARTITION ${opPartitonName}_0 FORCE"
    sql "ALTER TABLE ${tableName}_2 DROP PARTITION ${opPartitonName}_1 FORCE"
    sql "sync"

    helper.ccrJobCreateAllowTableExists("${tableName}_2")

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}_2", 60))

    // table sync should NOT clean the exists tables in the same db!!!
    v = target_sql "SELECT * FROM ${tableName}_2"
    assertTrue(v.size() == (insert_num-10));
    v = target_sql """ SHOW TABLES LIKE "${tableName}_1" """
    assertTrue(v.size() == 1);
}


