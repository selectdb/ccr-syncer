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

suite("test_syncer_ts_allow_tablet_exists") {
    def versions = sql_return_maparray "show variables like 'version_comment'"
    if (versions[0].Value.contains('doris-2.0.') || versions[0].Value.contains('doris-2.1') ||
        versions[0].Value.contains('doris-3.0.')) {
        logger.info("2.0/2.1/3.0 not support this case, current version is: ${versions[0].Value}")
        return
    }

    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

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

    sql """ INSERT INTO ${tableName} VALUES ${values.join(",")} """
    target_sql """ INSERT INTO ${tableName} VALUES ${values.join(",")} """

    def v = target_sql "SELECT * FROM ${tableName}"
    assertEquals(v.size(), insert_num);
    sql "sync"

    // Since this table is not syncing, the `is_being_sycned` properties should not exists.
    v = target_sql """SHOW CREATE TABLE ${tableName}"""
    assertTrue(v[0][1].contains("is_being_synced\" = \"false") || !v[0][1].contains("is_being_synced"));

    helper.ccrJobCreateAllowTableExists(tableName)

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 60))

    // table sync should NOT clean the exists tables in the same db!!!
    v = target_sql "SELECT * FROM ${tableName}"
    assertTrue(v.size() == insert_num);
    v = target_sql """SHOW CREATE TABLE ${tableName}"""
    assertTrue(v[0][1].contains("is_being_synced\" = \"true"));
}



