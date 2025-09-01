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

suite('test_cds_view_signature_not_matched') {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", 'helper.groovy'))

    def suffix = helper.randomSuffix()
    def tableName = 'tbl_' + suffix
    def test_num = 0
    def insert_num = 20
    def opPartitonName = 'less'

    def exist = { res -> Boolean
        return res.size() != 0
    }
    def notExist = { res -> Boolean
        return res.size() == 0
    }

    helper.enableDbBinlog()

    logger.info('create table with same schema')

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
            PARTITION `${opPartitonName}_2` VALUES LESS THAN ("20"),
            PARTITION `${opPartitonName}_3` VALUES LESS THAN ("30"),
            PARTITION `${opPartitonName}_4` VALUES LESS THAN ("40")
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        )
    """

    logger.info('create two views with different schema')
    sql """
        CREATE VIEW view_test_${suffix} (k1, v1)
        AS
        SELECT test as k1, SUM(id) FROM ${tableName}
        GROUP BY k1, id;
        """

    target_sql """
        CREATE VIEW view_test_${suffix} (k1, v1)
        AS
        SELECT test as k1, MAX(id) FROM ${tableName}
        GROUP BY k1, id;
        """

    List<String> values = []
    for (int index = 0; index < insert_num; index++) {
        values.add("(${test_num}, ${index})")
    }

    sql """ INSERT INTO ${tableName} VALUES ${values.join(',')} """
    sql 'sync'

    def v = sql "SELECT * FROM ${tableName}"
    assertEquals(v.size(), insert_num)

    helper.ccrJobDelete()
    helper.ccrJobCreate()

    logger.info('dest cluster drop unmatched views')
    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 60))

    v = target_sql "SELECT * FROM ${tableName}"
    assertTrue(v.size() == insert_num)

    logger.info('Insert new records, need to be synced')

    values.clear()
    for (int index = insert_num; index < insert_num * 2; index++) {
        values.add("(${test_num}, ${index})")
    }

    sql """ INSERT INTO ${tableName} VALUES ${values.join(',')} """
    sql 'sync'
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName}", insert_num * 2, 60))
}
