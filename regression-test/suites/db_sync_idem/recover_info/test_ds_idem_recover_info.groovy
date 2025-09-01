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

suite('test_ds_idem_recover_info') {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", 'helper.groovy'))

    if (!helper.has_feature("feature_idempotent_ddl")) {
        logger.info("this case only works with feature_idempotent_ddl")
        return
    }

    def suffix = helper.randomSuffix()
    def tableName = 'tbl_' + suffix

    def exist = { res -> Boolean
        return res.size() != 0
    }
    def notExist = { res -> Boolean
        return res.size() == 0
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
        PARTITION BY RANGE(id)
        (
            PARTITION `p1` VALUES LESS THAN ("100"),
            PARTITION `p2` VALUES LESS THAN ("200")
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num"="3",
            "binlog.enable" = "true",
            "binlog.ttl_seconds" = "180"
        )
    """
    helper.ccrJobDelete()
    helper.ccrJobCreate()

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 60))

    // The below failpoints are automatically removed when the first hit.
    //
    // The first failpoint is used to simulate the failure occurred before committed.
    // The second failpoint is used to simulate the failure of committed but rpc failed.
    helper.addFailpoint('handle_binlog_idempotent:before', 'RECOVER_INFO')
    helper.addFailpoint('handle_binlog_idempotent:after', 'RECOVER_INFO')

    sql """
        INSERT INTO ${tableName} VALUES (1, 1), (2, 2), (3, 3)
    """

    assertTrue(helper.checkSelectTimesOf("select * from ${tableName}", 3, 30))
    // recover table
    sql """
        DROP TABLE ${tableName}
    """

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}\"", notExist, 60, "sql"))
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}\"", notExist, 60, "target"))

    sql """
        RECOVER TABLE ${tableName}
    """
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}\"", exist, 60, "sql"))
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}\"", exist, 60, "target"))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName}", 3, 60))

    sql """
        INSERT INTO ${tableName} VALUES (150, 150), (151, 152), (153, 153)
    """

    assertTrue(helper.checkSelectTimesOf("select * from ${tableName}", 6, 30))
    // recover partition
    sql """
        ALTER TABLE ${tableName} DROP PARTITION `p1`
    """
    assertTrue(helper.checkShowTimesOf("""
                                SHOW PARTITIONS
                                FROM TEST_${context.dbName}.${tableName}
                                WHERE PartitionName = "p1"
                                """,
                                notExist, 60, "target"))

    sql """
        RECOVER PARTITION p1 FROM ${tableName}
    """
    assertTrue(helper.checkShowTimesOf("""
                                SHOW PARTITIONS
                                FROM TEST_${context.dbName}.${tableName}
                                WHERE PartitionName = "p1"
                                """,
                                exist, 60, "target"))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName}", 6, 60))
}
