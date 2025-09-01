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

suite("test_ts_part_alter") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def tableName = "test_ts_part_alter_partition_tbl"

    def exist = { res -> Boolean
        return res.size() != 0
    }

    def notExist = { res -> Boolean
        return res.size() == 0
    }

    helper.enableDbBinlog()

    logger.info("=== Test 1: Alter partitions case ===")

    sql "DROP TABLE IF EXISTS ${tableName}"
    target_sql "DROP TABLE IF EXISTS ${tableName}"

    sql """
        CREATE TABLE ${tableName} (
            id int null,
            k largeint null
        )
        ENGINE=OLAP
        AUTO PARTITION BY LIST (`id`, `k`)
        (
        )
        DISTRIBUTED BY HASH(`k`) BUCKETS 16
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        );
    """

    helper.ccrJobDelete(tableName)
    helper.ccrJobCreate(tableName)

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 30))
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}\"", exist, 60, "sql"))
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}\"", exist, 60, "target"))

    sql """
        ALTER TABLE ${tableName}
        ADD PARTITION p0
        VALUES IN ((NULL, MAXVALUE))
    """

    sql """
        ALTER TABLE ${tableName}
        ADD PARTITION p1
        VALUES IN ((NULL, NULL))
    """

    sql """
        ALTER TABLE ${tableName}
        ADD PARTITION p2
        VALUES IN ((MAXVALUE, MAXVALUE))
    """

    sql """
        ALTER TABLE ${tableName}
        ADD PARTITION p3
        VALUES IN ((MAXVALUE, NULL))
    """

    assertTrue(helper.checkShowTimesOf("""
                                SHOW PARTITIONS
                                FROM ${tableName}
                                WHERE PartitionName = \"p1\"
                                """,
                                exist, 30, "target"))

    assertTrue(helper.checkShowTimesOf("""
                                SHOW PARTITIONS
                                FROM ${tableName}
                                WHERE PartitionName = \"p2\"
                                """,
                                exist, 30, "target"))

    assertTrue(helper.checkShowTimesOf("""
                                SHOW PARTITIONS
                                FROM ${tableName}
                                WHERE PartitionName = \"p2\"
                                """,
                                exist, 30, "target"))

    assertTrue(helper.checkShowTimesOf("""
                                SHOW PARTITIONS
                                FROM ${tableName}
                                WHERE PartitionName = \"p2\"
                                """,
                                exist, 30, "target"))

    logger.info("=== Test 2: Add partitions from insert case ===")

    sql "DROP TABLE IF EXISTS ${tableName}"
    target_sql "DROP TABLE IF EXISTS ${tableName}"

    sql """
        CREATE TABLE ${tableName} (
            id int null,
            k largeint null
        )
        ENGINE=OLAP
        AUTO PARTITION BY LIST (`id`, `k`)
        (
        )
        DISTRIBUTED BY HASH(`k`) BUCKETS 16
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        );
    """

    helper.ccrJobDelete(tableName)
    helper.ccrJobCreate(tableName)

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 30))
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}\"", exist, 60, "sql"))
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}\"", exist, 60, "target"))

    sql """
        insert into ${tableName} values (1,1), (-1,-1);
    """

    sql """
        insert into ${tableName} values (null,null),(null,1);
    """

    assertTrue(helper.checkShowTimesOf("""
                                SHOW PARTITIONS
                                FROM ${tableName}
                                WHERE PartitionName = \"pXX\"
                                """,
                                exist, 30, "target"))

    assertTrue(helper.checkShowTimesOf("""
                                SHOW PARTITIONS
                                FROM ${tableName}
                                WHERE PartitionName = \"pX11\"
                                """,
                                exist, 30, "target"))

    assertTrue(helper.checkShowTimesOf("""
                                SHOW PARTITIONS
                                FROM ${tableName}
                                WHERE PartitionName = \"p_2d122d12\"
                                """,
                                exist, 30, "target"))

    assertTrue(helper.checkShowTimesOf("""
                                SHOW PARTITIONS
                                FROM ${tableName}
                                WHERE PartitionName = \"p1111\"
                                """,
                                exist, 30, "target"))
}
