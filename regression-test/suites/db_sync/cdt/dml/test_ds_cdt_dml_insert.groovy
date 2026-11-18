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

suite("test_ds_cdt_dml_insert") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def dbName = context.dbName
    def tableName = "tbl_" + helper.randomSuffix()
    def test_num = 0
    def insert_num = 5

    def exist = { res -> Boolean
        return res.size() != 0
    }

    helper.enableDbBinlog()

    logger.info("=== Test 1: test json ===")

    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
    target_sql "DROP TABLE IF EXISTS TEST_${dbName}.${tableName}"
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            id INT,
            j JSON
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 10
        PROPERTIES(
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
        """

    helper.ccrJobDelete()
    helper.ccrJobCreate()

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 30))

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}\"", exist, 60, "sql"))

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}\"", exist, 60, "target"))

    sql """INSERT INTO ${tableName} VALUES(26, NULL)"""
    sql """INSERT INTO ${tableName} VALUES(27, '{"k1":"v1", "k2": 200}')"""
    sql """INSERT INTO ${tableName} VALUES(28, '{"a.b.c":{"k1.a1":"v31", "k2": 300},"a":"niu"}')"""
    sql """INSERT INTO ${tableName} VALUES(29, '12524337771678448270')"""
    sql """INSERT INTO ${tableName} VALUES(30, '-9223372036854775808')"""
    sql """INSERT INTO ${tableName} VALUES(31, '18446744073709551615')"""
    sql """INSERT INTO ${tableName} VALUES(32, '{"":"v1"}')"""
    sql """INSERT INTO ${tableName} VALUES(33, '{"":1, " ":"v1"}')"""
    sql """INSERT INTO ${tableName} VALUES(34, '{"":1, "ab":"v1", " ":"v1", "  ": 2}')"""

    assertTrue(helper.checkShowTimesOf("SELECT * FROM ${tableName} ORDER BY id", { r -> r.size() == 9}, 60, "target"))

    logger.info("=== Test 2: test variant ===")

    sql " DROP TABLE IF EXISTS ${tableName}_0"
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName}_0 (
            id INT,
            j VARIANT
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 10
        PROPERTIES(
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
        """
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}_0\"", exist, 60, "sql"))
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}_0\"", exist, 60, "target"))

    sql """insert into ${tableName}_0 values (1,  '[1]'),(1,  '{"a" : 1}');"""
    sql """insert into ${tableName}_0 values (2,  '[2]'),(1,  '{"a" : [[[1]]]}');"""
    sql """insert into ${tableName}_0 values (3,  '3'),(1,  '{"a" : 1}'), (1,  '{"a" : [1]}');"""
    sql """insert into ${tableName}_0 values (4,  '"4"'),(1,  '{"a" : "1223"}');"""
    sql """insert into ${tableName}_0 values (5,  '5'),(1,  '{"a" : [1]}');"""
    sql """insert into ${tableName}_0 values (6,  '"[6]"'),(1,  '{"a" : ["1", 2, 1.1]}');"""
    sql """insert into ${tableName}_0 values (7,  '7'),(1,  '{"a" : 1, "b" : {"c" : 1}}');"""
    sql """insert into ${tableName}_0 values (8,  '8.11111'),(1,  '{"a" : 1, "b" : {"c" : [{"a" : 1}]}}');"""
    sql """insert into ${tableName}_0 values (9,  '"9999"'),(1,  '{"a" : 1, "b" : {"c" : [{"a" : 1}]}}');"""
    sql """insert into ${tableName}_0 values (10,  '1000000'),(1,  '{"a" : 1, "b" : {"c" : [{"a" : 1}]}}');"""
    sql """insert into ${tableName}_0 values (11,  '[123.1]'),(1999,  '{"a" : 1, "b" : {"c" : 1}}'),(19921,  '{"a" : 1, "b" : 10}');"""
    sql """insert into ${tableName}_0 values (12,  '[123.2]'),(1022,  '{"a" : 1, "b" : 10}'),(1029,  '{"a" : 1, "b" : {"c" : 1}}');"""

    assertTrue(helper.checkShowTimesOf("SELECT * FROM ${tableName}_0 ORDER BY id", { r -> r.size() == 27}, 60, "target"))

    logger.info("=== Test 3: test struct ===")

    sql "DROP TABLE IF EXISTS ${tableName}_1"
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName}_1 (
        `k1` INT(11) NULL,
        `k2` STRUCT<f1:BOOLEAN,f2:TINYINT,f3:SMALLINT,f4:INT,f5:INT,f6:BIGINT,f7:LARGEINT> NULL,
        `k3` STRUCT<f1:FLOAT,f2:DOUBLE,f3:DECIMAL(10,3)> NULL,
        `k4` STRUCT<f1:DATE,f2:DATETIME,f3:DATEV2,f4:DATETIMEV2> NULL,
        `k5` STRUCT<f1:CHAR(10),f2:VARCHAR(10),f3:STRING> NOT NULL
        )
        DUPLICATE KEY(`k1`)
        DISTRIBUTED BY HASH(`k1`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
    """
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}_1\"", exist, 60, "sql"))
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}_1\"", exist, 60, "target"))

    sql "INSERT INTO ${tableName}_1 VALUES(1, {1,11,111,1111,11111,11111,111111},null,null,{'','',''})"
    sql "INSERT INTO ${tableName}_1 VALUES(2, {null,null,null,null,null,null,null},{2.1,2.22,2.333},null,{null,null,null})"
    sql "INSERT INTO ${tableName}_1 VALUES(3, null,{null,null,null},{'2023-02-23','2023-02-23 00:10:19','2023-02-23','2023-02-23 00:10:19'},{'','',''})"
    sql "INSERT INTO ${tableName}_1 VALUES(4, null,null,{null,null,null,null},{'abc','def','hij'})"

    assertTrue(helper.checkShowTimesOf("SELECT * FROM ${tableName}_1 ORDER BY k1", { r -> r.size() == 4}, 60, "target"))

    logger.info("=== Test 4: test array ===")

    sql "DROP TABLE IF EXISTS ${tableName}_2"
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName}_2 (
            `k1` INT(11) NULL COMMENT "",
            `k2` ARRAY<CHAR(5)> NULL COMMENT "",
            `k3` ARRAY<CHAR(5)> NOT NULL COMMENT "",
            `k4` ARRAY<ARRAY<CHAR(5)>> NULL COMMENT ""
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        DISTRIBUTED BY HASH(`k1`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true",
            "storage_format" = "V2"
        )
    """

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}_2\"", exist, 60, "sql"))
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}_2\"", exist, 60, "target"))

    sql "INSERT INTO ${tableName}_2 VALUES (4, ['12345','123'], ['4'], NULL)"
    sql "INSERT INTO ${tableName}_2 VALUES (5, NULL, [NULL, '4'], NULL)"
    sql "INSERT INTO ${tableName}_2 VALUES (6, NULL, ['4'], [['123'],['222']])"
    sql "INSERT INTO ${tableName}_2 VALUES (7, NULL, ['4'], [['12345',NULL],['222']])"

    assertTrue(helper.checkShowTimesOf("SELECT * FROM ${tableName}_2 ORDER BY k1", { r -> r.size() == 4}, 60, "target"))

    logger.info("=== Test 5: test array ===")

    sql "DROP TABLE IF EXISTS ${tableName}_3"
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName}_3
        (
            `id` INT NULL,
            `name` TEXT NULL,
            `score` MAP<TEXT,INT> NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        );
    """

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}_3\"", exist, 60, "sql"))
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}_3\"", exist, 60, "target"))

    sql """ insert into ${tableName}_3 values (0, "zhangsan", {"Chinese":"80","Math":"60","English":"90"}); """
    sql """ insert into ${tableName}_3 values (1, "lisi", {"null":null}); """
    sql """ insert into ${tableName}_3 values (2, "wangwu", {"Chinese":"88","Math":"90","English":"96"}); """
    sql """ insert into ${tableName}_3 values (3, "lisi2", {null:null}); """
    sql """ insert into ${tableName}_3 values (4, "amory", NULL); """

    assertTrue(helper.checkShowTimesOf("SELECT * FROM ${tableName}_3 ORDER BY id", { r -> r.size() == 5}, 60, "target"))

    logger.info("=== Test 6: test array ===")

    sql "DROP TABLE IF EXISTS ${tableName}_4"
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName}_4
        (
            k int,
            id_bitmap bitmap
        )
        DISTRIBUTED BY HASH(`k`) BUCKETS AUTO
        PROPERTIES
        (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        );
    """

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}_4\"", exist, 60, "sql"))
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}_4\"", exist, 60, "target"))

    sql "insert into ${tableName}_4 values(1,to_bitmap(1));"
    sql "insert into ${tableName}_4 values(2,bitmap_or(to_bitmap(3),to_bitmap(1000)));"
    sql "insert into ${tableName}_4 values(3,bitmap_or(to_bitmap(999),to_bitmap(1000),to_bitmap(888888)));"
    sql "insert into ${tableName}_4 values(4,bitmap_from_string('1,0,1,2,3,1,5,99,876,2445'));"
    sql "insert into ${tableName}_4 values(5,bitmap_or(bitmap_from_string('90,5,876'),to_bitmap(1000)));"

    assertTrue(helper.checkShowTimesOf("SELECT * FROM ${tableName}_4", { r -> r.size() == 5}, 60, "target"))

    logger.info("=== Test 7: test HLL ===")

    sql "DROP TABLE IF EXISTS ${tableName}_5"
    sql """
        CREATE TABLE ${tableName}_5
        (
            `k1` LARGEINT NOT NULL,
            `v_hll` HLL HLL_UNION NOT NULL
        ) ENGINE=OLAP
        AGGREGATE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY RANDOM BUCKETS 10
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}_5\"", exist, 60, "sql"))
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}_5\"", exist, 60, "target"))

    sql """ insert into ${tableName}_5 values(1,hll_hash(1)) """
    sql """ insert into ${tableName}_5 values(1,hll_hash(2)) """
    sql """ insert into ${tableName}_5 values(1,hll_hash(3)) """
    sql """ insert into ${tableName}_5 values(2,hll_hash(4)) """
    sql """ insert into ${tableName}_5 values(2,hll_hash(5)) """
    sql """ insert into ${tableName}_5 values(2,hll_hash(6)) """

    assertTrue(helper.checkShowTimesOf("SELECT * FROM ${tableName}_5", { r -> r.size() == 2}, 60, "target"))
}
