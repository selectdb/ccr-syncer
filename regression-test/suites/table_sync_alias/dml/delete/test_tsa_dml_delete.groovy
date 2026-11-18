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

suite("test_tsa_dml_delete") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def tableName = "tbl_" + helper.randomSuffix()
    def aliasTableName = "alias_tbl_" + helper.randomSuffix()
    def test_num = 0
    def insert_num = 29

    helper.set_alias(aliasTableName)

    sql "DROP TABLE IF EXISTS ${tableName}"
    target_sql "DROP TABLE IF EXISTS ${aliasTableName}"

    sql """
        CREATE TABLE if NOT EXISTS ${tableName} 
        (
            col_1 tinyint,
            col_2 smallint,
            col_3 int,
            col_4 bigint,
            col_5 decimal(10,3),
            col_6 char,
            col_7 varchar(20),
            col_9 date,
            col_10 datetime,
            col_11 boolean,
            col_8 string,
        ) ENGINE=OLAP
        duplicate KEY(`col_1`, col_2, col_3, col_4, col_5, col_6, col_7,  col_9, col_10, col_11)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`col_1`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
    """
    sql """ALTER TABLE ${tableName} set ("binlog.enable" = "true")"""

    helper.ccrJobDelete(tableName)
    helper.ccrJobCreate(tableName)

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 30))


    logger.info("=== Test 0: Common insert case ===")
  
    sql """
        INSERT INTO ${tableName} VALUES 
        (1, 2, 3, 4, 11.22, 'a', 'b', '2023-01-01', '2023-01-01 00:01:02', true, 'aaa'),
        (2, 3, 4, 5, 22.33, 'b', 'c', '2023-01-02', '2023-01-02 00:01:02', false, 'bbb'),
        (3, 4, 5, 6, 33.44, 'c', 'd', '2023-01-03', '2023-01-03 00:01:02', true, 'ccc'),
        (4, 5, 6, 7, 44.55, 'd', 'e', '2023-01-04', '2023-01-04 00:01:02', false, 'ddd'),
        (5, 6, 7, 8, 55.66, 'e', 'f', '2023-01-05', '2023-01-05 00:01:02', true, 'eee'),
        (6, 7, 8, 9, 66.77, 'f', 'g', '2023-01-06', '2023-01-06 00:01:02', false, 'fff'),
        (7, 8, 9, 10, 77.88, 'g', 'h', '2023-01-07', '2023-01-07 00:01:02', true, 'ggg'),
        (8, 9, 10, 11, 88.99, 'h', 'i', '2023-01-08', '2023-01-08 00:01:02', false, 'hhh'),
        (9, 10, 11, 12, 99.1, 'i', 'j', '2023-01-09', '2023-01-09 00:01:02', true, 'iii'),
        (10, 11, 12, 13, 101.2, 'j', 'k', '2023-01-10', '2023-01-10 00:01:02', false, 'jjj'),
        (11, 12, 13, 14, 102.2, 'l', 'k', '2023-01-11', '2023-01-11 00:01:02', true, 'kkk'),
        (12, 13, 14, 15, 103.2, 'm', 'l', '2023-01-12', '2023-01-12 00:01:02', false, 'lll'),
        (13, 14, 15, 16, 104.2, 'n', 'm', '2023-01-13', '2023-01-13 00:01:02', true, 'mmm'),
        (14, 15, 16, 17, 105.2, 'o', 'n', '2023-01-14', '2023-01-14 00:01:02', false, 'nnn'),
        (15, 16, 17, 18, 106.2, 'p', 'o', '2023-01-15', '2023-01-15 00:01:02', true, 'ooo'),
        (15, 16, 17, 18, 106.2, 'q', 'p', '2023-01-16', '2023-01-16 00:01:02', false, 'ppp'),
        (16, 17, 18, 19, 107.2, 'r', 'q', '2023-01-17', '2023-01-17 00:01:02', true, 'qqq'),
        (17, 18, 19, 20, 108.2, 's', 'r', '2023-01-18', '2023-01-18 00:01:02', false, 'rrr'),
        (18, 19, 20, 21, 109.2, 't', 's', '2023-01-19', '2023-01-19 00:01:02', true, 'sss'),
        (19, 20, 21, 22, 110.2, 'v', 't', '2023-01-20', '2023-01-20 00:01:02', false, 'ttt'),
        (20, 21, 22, 23, 111.2, 'u', 'u', '2023-01-21', '2023-01-21 00:01:02', true, 'uuu'),
        (21, 22, 23, 24, 112.2, 'w', 'v', '2023-01-22', '2023-01-22 00:01:02', false, 'vvv'),
        (22, 23, 24, 25, 113.2, 'x', 'w', '2023-01-23', '2023-01-23 00:01:02', true, 'www'),
        (23, 24, 25, 26, 114.2, 'y', 'x', '2023-01-24', '2023-01-24 00:01:02', false, 'xxx'),
        (24, 25, 26, 27, 115.2, 'z', 'y', '2023-01-25', '2023-01-25 00:01:02', true, 'yyy'),
        (25, 26, 27, 28, 116.2, 'a', 'z', '2023-01-26', '2023-01-26 00:01:02', false, 'zzz'),
        (26, 27, 28, 29, 117.2, 'b', 'a', '2023-01-27', '2023-01-27 00:01:02', true, 'aaa'),
        (27, 28, 29, 30, 118.2, 'c', 'b', '2023-01-28', '2023-01-28 00:01:02', false, 'bbb'),
        (28, 29, 30, 31, 119.2, 'd', 'c', '2023-01-29', '2023-01-29 00:01:02', true, 'ccc')

        """
    sql "sync"
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${aliasTableName}",
                                  insert_num, 30))

    logger.info("=== Test 1: delete row case ===")
    sql "DELETE FROM ${tableName} WHERE col_1 = 1"
    sql "DELETE FROM ${tableName} WHERE col_2 = 3"
    sql "DELETE FROM ${tableName} WHERE col_3 = 5"
    sql "DELETE FROM ${tableName} WHERE col_4 = 7"
    sql "DELETE FROM ${tableName} WHERE col_5 = 55.66"
    sql "DELETE FROM ${tableName} WHERE col_6 = 'f'"
    sql "DELETE FROM ${tableName} WHERE col_7 = 'h'"
    sql "DELETE FROM ${tableName} WHERE col_8 = 'hhh'"
    sql "DELETE FROM ${tableName} WHERE col_9 = '2023-01-09'"
    sql "DELETE FROM ${tableName} WHERE col_10 = '2023-01-10 00:01:02'"
    sql "DELETE FROM ${tableName} WHERE col_11 = true"
    sql "DELETE FROM ${tableName} WHERE col_1 >= 27"
    sql "DELETE FROM ${tableName} WHERE col_1 != 26 and col_1 != 25 and col_1 != 24 and col_1 != 23"


    // 'select test from TEST_${context.dbName}.${tableName}' should return 2 rows
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM TEST_${context.dbName}.${aliasTableName}", 2, 30))

}
