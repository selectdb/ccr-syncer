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

suite("test_ds_tbl_create_index") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def dbName = context.dbName
    def tableNameFull = "tbl_full"
    def tableNameIndex = "tbl_index"
    def test_num = 0
    def insert_num = 5

    def exist = { res -> Boolean
        return res.size() != 0
    }

    def origin_query_index_id = { indexName ->
        def res = sql_return_maparray "SHOW TABLETS FROM ${dbName}.${tableNameIndex}"
        def tabletId = res[0].TabletId
        res = sql_return_maparray "SHOW TABLET ${tabletId}"
        def dbId = res[0].DbId
        def tableId = res[0].TableId
        res = sql_return_maparray """ SHOW PROC "/dbs/${dbId}/${tableId}/indexes" """
        for (def record in res) {
            if (record.KeyName == indexName) {
                return record.IndexId
            }
        }
        throw new Exception("index ${indexName} is not exists")
    }

    def target_query_index_id = { indexName ->
        def res = target_sql_return_maparray "SHOW TABLETS FROM TEST_${dbName}.${tableNameIndex}"
        def tabletId = res[0].TabletId
        res = target_sql_return_maparray "SHOW TABLET ${tabletId}"
        def dbId = res[0].DbId
        def tableId = res[0].TableId
        res = target_sql_return_maparray """ SHOW PROC "/dbs/${dbId}/${tableId}/indexes" """
        for (def record in res) {
            if (record.KeyName == indexName) {
                return record.IndexId
            }
        }
        throw new Exception("index ${indexName} is not exists")
    }
    try {
        sql """ ADMIN SET FRONTEND CONFIG ("restore_reset_index_id" = "false") """
        target_sql """ ADMIN SET FRONTEND CONFIG ("restore_reset_index_id" = "false") """

        sql "DROP TABLE IF EXISTS ${dbName}.${tableNameFull}"
        target_sql "DROP TABLE IF EXISTS TEST_${dbName}.${tableNameFull}"
        sql "DROP TABLE IF EXISTS ${dbName}.${tableNameIndex}"
        target_sql "DROP TABLE IF EXISTS TEST_${dbName}.${tableNameIndex}"


        helper.enableDbBinlog()
        helper.ccrJobDelete()
        helper.ccrJobCreate()

        logger.info("=== Test 1: create table full sync ===")

        sql """
            CREATE TABLE if NOT EXISTS ${tableNameFull}
            (
                `id` LARGEINT NOT NULL,
                `test` STRING NOT NULL,
                `value` STRING DEFAULT "",
                INDEX `idx_value` (`value`) USING INVERTED PROPERTIES ("parser" = "english")
            )
            ENGINE=OLAP
            UNIQUE KEY(`id`)
            PARTITION BY RANGE(`id`)
            (   
                PARTITION p1 VALUES LESS THAN ("10"),
                PARTITION p2 VALUES LESS THAN ("20"),
                PARTITION p3 VALUES LESS THAN ("30")
            )
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "binlog.enable" = "true"
            )
        """

        assertTrue(helper.checkRestoreFinishTimesOf("${tableNameFull}", 30))

        assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableNameFull}\"", exist, 60, "sql"))

        assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableNameFull}\"", exist, 60, "target"))

        logger.info("=== Test 2: create table partial sync ===")

        sql """
            CREATE TABLE if NOT EXISTS ${tableNameIndex}
            (
                `id` LARGEINT NOT NULL,
                `test` STRING NOT NULL,
                `value` STRING DEFAULT "",
                INDEX `idx_value` (`value`) USING INVERTED PROPERTIES ("parser" = "english")
            )
            ENGINE=OLAP
            UNIQUE KEY(`id`)
            PARTITION BY RANGE(`id`)
            (
                PARTITION p1 VALUES LESS THAN ("10"),
                PARTITION p2 VALUES LESS THAN ("20"),
                PARTITION p3 VALUES LESS THAN ("30"),
                PARTITION p4 VALUES LESS THAN ("40"),
                PARTITION p5 VALUES LESS THAN ("50"),
                PARTITION p6 VALUES LESS THAN ("60"),
                PARTITION p7 VALUES LESS THAN ("70")
            )
            DISTRIBUTED BY HASH(id) BUCKETS 2
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "binlog.enable" = "true"
            )
        """

        List<String> values = []
        int numRows = 6;
        for (int j = 0; j <= numRows; ++j) {
            values.add("(${j}1, \"${j} ${j*10} ${j*100}\", \"${j*11} ${j*12}\")")
        }
        sql "INSERT INTO ${tableNameIndex} VALUES ${values.join(",")}"

        assertTrue(helper.checkRestoreFinishTimesOf("${tableNameIndex}", 30))

        assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableNameIndex}\"", exist, 60, "sql"))

        assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableNameIndex}\"", exist, 60, "target"))

        assertTrue(helper.checkShowTimesOf("select * from ${tableNameIndex}", exist, 60, "sql"))
        
        assertTrue(helper.checkShowTimesOf("select * from ${tableNameIndex}", exist, 60, "target"))

        logger.info("=== Test 3: check inverted index id ===")

        def originIndexId = origin_query_index_id("idx_value")
        logger.info("the exists origin index id is ${originIndexId}")

        def targetIndexId = target_query_index_id("idx_value")
        logger.info("the exists target index id is ${targetIndexId}")

        assertEquals(originIndexId, targetIndexId)

        sql """ set enable_match_without_inverted_index = false """
        def res = sql """ SELECT /*+ SET_VAR(inverted_index_skip_threshold = 0, enable_common_expr_pushdown = true) */ * FROM ${tableNameIndex} WHERE value MATCH_ANY "11" """
        logger.info(res[0][1])
        assertTrue(res.size() > 0)

        target_sql """ set enable_match_without_inverted_index = false """
        res = target_sql """ SELECT /*+ SET_VAR(inverted_index_skip_threshold = 0, enable_common_expr_pushdown = true) */ * FROM ${tableNameIndex} WHERE value MATCH_ANY "11" """
        logger.info(res[0][1])
        assertTrue(res.size() > 0)

    } finally {
        target_sql """ set enable_match_without_inverted_index = true """
        sql """ set enable_match_without_inverted_index = true """
        target_sql """ ADMIN SET FRONTEND CONFIG ("restore_reset_index_id" = "true") """
        sql """ ADMIN SET FRONTEND CONFIG ("restore_reset_index_id" = "true") """
    }
}
