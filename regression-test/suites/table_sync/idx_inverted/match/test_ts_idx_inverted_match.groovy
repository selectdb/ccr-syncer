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
suite("test_ts_idx_inverted_match") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def tableName = "tbl_" + helper.randomSuffix()
    def test_num = 0
    def insert_num = 50

    def exist = { res -> Boolean
        return res.size() != 0
    }

    def has_count = { count ->
        return { res -> Boolean
            res.size() == count
        }
    }

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE if NOT EXISTS ${tableName}
        (
            `test` INT,
            `id` INT,
            `value` String,
            `value1` String,
            INDEX idx_inverted_1 (value) USING INVERTED PROPERTIES("parser" = "english")
        )
        ENGINE=OLAP
        DUPLICATE KEY(`test`, `id`)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
    """

    def values = [];
    for (int index = 0; index < insert_num; index++) {
        values.add("(${test_num}, ${index}, '${index} ${index*10} ${index*100}', '${index*12} ${index*14}')")
    }
    sql """
        INSERT INTO ${tableName} VALUES ${values.join(",")}
        """
    sql "sync"

    helper.ccrJobCreate(tableName)
    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 30))
    def indexes = target_sql_return_maparray "SHOW INDEXES FROM ${tableName}"
    logger.info("indexes is: ${indexes}")
    assertTrue(indexes.any { it.Key_name == "idx_inverted_1" && it.Index_type == "INVERTED" })

    try {
        target_sql "SET enable_match_without_inverted_index = false"
        def res = sql """SELECT * FROM ${tableName} WHERE value MATCH_ANY "10" """
        assertTrue(res.size() > 0)

        res = target_sql """ SELECT * FROM ${tableName} WHERE value MATCH_ANY "10" """
        assertTrue(res.size() > 0)

        sql """
            ALTER TABLE ${tableName}
            ADD INDEX idx_inverted_2(value1) USING INVERTED PROPERTIES("parser" = "english")
        """
        sql """
            BUILD INDEX idx_inverted_2 ON ${tableName}
        """

        assertTrue(helper.checkShowTimesOf("""
                                    SHOW BUILD INDEX FROM ${context.dbName}
                                    WHERE TableName = "${tableName}" AND State = "FINISHED"
                                    """,
                                    has_count(1), 30))

        sql """ INSERT INTO ${tableName} VALUES (100, 100, "100", "100") """
        assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName}", insert_num + 1, 30))

        indexes = target_sql_return_maparray "SHOW INDEXES FROM ${tableName}"
        logger.info("new indexes is: ${indexes}")
        assertTrue(indexes.any { it.Key_name == "idx_inverted_2" && it.Index_type == "INVERTED" })

        res = sql """ SELECT * FROM ${tableName} WHERE value1 MATCH_ANY "12" """
        assertTrue(res.size() > 0)

        res = target_sql """ SELECT * FROM ${tableName} WHERE value1 MATCH_ANY "12" """
        assertTrue(res.size() > 0)
    } finally {
        target_sql "SET enable_match_without_inverted_index = true"
    }
}
