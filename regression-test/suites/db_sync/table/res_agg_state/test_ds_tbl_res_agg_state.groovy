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
suite("test_ds_tbl_res_agg_state") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def tableName = "tbl_" + helper.randomSuffix()
    def aggTableName = "agg_" + helper.randomSuffix()
    def test_num = 0
    def insert_num = 5

    def exist = { res -> Boolean
        return res.size() != 0
    }

    sql """
        CREATE TABLE if NOT EXISTS ${tableName}
        (
            `test` INT,
            `id` INT
        )
        ENGINE=OLAP
        UNIQUE KEY(`test`, `id`)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "store_row_column" = "true",
            "binlog.enable" = "true"
        )
    """

    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${tableName} VALUES (${test_num}, ${index})
            """
    }
    sql "sync"

    helper.enableDbBinlog()
    helper.ccrJobDelete()
    helper.ccrJobCreate()

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 30))

    logger.info("=== Test 1: Create table with agg state ===")
    sql """set enable_agg_state=true"""
    sql """
    create table ${aggTableName} (
        k1 int null,
        k2 agg_state<max_by(int not null,int)> generic,
        k3 agg_state<group_concat(string)> generic
    )
    aggregate key (k1)
    distributed BY hash(k1) buckets 3
    properties("replication_num" = "1");
    """

    assertTrue(helper.check_table_exists(aggTableName, 60))

    sql "insert into ${aggTableName} values(1,max_by_state(3,1),group_concat_state('a'))"
    sql "insert into ${aggTableName} values(1,max_by_state(2,2),group_concat_state('bb'))"
    sql "insert into ${aggTableName} values(2,max_by_state(1,3),group_concat_state('ccc'))"

    assertTrue(helper.checkSelectTimesOf("select * from ${aggTableName}", 2, 60))
}

