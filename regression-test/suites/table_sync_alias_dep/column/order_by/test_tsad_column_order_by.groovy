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
suite("test_tsad_column_order_by") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def tableName = "tbl_" + helper.randomSuffix()
    def aliasTableName = "alias_tbl_" + helper.randomSuffix()
    def test_num = 0
    def insert_num = 5

    helper.set_alias(aliasTableName)

    def exist = { res -> Boolean
        return res.size() != 0
    }

    def has_count = { count ->
        return { res -> Boolean
            res.size() == count
        }
    }

    helper.enableDbBinlog()
    sql "DROP TABLE IF EXISTS ${tableName}"
    target_sql "DROP TABLE IF EXISTS ${aliasTableName}"
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
            "binlog.enable" = "true"
        )
    """

    helper.enableDbBinlog()
    helper.ccrJobDelete(tableName)
    helper.ccrJobCreate(tableName)

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 30))

    assertTrue(helper.checkShowTimesOf(""" SHOW TABLES LIKE "${aliasTableName}" """, exist, 60, "target"))

    // 1. Pause ccr job
    helper.ccrJobPause(tableName)

    // 2. Insert N data
    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${tableName} VALUES (${test_num}, ${index})
            """
    }

    assertTrue(helper.checkShowTimesOf(""" select * from ${tableName} """, { r -> r.size() == insert_num }, 60, "sql"))

    assertTrue(helper.checkShowTimesOf(""" show columns from ${tableName} """, { r -> return r[0][0] == 'test' }, 60, "sql"))
    assertTrue(helper.checkShowTimesOf(""" show columns from ${tableName} """, { r -> return r[1][0] == 'id' }, 60, "sql"))

    // 3. Do operation & wait it finishes upstream
    sql """
        ALTER TABLE ${tableName} ORDER BY (`id`, `test`)
        """

    assertTrue(helper.checkShowTimesOf(""" show columns from ${tableName} """, { r -> return r[0][0] == 'id' }, 60, "sql"))
    assertTrue(helper.checkShowTimesOf(""" show columns from ${tableName} """, { r -> return r[1][0] == 'test' }, 60, "sql"))


    // 4. Insert N data
    for (int index = insert_num; index < insert_num * 2; index++) {
        sql """
            INSERT INTO ${tableName} VALUES (${index}, ${test_num})
            """
    }

    assertTrue(helper.checkShowTimesOf(""" select * from ${tableName} """, { r -> r.size() == insert_num * 2 }, 60, "sql"))

    // 5. Resume ccr job
    helper.ccrJobResume(tableName)

    // 6. Verify data and operation are synced downstream
    assertTrue(helper.checkShowTimesOf(""" select * from ${aliasTableName} """, { r -> r.size() == insert_num * 2 }, 60, "target"))
    assertTrue(helper.checkShowTimesOf(""" show columns from ${aliasTableName} """, { r -> return r[0][0] == 'id' }, 60, "target"))
    assertTrue(helper.checkShowTimesOf(""" show columns from ${aliasTableName} """, { r -> return r[1][0] == 'test' }, 60, "target"))
}

