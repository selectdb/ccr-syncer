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
suite("test_ds_col_alter_comment") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def dbName = context.dbName
    def dbNameTarget = "TEST_" + context.dbName
    def tableName = "tbl_" + helper.randomSuffix()
    def test_num = 0
    def insert_num = 5

    def exist = { res -> Boolean
        return res.size() != 0
    }

    def has_count = { count ->
        return { res -> Boolean
            res.size() == count
        }
    }

    def is_new_comment = { res -> Boolean
        return res[0][1].contains("`id` int NULL COMMENT \"new column comment\"")
    }

    helper.enableDbBinlog()
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
    target_sql "DROP TABLE IF EXISTS ${dbNameTarget}.${tableName}"

    sql """
        CREATE TABLE if NOT EXISTS ${tableName}
        (
            `test` INT,
            `id` INT COMMENT "old column comment",
            `value` INT
        )
        ENGINE=OLAP
        UNIQUE KEY(`test`, `id`)
        DISTRIBUTED BY HASH(test) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
    """

    def values = [];
    for (int index = 0; index < insert_num; index++) {
        values.add("(${test_num}, ${index}, ${index})")
    }
    sql """
        INSERT INTO ${tableName} VALUES ${values.join(",")}
        """
    sql "sync"

    helper.ccrJobDelete()
    helper.ccrJobCreate()

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 30))

    sql """
        ALTER TABLE ${tableName}
        MODIFY COLUMN `id` COMMENT "new column comment";
        """

    assertTrue(helper.checkShowTimesOf("SHOW CREATE TABLE `${tableName}`", is_new_comment, 30, "sql"))

    assertTrue(helper.checkShowTimesOf("SHOW CREATE TABLE `${tableName}`", is_new_comment, 30, "target_sql"))
}

