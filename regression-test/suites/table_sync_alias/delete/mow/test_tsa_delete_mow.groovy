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

suite('test_tsa_delete_mow') {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", 'helper.groovy'))

    def tableName = 'tbl_' + helper.randomSuffix()
    def aliasTableName = 'alias_' + helper.randomSuffix()
    helper.set_alias(aliasTableName)
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
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true",
            "binlog.ttl_seconds" = "180",
            "enable_unique_key_merge_on_write" = "true"
        )
    """
    helper.ccrJobDelete(tableName)
    helper.ccrJobCreate(tableName)

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 60))
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${aliasTableName}\"", { res -> res.size() == 1 }, 60, 'target'))

    for (int i = 0; i < 10; i++) {
        sql """ INSERT INTO ${tableName} VALUES (${i}, ${i}) """
    }
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${aliasTableName}", 10, 60))

    sql """ DELETE FROM ${tableName} 
            PARTITION `p1`
            WHERE test < 5    
    """

    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${aliasTableName}", 5, 60))
}
