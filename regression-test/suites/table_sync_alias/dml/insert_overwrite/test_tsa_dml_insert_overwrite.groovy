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

suite('test_tsa_dml_insert_overwrite') {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", 'helper.groovy'))

    def baseTableName = 'test_' + helper.randomSuffix()
    def aliasBaseTableName = 'test_alias_' + helper.randomSuffix()
    def exist = { res -> Boolean
        return res.size() != 0
    }
    def notExist = { res -> Boolean
        return res.size() == 0
    }

    tableName = "${baseTableName}_unpart"
    aliasTableName = "${aliasBaseTableName}_unpart"
    helper.set_alias(aliasTableName)
    sql """
        CREATE TABLE if NOT EXISTS ${tableName}
        (
            `test` INT,
            `id` INT NOT NULL
        )
        ENGINE=OLAP
        UNIQUE KEY(`test`, `id`)
        DISTRIBUTED BY HASH(id) BUCKETS AUTO
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
    """

    helper.ccrJobCreate(tableName)

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 60))

    def versions = sql_return_maparray "show variables like 'version_comment'"
    if (versions[0].Value.contains('doris-2.0.')) {
        logger.info("2.0 not support INSERT OVERWRITE yet, current version is: ${versions[0].Value}")
        return
    }

    sql """
        INSERT OVERWRITE TABLE ${tableName} VALUES (1, 100);
       """

    assertTrue(helper.checkShowTimesOf("""
                                SELECT * FROM ${aliasTableName}
                                WHERE id = 100
                                """,
                                exist, 60, 'target'))

    sql """
        INSERT INTO ${tableName} VALUES (1, 200);
       """
    assertTrue(helper.checkSelectTimesOf(""" SELECT * FROM ${aliasTableName} """, 1, 60))
}
