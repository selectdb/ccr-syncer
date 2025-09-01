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
suite("test_ds_tbl_drop_recover2") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    if (!helper.is_version_supported([30099, 20199, 20099])) {
        // not support doris 3.0/2.1/2.0
        def version = helper.upstream_version()
        logger.info("skip this suite because version is not supported, upstream version ${version}")
        return
    }

    def tableName = "tbl_recover" + helper.randomSuffix()
    def test_num = 0
    def insert_num = 3
    def opPartitonName = "less"

    def exist = { res -> Boolean
        return res.size() != 0
    }
    def notExist = { res -> Boolean
        return res.size() == 0
    }

    helper.ccrJobDelete()

    sql """
        CREATE TABLE if NOT EXISTS ${tableName}_1
        (
            `test` INT,
            `id` INT
        )
        ENGINE=OLAP
        UNIQUE KEY(`test`, `id`)
        PARTITION BY RANGE(`id`)
        (
            PARTITION `${opPartitonName}_0` VALUES LESS THAN ("0"),
            PARTITION `${opPartitonName}_1` VALUES LESS THAN ("1000")
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
    """



    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${tableName}_1 VALUES (${test_num}, ${index})
            """
    }

    sql """
    DROP TABLE ${tableName}_1
    """
    helper.enableDbBinlog()
    helper.ccrJobCreate()
    int interations = 10;
    for(int t = 0; t <= interations; t += 1){
        /* first iteration already deleted */
        sql """
        DROP TABLE if exists ${tableName}_1
        """

        assertTrue(helper.checkShowTimesOf(""" SHOW TABLES LIKE "${tableName}_1" """, notExist, 60, "sql"))    // check recovered in local
        assertTrue(helper.checkShowTimesOf(""" SHOW TABLES LIKE "${tableName}_1" """, notExist, 60, "target"))

        sql """
        RECOVER TABLE ${tableName}_1
        """    
        assertTrue(helper.checkShowTimesOf(""" SHOW TABLES LIKE "${tableName}_1" """, exist, 60, "sql"))    // check recovered in local
        assertTrue(helper.checkShowTimesOf(""" SHOW TABLES LIKE "${tableName}_1" """, exist, 60, "target")) // check recovered in target

        assertTrue(helper.checkRestoreFinishTimesOf("${tableName}_1", 60))

        test_num = t + 10;
        for (int index = 0; index < insert_num; index++) {
            sql """
                INSERT INTO ${tableName}_1 VALUES (${test_num}, ${index})
                """
        }
        // need check restore,
        assertTrue(helper.checkRestoreFinishTimesOf("${tableName}_1", 60))
        // check in remote available.
        assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName}_1 WHERE test=${test_num}",
                                    insert_num, 30))

    }
    order_qt_target_sql_content_2("SELECT * FROM ${tableName}_1")
    qt_sql_source_content_2("SELECT * FROM ${tableName}_1")
}
