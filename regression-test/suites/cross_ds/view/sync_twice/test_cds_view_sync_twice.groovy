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

suite("test_cds_view_sync_twice") {
    def versions = sql_return_maparray "show variables like 'version_comment'"
    if (versions[0].Value.contains('doris-2.0.') || versions[0].Value.contains('doris-2.1.')) {
        logger.info("2.0/2.1 not support this case, current version is: ${versions[0].Value}")
        return
    }

    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def createDuplicateTable = { tableName ->
       sql """
            CREATE TABLE if NOT EXISTS ${tableName}
            (
                user_id            BIGINT       NOT NULL COMMENT "用户 ID",
                name               VARCHAR(20)           COMMENT "用户姓名",
                age                INT                   COMMENT "用户年龄"
            )
            ENGINE=OLAP
            DUPLICATE KEY(user_id)
            DISTRIBUTED BY HASH(user_id) BUCKETS 10
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "binlog.enable" = "true"
            )
        """
    }

    def exist = { res -> Boolean
        return res.size() != 0
    }
    def notExist = { res -> Boolean
        return res.size() == 0
    }

    def suffix = helper.randomSuffix()
    def tableDuplicate0 = "tbl_duplicate_0_${suffix}"
    createDuplicateTable(tableDuplicate0)
    sql """
        INSERT INTO ${tableDuplicate0} VALUES
        (1, "Emily", 25),
        (2, "Benjamin", 35),
        (3, "Olivia", 28),
        (4, "Alexander", 60),
        (5, "Ava", 17);
        """

    helper.enableDbBinlog()

    logger.info("=== Test1: create view ===")
    sql """
        CREATE VIEW view_test_${suffix} (k1, name,  v1)
        AS
        SELECT user_id as k1, name,  SUM(age) FROM ${tableDuplicate0}
        GROUP BY k1,name;
        """

    helper.ccrJobDelete()
    helper.ccrJobCreate()

    assertTrue(helper.checkRestoreFinishTimesOf("${tableDuplicate0}", 30))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableDuplicate0}", 5, 30))

    // the view will be restored again.
    logger.info("=== Test 2: delete job and create it again ===")
    test_num = 5
    helper.ccrJobDelete()

    sql """
        INSERT INTO ${tableDuplicate0} VALUES (6, "Zhangsan", 31)
        """
    sql "sync"

    num_restore = helper.getRestoreRowSize(tableDuplicate0)
    helper.ccrJobCreate()
    assertTrue(helper.checkRestoreNumAndFinishedTimesOf("${tableDuplicate0}", num_restore + 1, 30))

    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableDuplicate0}", 6, 50))
    def view_size = target_sql "SHOW VIEW FROM ${tableDuplicate0}"
    assertTrue(view_size.size() == 1);
}

