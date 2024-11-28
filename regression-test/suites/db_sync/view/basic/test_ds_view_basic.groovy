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

suite("test_ds_view_basic") {
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

    def checkRestoreRowsTimesOf = {rowSize, times -> Boolean
        Boolean ret = false
        while (times > 0) {
            def sqlInfo = target_sql "SHOW RESTORE FROM TEST_${context.dbName}"
            if (sqlInfo.size() == rowSize) {
                ret = true
                break
            } else if (--times > 0 && sqlInfo.size < rowSize) {
                sleep(sync_gap_time)
            }
        }

        return ret
    }
    def checkTableOrViewExists = { res, name -> Boolean
        for (List<Object> row : res) {
            if ("${row[0]}".equals(name)) {
                return true
            }
        }
        return false
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

    sql "ALTER DATABASE ${context.dbName} SET properties (\"binlog.enable\" = \"true\")"

    helper.ccrJobDelete()
    helper.ccrJobCreate()

    assertTrue(helper.checkRestoreFinishTimesOf("${tableDuplicate0}", 30))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableDuplicate0}", 5, 30))

    logger.info("=== Test1: create view and materialized view ===")
    sql """
        CREATE VIEW view_test_${suffix} (k1, name,  v1)
        AS
        SELECT user_id as k1, name,  SUM(age) FROM ${tableDuplicate0}
        GROUP BY k1,name;
        """

    sql """
        create materialized view user_id_name_${suffix} as
        select user_id, name from ${tableDuplicate0};
        """

    def checkViewExistFunc = { res -> Boolean
        return checkTableOrViewExists(res, "view_test_${suffix}")
    }
    assertTrue(helper.checkShowTimesOf("SHOW VIEWS",
        checkViewExistFunc, 30, func = "target_sql"))

    explain {
        sql("select user_id, name from ${tableDuplicate0}")
        contains "user_id_name"
    }

    logger.info("=== Test 2: drop view ===")
    sql "DROP VIEW view_test_${suffix}"
    sql "sync"
    def checkViewNotExistFunc = { res -> Boolean
        return !checkTableOrViewExists(res, "view_test_${suffix}")
    }
    assertTrue(helper.checkShowTimesOf("SHOW VIEWS", checkViewNotExistFunc, 30, func = "target_sql"))

     logger.info("=== Test 2: delete job ===")
     test_num = 5
     helper.ccrJobDelete()

   sql """
        INSERT INTO ${tableDuplicate0} VALUES (6, "Zhangsan", 31)
        """

    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableDuplicate0}", 5, 5))
}
