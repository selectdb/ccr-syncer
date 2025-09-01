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

suite("test_ds_view_drop_create") {
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
    sql """ DROP VIEW IF EXISTS view_test_${suffix} """
    sql """ DROP VIEW IF EXISTS view_test_1_${suffix} """
    createDuplicateTable(tableDuplicate0)
    sql """
        INSERT INTO ${tableDuplicate0} VALUES
        (1, "Emily", 25),
        (2, "Benjamin", 35),
        (3, "Olivia", 28),
        (4, "Alexander", 60),
        (5, "Ava", 17),
        (5, "Ava", 18);
        """

    sql "ALTER DATABASE ${context.dbName} SET properties (\"binlog.enable\" = \"true\")"

    logger.info("=== Test1: create view ===")
    sql """
        CREATE VIEW view_test_${suffix} (k1, name,  v1)
        AS
        SELECT user_id as k1, name, SUM(age) FROM ${tableDuplicate0}
        GROUP BY k1,name;
        """

    helper.ccrJobDelete()
    helper.ccrJobCreate()

    assertTrue(helper.checkRestoreFinishTimesOf("${tableDuplicate0}", 30))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableDuplicate0}", 6, 30))

    // drop the view, and create it again.
    // Must be incremental sync.
    sql """
        DROP VIEW view_test_${suffix}
    """

    sql """
        CREATE VIEW view_test_${suffix} (k1, name,  v1)
        AS
        SELECT user_id as k1, name,  SUM(age) FROM ${tableDuplicate0}
        GROUP BY k1,name;
    """

    // Since create view is synced to downstream, this insert will be sync too.
    sql """
        INSERT INTO ${tableDuplicate0} VALUES
            (6, "Zhangsan", 31),
            (5, "Ava", 20);
        """
    sql "sync"

    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableDuplicate0}", 8, 50))
    def view_size = target_sql "SHOW VIEW FROM ${tableDuplicate0}"
    assertTrue(view_size.size() == 1);

    // pause and create again, so create view will query the upstream to found table name.
    helper.ccrJobPause()

    sql """
        CREATE VIEW view_test_1_${suffix} (k1, name,  v1)
        AS
        SELECT user_id as k1, name,  SUM(age) FROM ${tableDuplicate0}
        GROUP BY k1,name;
    """
    sql """ DROP VIEW view_test_1_${suffix} """

    helper.ccrJobResume()

    // insert will be sync.
    sql """
        INSERT INTO ${tableDuplicate0} VALUES
            (6, "Zhangsan", 31),
            (5, "Ava", 20);
        """
    sql "sync"

    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableDuplicate0}", 10, 50))
    view_size = target_sql "SHOW VIEW FROM ${tableDuplicate0}"
    assertTrue(view_size.size() == 1);

}

