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

suite("test_ts_absorb_view_create") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def tableName = "tbl_" + helper.randomSuffix()
    def viewName = "view_test" + helper.randomSuffix()

    def exist = { res -> Boolean
        return res.size() != 0
    }
    def notExist = { res -> Boolean
        return res.size() == 0
    }

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

    helper.enableDbBinlog()
    helper.ccrJobDelete(tableName)
    helper.ccrJobCreate(tableName)

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 30))
    assertTrue(helper.checkShowTimesOf(""" SHOW TABLES LIKE "${tableName}" """, exist, 60, "target"))
    // 0. Insert N data
    sql """
        INSERT INTO ${tableName} VALUES
        (1, "Emily", 25),
        (2, "Benjamin", 35),
        (3, "Olivia", 28),
        (4, "Alexander", 60),
        (5, "Ava", 17);
        """
    assertTrue(helper.checkShowTimesOf(""" select * from ${tableName} """, { r -> r.size() == 5 }, 60, "sql"))
    assertTrue(helper.checkShowTimesOf(""" select * from ${tableName} """, { r -> r.size() == 5 }, 60, "target"))
    // 1. Pause ccr job
    helper.ccrJobPause(tableName)

    // 2. Insert N data
    sql """
        INSERT INTO ${tableName} VALUES
        (6, "John", 10),
        (7, "Jack", 11),
        (8, "Tom", 12),
        (9, "Alias", 13),
        (10, "Doris", 14);
        """
    assertTrue(helper.checkShowTimesOf(""" select * from ${tableName} """, { r -> r.size() == 10 }, 60, "sql"))


    // 3. Do operation & wait it finishes upstream
    sql """
        CREATE VIEW ${viewName} (k1, name,  v1)
        AS
        SELECT user_id as k1, name, SUM(age) FROM ${tableName}
        GROUP BY k1,name;
        """
    assertTrue(helper.checkShowTimesOf(""" SHOW VIEWS LIKE "${viewName}" """, exist, 60, "sql"))
    assertTrue(helper.checkShowTimesOf(""" select * from ${viewName} """, { r -> r.size() == 10 }, 60, "sql"))

    // 4. Insert N data
    sql """
        INSERT INTO ${tableName} VALUES
        (11, "Windows", 10),
        (12, "Android", 11),
        (13, "IOS", 12),
        (14, "MacOS", 13),
        (15, "Linux", 14);
        """
    assertTrue(helper.checkShowTimesOf(""" select * from ${tableName} """, { r -> r.size() == 15 }, 60, "sql"))

    // 5. Force trigger fullsnapshot
    helper.force_fullsync(tableName)

    // 6. Resume ccr job
    helper.ccrJobResume(tableName)
  
    // 7. Verify data and operation are synced downstream
    assertTrue(helper.checkShowTimesOf(""" select * from ${tableName} """, { r -> r.size() == 15}, 60, "target"))
    assertTrue(helper.checkShowTimesOf(""" SHOW VIEWS LIKE "${viewName}" """, exist, 60, "target"))
    assertTrue(helper.checkShowTimesOf(""" select * from ${viewName} """, { r -> r.size() == 15 }, 60, "target"))

}

