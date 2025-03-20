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

suite("test_dsd_view_drop") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def tableName = "tbl_" + helper.randomSuffix()
    def viewName = "view_test_" + helper.randomSuffix()

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

    sql """
        CREATE VIEW ${viewName} (k1, name,  v1)
        AS
        SELECT user_id as k1, name, SUM(age) FROM ${tableName}
        GROUP BY k1,name;
        """

    helper.enableDbBinlog()
    helper.ccrJobDelete()
    helper.ccrJobCreate()

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 30))
    assertTrue(helper.checkShowTimesOf(""" SHOW TABLES LIKE "${tableName}" """, exist, 60, "target"))
    assertTrue(helper.checkShowTimesOf(""" SHOW VIEWS LIKE "${viewName}" """, exist, 60, "target"))
    
    // 1. Pause ccr job
    helper.ccrJobPause()

    // 2. Insert N data
    sql """
        INSERT INTO ${tableName} VALUES
        (1, "Emily", 25),
        (2, "Benjamin", 35),
        (3, "Olivia", 28),
        (4, "Alexander", 60),
        (5, "Ava", 17);
        """
    assertTrue(helper.checkShowTimesOf(""" select * from ${tableName} """, { r -> r.size() == 5 }, 60, "sql"))


    // 3. Do operation & wait it finishes upstream
    sql "DROP VIEW IF EXISTS ${viewName}"
    assertTrue(helper.checkShowTimesOf(""" SHOW VIEWS LIKE "${viewName}" """, notExist, 60, "sql"))

    // 4. Insert N data
    sql """
        INSERT INTO ${tableName} VALUES
        (6, "John", 10),
        (7, "Jack", 11),
        (8, "Tom", 12),
        (9, "Alias", 13),
        (10, "Doris", 14);
        """
    assertTrue(helper.checkShowTimesOf(""" select * from ${tableName} """, { r -> r.size() == 10 }, 60, "sql"))


    // 5. Resume ccr job
    helper.ccrJobResume()
    assertTrue(helper.checkShowTimesOf(""" select * from ${tableName} """, { r -> r.size() == 10 }, 60, "target"))
    assertTrue(helper.checkShowTimesOf(""" SHOW VIEWS LIKE "${viewName}" """, notExist, 60, "target"))

}

