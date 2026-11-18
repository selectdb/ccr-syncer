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
suite("test_cds_tbl_drop_create_1", "nonConcurrent") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def tableName = "tbl_" + helper.randomSuffix()
    def test_num = 0
    def insert_num = 10

    def exist = { res -> Boolean
        return res.size() != 0
    }
    def notExist = { res -> Boolean
        return res.size() == 0
    }

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
    helper.ccrJobDelete()
    helper.ccrJobCreate()

    GetDebugPoint().disableDebugPointForAllFEs("FE.PAUSE_PENDING_BACKUP_JOB") // avoid effect by previous test
    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 30))
    assertTrue(helper.checkShowTimesOf(""" SHOW TABLES LIKE "${tableName}" """, exist, 60, "target"))
    
    // 0. insert N data
    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${tableName} VALUES (${test_num}, ${index})
            """
    }
    
    assertTrue(helper.checkSelectTimesOf(""" select * from ${tableName} """, insert_num, 30))

    // 1. Pause backup JOB
    def prefix = helper.get_backup_label_prefix()
    GetDebugPoint().enableDebugPointForAllFEs("FE.PAUSE_PENDING_BACKUP_JOB", [value: prefix])
    
    // 2. Trigger full snapshot
    helper.force_fullsync()
    
    // 3. When upstream is backup
    def is_backup_running = { res ->
        for (int i = 0; i < res.size(); i++) {
            logger.info("backup job status: ${res[i]}")
            if (res[i][3] != "CANCELLED" && res[i][3] != "FINISHED") {
                return true
            }
        }
        return false
    }
    assertTrue(helper.checkShowTimesOf(
        """ SHOW BACKUP WHERE SnapshotName LIKE "${prefix}%" """,
        is_backup_running, 60))

    // 4. Drop table
    sql """ DROP TABLE ${tableName} FORCE """

    // 5. Create table
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

    // 6. Insert N data
    for (int index = 0; index < insert_num * 2; index++) {
        sql """
            INSERT INTO ${tableName} VALUES (${test_num}, ${index})
            """
    }
    assertTrue(helper.checkShowTimesOf(""" select * from ${tableName} """, { r -> r.size() == insert_num * 2 }, 30, "sql"))

    // 7. Resume backup JOB
    GetDebugPoint().disableDebugPointForAllFEs("FE.PAUSE_PENDING_BACKUP_JOB")

    // 8. Verify data and operation are synced downstream
    assertTrue(helper.checkSelectTimesOf(""" select * from ${tableName} """, insert_num * 2, 30))
}
