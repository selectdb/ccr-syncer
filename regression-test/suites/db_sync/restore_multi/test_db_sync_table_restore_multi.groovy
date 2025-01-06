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

suite("test_db_sync_table_restore_multi") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def tableName = "test_db_sync_backup_restore_table_1"
    def tableName2 = "test_db_sync_backup_restore_table_2"    
    def newtableName = "test_db_sync_backup_restore_table_new_1"
    def newtableName2 = "test_db_sync_backup_restore_table_new_2"
    def snapshotName = "test_db_sync_backup_restore_table_snapshot"
    def repoName = "repo_" + UUID.randomUUID().toString().replace("-", "")
    def test_num = 0
    def insert_num = 10
    def syncer = getSyncer()
    def dbNameOrigin = context.dbName
    def dbNameTarget = "TEST_" + context.dbName
    syncer.createS3Repository(repoName)
    
    target_sql("DROP DATABASE IF EXISTS ${dbNameTarget}")
    sql "DROP TABLE IF EXISTS ${dbNameOrigin}.${tableName}"
    sql "DROP TABLE IF EXISTS ${dbNameOrigin}.${newtableName}"

    def exist = { res -> Boolean
        return res.size() != 0
    }
    def notExist = { res -> Boolean
        return res.size() == 0
    }

    sql """
        CREATE TABLE if NOT EXISTS ${dbNameOrigin}.${tableName}
        (
            `test` INT,
            `id` INT
        )
        UNIQUE KEY(`test`, `id`)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
    """



    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${dbNameOrigin}.${tableName} VALUES (${test_num}, ${index})
            """
    }
    
    sql """
        CREATE TABLE if NOT EXISTS ${dbNameOrigin}.${tableName2}
        (
            `test` INT,
            `id` INT
        )
        UNIQUE KEY(`test`, `id`)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
    """



    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${dbNameOrigin}.${tableName2} VALUES (${test_num}, ${index})
            """
    }
    
    helper.enableDbBinlog()
    helper.ccrJobDelete()
    helper.ccrJobCreate()


    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 60))
    logger.info("=== Test 1: Check table not exist ===")

    assertTrue(helper.checkShowTimesOf(""" SHOW TABLES LIKE "${newtableName}" """, notExist, 60, "sql"))


    logger.info("=== Test 2: Backup table===")

    assertTrue(helper.checkShowTimesOf(""" select * from ${dbNameOrigin}.${tableName} """, exist, 60, "sql"))

    sql """ 
            BACKUP SNAPSHOT ${snapshotName} 
            TO `${repoName}` 
            ON ( ${tableName}, ${tableName2})
            PROPERTIES ("type" = "full")
        """

    syncer.waitSnapshotFinish()
    def snapshot = syncer.getSnapshotTimestamp(repoName, snapshotName)
    assertTrue(snapshot != null)
    syncer.waitTargetRestoreFinish()

    logger.info("=== Test 3: Restore new table ===")

    sql """
        RESTORE SNAPSHOT ${snapshotName}
        FROM `${repoName}`
        ON (${tableName} as ${newtableName},
            ${tableName2} as ${newtableName2})
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "replication_num" = "1"
        )
    """

    syncer.waitAllRestoreFinish()

    logger.info("=== Test 4: Check table ===")

    assertTrue(helper.checkShowTimesOf(""" SHOW TABLES LIKE "${tableName}" """, exist, 60, "sql"))

    assertTrue(helper.checkShowTimesOf(""" SHOW TABLES LIKE "${newtableName}" """, exist, 60, "sql"))

    assertTrue(helper.checkShowTimesOf(""" SHOW TABLES LIKE "${tableName}" """, exist, 60, "target_sql"))

    assertTrue(helper.checkShowTimesOf(""" SHOW TABLES LIKE "${newtableName}" """, exist, 60, "target_sql"))

    order_qt_sql_source_content("SELECT * FROM ${tableName}")
    order_qt_target_sql_content("SELECT * FROM ${newtableName}")
    order_qt_sql_source_content("SELECT * FROM ${tableName2}")
    order_qt_target_sql_content("SELECT * FROM ${newtableName2}")    
}
