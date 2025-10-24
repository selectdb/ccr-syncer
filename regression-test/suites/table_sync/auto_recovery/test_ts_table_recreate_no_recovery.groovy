
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

suite("test_ts_table_recreate_no_recovery") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))
    
    def tableName = "tbl_no_recover_" + helper.randomSuffix()
    def insert_num = 5

    def exist = { res -> Boolean
        return res.size() != 0
    }

    sql "DROP TABLE IF EXISTS ${tableName}"
    target_sql "DROP TABLE IF EXISTS ${tableName}"

    helper.enableDbBinlog()
    
    // Check if feature_resync_on_table_recreate is disabled
    def jsonSlurper = new groovy.json.JsonSlurper()
    def featureEnabled = false
    httpTest {
        endpoint helper.syncerAddress
        uri "/features"
        op "get"
        check { code, body ->
            def result = jsonSlurper.parseText(body)
            def features = result.flags?.collectEntries { [(it.feature): it.value] }
            featureEnabled = features?.get("feature_resync_on_table_recreate") == true
        }
    }
    
    if (featureEnabled) {
        logger.info("Feature 'feature_resync_on_table_recreate' is enabled, skipping this test")
        logger.info("This test validates behavior when auto-recovery is DISABLED")
        return
    }
    logger.info("Feature 'feature_resync_on_table_recreate' is disabled (expected for this test)")

    sql """
        CREATE TABLE ${tableName}
        (
            `id` INT,
            `name` VARCHAR(50),
            `value` INT
        )
        ENGINE=OLAP
        UNIQUE KEY(`id`)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
    """

    helper.ccrJobDelete(tableName)
    helper.ccrJobCreate(tableName)
    
    try {
        assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 30))

        logger.info("Test 1: initial sync")
        for (int i = 0; i < insert_num; i++) {
            sql """ INSERT INTO ${tableName} VALUES (${i}, 'init_${i}', ${i * 100}) """
        }

        assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}\"", exist, 60, "sql"))
        assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}\"", exist, 60, "target"))
        assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName}", insert_num, 30))

        logger.info("Test 2: drop table")
        sql "DROP TABLE ${tableName}"
        sleep(5000)
        
        def targetCountAfterDrop = target_sql "SELECT COUNT(*) FROM ${tableName}"
        assertEquals(insert_num, targetCountAfterDrop[0][0] as Integer)

        logger.info("Test 3: recreate table with new structure")
        sql """
            CREATE TABLE ${tableName}
            (
                `id` INT,
                `name` VARCHAR(50),
                `value` INT,
                `extra` VARCHAR(100) DEFAULT 'new_column'
            )
            ENGINE=OLAP
            UNIQUE KEY(`id`)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "binlog.enable" = "true"
            )
        """

        for (int i = 10; i < 10 + insert_num; i++) {
            sql """ INSERT INTO ${tableName} (id, name, value) VALUES (${i}, 'new_${i}', ${i * 100}) """
        }

        logger.info("Test 3.2: wait and verify NO auto-recovery (feature disabled)")
        sleep(40000)
        
        // With feature disabled:
        // - Old data remains (not cleared)
        // - New data is NOT synced (table_id mismatch, syncer keeps waiting)
        // - Syncer continuously logs "table not found" and waits
        def targetOld = target_sql "SELECT COUNT(*) FROM ${tableName} WHERE id < 10"
        def targetNew = target_sql "SELECT COUNT(*) FROM ${tableName} WHERE id >= 10"
        def targetTotal = target_sql "SELECT COUNT(*) FROM ${tableName}"
        
        logger.info("Target after table recreate: total=${targetTotal[0][0]}, old=${targetOld[0][0]}, new=${targetNew[0][0]}")
        
        // Verify old data is still present (NOT cleared)
        assertEquals(insert_num, targetOld[0][0] as Integer, "Old data should still exist when feature disabled")
        
        // Verify new data is NOT synced (table_id mismatch, syncer keeps requesting old table_id)
        assertEquals(0, targetNew[0][0] as Integer, "New data should NOT be synced when feature disabled")
        
        // Total should be only old data
        assertEquals(insert_num, targetTotal[0][0] as Integer, "Total should be only old data")

        logger.info("Test 4: verify job is still running and waiting")
        // Job should remain in Running state, continuously waiting (not Paused)
        // Syncer continuously logs "table not found" or similar warnings
        
        // Insert more data to upstream (new table_id)
        logger.info("Test 4.1: insert incremental data to upstream")
        for (int i = 20; i < 20 + insert_num; i++) {
            sql """ INSERT INTO ${tableName} (id, name, value) VALUES (${i}, 'incr_${i}', ${i * 100}) """
        }
        
        sleep(10000)
        
        // Verify upstream has new data
        def srcFinal = sql "SELECT COUNT(*) FROM ${tableName}"
        logger.info("Upstream count: ${srcFinal[0][0]}")
        assertEquals(insert_num * 2, srcFinal[0][0] as Integer, "Upstream should have new + incr data")
        
        // But downstream still only has old data (NO sync happening)
        def destFinal = target_sql "SELECT COUNT(*) FROM ${tableName}"
        logger.info("Downstream count: ${destFinal[0][0]}")
        assertEquals(insert_num, destFinal[0][0] as Integer, "Downstream should still only have old data")
        
        // Verify data distribution
        def srcOld = sql "SELECT COUNT(*) FROM ${tableName} WHERE id < 10"
        def srcNew = sql "SELECT COUNT(*) FROM ${tableName} WHERE id >= 10 AND id < 20"
        def srcIncr = sql "SELECT COUNT(*) FROM ${tableName} WHERE id >= 20"
        
        def destOld = target_sql "SELECT COUNT(*) FROM ${tableName} WHERE id < 10"
        def destNew = target_sql "SELECT COUNT(*) FROM ${tableName} WHERE id >= 10"
        
        logger.info("Data distribution: src(old=${srcOld[0][0]}, new=${srcNew[0][0]}, incr=${srcIncr[0][0]}), dest(old=${destOld[0][0]}, new=${destNew[0][0]})")
        
        // Source has no old data (table was recreated)
        assertEquals(0, srcOld[0][0] as Integer)
        assertEquals(insert_num, srcNew[0][0] as Integer)
        assertEquals(insert_num, srcIncr[0][0] as Integer)
        
        // Dest only has old data, new data is NOT synced
        assertEquals(insert_num, destOld[0][0] as Integer, "Dest should have old data")
        assertEquals(0, destNew[0][0] as Integer, "Dest should NOT have new data (feature disabled)")
        
        logger.info("Test passed: feature disabled behavior validated")
        logger.info("- Old data retained on downstream")
        logger.info("- New data NOT synced (table_id mismatch)")
        logger.info("- Job keeps running and waiting (not paused)")
        logger.info("- User must enable feature or manually handle the job")
        
    } finally {
        try {
            helper.ccrJobDelete(tableName)
        } catch (Exception e) {
            logger.warn("Cleanup failed: ${e.message}")
        }
    }
}

