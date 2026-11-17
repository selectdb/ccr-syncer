
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

suite("test_ts_table_recreate_recovery") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))
    
    def tableName = "tbl_recreate_" + helper.randomSuffix()
    def insert_num = 5

    def exist = { res -> Boolean
        return res.size() != 0
    }

    sql "DROP TABLE IF EXISTS ${tableName}"
    target_sql "DROP TABLE IF EXISTS ${tableName}"

    helper.enableDbBinlog()
    
    // Check if feature_resync_on_table_recreate is enabled
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
    
    if (!featureEnabled) {
        throw new IllegalStateException("Feature 'feature_resync_on_table_recreate' is disabled. Enable syncer with: -feature_resync_on_table_recreate=true")
    }
    logger.info("Feature 'feature_resync_on_table_recreate' is enabled")

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

        logger.info("Test 3.2: wait for auto recovery")
        sleep(40000)
        assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}\"", exist, 60, "target"))
        
        def recoverySuccess = false
        for (int retry = 0; retry < 3; retry++) {
            def newData = target_sql "SELECT COUNT(*) FROM ${tableName} WHERE id >= 10"
            def oldData = target_sql "SELECT COUNT(*) FROM ${tableName} WHERE id < 10"
            logger.info("Check #${retry + 1}: new=${newData[0][0]}, old=${oldData[0][0]}")
            
            if (newData[0][0] >= insert_num && oldData[0][0] == 0) {
                recoverySuccess = true
                break
            }
            if (retry < 2) sleep(20000)
        }
        
        if (!recoverySuccess) {
            def finalNew = target_sql "SELECT COUNT(*) FROM ${tableName} WHERE id >= 10"
            def finalOld = target_sql "SELECT COUNT(*) FROM ${tableName} WHERE id < 10"
            fail("Auto recovery failed: expected old=0, new=${insert_num}, but got old=${finalOld[0][0]}, new=${finalNew[0][0]}")
        }

        logger.info("Test 3.3: final verification")
        def finalTotal = target_sql "SELECT COUNT(*) FROM ${tableName}"
        assertEquals(insert_num, finalTotal[0][0] as Integer)

        logger.info("Test 4: incremental sync after recovery")
        for (int i = 20; i < 20 + insert_num; i++) {
            sql """ INSERT INTO ${tableName} (id, name, value) VALUES (${i}, 'incr_${i}', ${i * 100}) """
        }
        
        sleep(5000)
        
        def checkIncremental = { res -> Boolean
            def count = res.size() > 0 ? res[0][0] as Integer : 0
            return count >= insert_num
        }
        assertTrue(helper.checkShowTimesOf("SELECT COUNT(*) FROM ${tableName} WHERE id >= 20", checkIncremental, 30, "target"))

        def srcFinal = sql "SELECT COUNT(*) FROM ${tableName}"
        def destFinal = target_sql "SELECT COUNT(*) FROM ${tableName}"
        
        logger.info("Final count: src=${srcFinal[0][0]}, dest=${destFinal[0][0]}")
        assertEquals(srcFinal[0][0], destFinal[0][0])
        
        def srcOld = sql "SELECT COUNT(*) FROM ${tableName} WHERE id < 10"
        def srcNew = sql "SELECT COUNT(*) FROM ${tableName} WHERE id >= 10 AND id < 20"
        def srcIncr = sql "SELECT COUNT(*) FROM ${tableName} WHERE id >= 20"
        def destOld = target_sql "SELECT COUNT(*) FROM ${tableName} WHERE id < 10"
        def destNew = target_sql "SELECT COUNT(*) FROM ${tableName} WHERE id >= 10 AND id < 20"
        def destIncr = target_sql "SELECT COUNT(*) FROM ${tableName} WHERE id >= 20"
        
        logger.info("Data distribution: old(${srcOld[0][0]}/${destOld[0][0]}), new(${srcNew[0][0]}/${destNew[0][0]}), incr(${srcIncr[0][0]}/${destIncr[0][0]})")
        
        assertEquals(0, srcOld[0][0] as Integer)
        assertEquals(0, destOld[0][0] as Integer)
        assertEquals(srcNew[0][0], destNew[0][0])
        assertEquals(srcIncr[0][0], destIncr[0][0])
        
    } finally {
        try {
            helper.ccrJobDelete(tableName)
        } catch (Exception e) {
            logger.warn("Cleanup failed: ${e.message}")
        }
    }
}