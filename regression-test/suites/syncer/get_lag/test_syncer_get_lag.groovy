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

suite("test_syncer_get_lag") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", 'helper.groovy'))
    def tableName = "t_lag_test_" + helper.randomSuffix()
    
    sql """ DROP TABLE IF EXISTS ${tableName} """
    target_sql """DROP TABLE IF EXISTS ${tableName}"""
    
    sql """
        CREATE TABLE ${tableName} (
            id INT,
            name VARCHAR(50),
            value DOUBLE
        )
        DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES(
            "replication_num" = "1",
            "binlog.enable" = "true"
        )
    """
    
    target_sql """
        DROP TABLE IF EXISTS ${tableName}
    """
    
    helper.enableDbBinlog()
    
    helper.ccrJobCreate(tableName)
    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 60))
    
    def validateLagData = { lag ->
        assertTrue(lag != null)
        assertTrue(lag.containsKey("lag"))
        assertTrue(lag.containsKey("next_commit_seq"))
        assertTrue(lag.containsKey("last_commit_seq"))
        assertTrue(lag.containsKey("first_binlog_timestamp"))
        assertTrue(lag.containsKey("last_binlog_timestamp"))
        assertTrue(lag.containsKey("next_binlog_timestamp"))
        assertTrue(lag.containsKey("time_interval_secs"))
        assertTrue(lag.containsKey("first_commit_seq"))
        assertTrue(lag.lag >= 0)
    }
    
    def initialLag = helper.get_job_lag(tableName)
    logger.info("Initial lag info: ${initialLag}")
    
    validateLagData(initialLag)
    Boolean res = initialLag.last_commit_seq - initialLag.next_commit_seq <= 1
    assertTrue(res);

    {
        sql """
            INSERT INTO ${tableName} VALUES (1, 'test1', 1.1), (2, 'test2', 2.2), (3, 'test3', 3.3)
        """

        def lagImmediatelyAfterInsert = helper.get_job_lag(tableName)
        logger.info("Lag info immediately after insert: ${lagImmediatelyAfterInsert}")
        
        validateLagData(lagImmediatelyAfterInsert)
        
        assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName}", 3, 10))
        
        def lagAfterSync = helper.get_job_lag(tableName)
        logger.info("Lag info after sync: ${lagAfterSync}")
        
        validateLagData(lagAfterSync)
    }
    
    {
        helper.ccrJobPause(tableName)
        
        def initialRowCount = 3
        
        sql """
            INSERT INTO ${tableName} VALUES (4, 'test4', 4.4), (5, 'test5', 5.5)
        """
        
        def lagAfterPause = helper.get_job_lag(tableName)
        logger.info("Lag info after pause: ${lagAfterPause}")
        
        validateLagData(lagAfterPause)
        
        sql """
            INSERT INTO ${tableName} VALUES (6, 'test6', 6.6), (7, 'test7', 7.7)
        """
        
        def afterRowCount =  target_sql """ SELECT * FROM ${tableName} """
        assertEquals(initialRowCount, afterRowCount.size())
        
        def lagAfterMoreData = helper.get_job_lag(tableName)
        logger.info("Lag info after more data insertion while paused: ${lagAfterMoreData}")
        
        validateLagData(lagAfterMoreData)
        
        if (lagAfterPause.next_commit_seq == lagAfterMoreData.next_commit_seq) {
            assertTrue(lagAfterMoreData.lag >= lagAfterPause.lag)
        }
    }
    
    {
        
        def sourceRowCount = sql """ SELECT * FROM ${tableName} """
        
        helper.ccrJobResume(tableName)
        
        def lagAfterResumeImmediate = helper.get_job_lag(tableName)
        logger.info("Lag info immediately after resume: ${lagAfterResumeImmediate}")
        
        validateLagData(lagAfterResumeImmediate)
        
        assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName}", sourceRowCount.size(), 10))
        
        def lagAfterResume = helper.get_job_lag(tableName)
        logger.info("Lag info after sync completion post-resume: ${lagAfterResume}")
        
        validateLagData(lagAfterResume)
        
        if (lagAfterResume.next_commit_seq >= lagAfterResumeImmediate.next_commit_seq) {
            res = lagAfterResume.lag <= lagAfterResumeImmediate.lag || lagAfterResume.lag == 0
            assertTrue(res == true);
        }
    }
    
    {
        
        def initialRowCount = sql """ SELECT * FROM ${tableName} """
        
        def insertCount = 100
        def values = (1..insertCount).collect { "($it, 'batch_$it', $it.0)" }.join(",")
        sql """
            INSERT INTO ${tableName} VALUES ${values}
        """
        
        def lagAfterBatch = helper.get_job_lag(tableName)
        logger.info("Lag info after large batch: ${lagAfterBatch}")
        
        validateLagData(lagAfterBatch)
        
        def expectedTotal = initialRowCount.size() + insertCount
        assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName}", expectedTotal, 20))
        
        def lagAfterSync = helper.get_job_lag(tableName)
        logger.info("Lag info after sync: ${lagAfterSync}")
        
        validateLagData(lagAfterSync)
        
        if (lagAfterSync.next_binlog_timestamp && lagAfterSync.last_binlog_timestamp && 
            !lagAfterSync.next_binlog_timestamp.isEmpty() && !lagAfterSync.last_binlog_timestamp.isEmpty()) {
            
            assertTrue(lagAfterSync.time_interval_secs >= 0)
        }
    }
    
    {
        
        def initialRowCount = sql """ SELECT * FROM ${tableName} """
        
        helper.ccrJobDesync(tableName)
        
        sql """
            INSERT INTO ${tableName} VALUES (1001, 'desync_test', 100.1), (1002, 'desync_test2', 100.2)
        """
        
        def lagAfterDesync = helper.get_job_lag(tableName)
        logger.info("Lag info after desync: ${lagAfterDesync}")
        
        validateLagData(lagAfterDesync)
        
        afterRowCount =  target_sql """ select * FROM ${tableName} """
        assertEquals(initialRowCount.size(), afterRowCount.size())
        
        helper.ccrJobSync(tableName)
        
        def lagAfterReSyncImmediate = helper.get_job_lag(tableName)
        logger.info("Lag info immediately after re-sync: ${lagAfterReSyncImmediate}")
        
        validateLagData(lagAfterReSyncImmediate)
        
        def expectedTotal = initialRowCount.size() + 2  // Added 2 rows during desync
        assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName}", expectedTotal, 10))
        
        def lagAfterReSync = helper.get_job_lag(tableName)
        logger.info("Lag info after re-sync completion: ${lagAfterReSync}")
        
        validateLagData(lagAfterReSync)
    }
    
    {
        
        def initialRowCount = sql """ select * FROM ${tableName} """
        
        for (int i = 0; i < 3; i++) {
            sql """
                INSERT INTO ${tableName} VALUES (2000+${i}, 'rapid_${i}', ${i}.0)
            """
            
            def lagAfterEachInsert = helper.get_job_lag(tableName)
            logger.info("Lag after rapid insert ${i}: ${lagAfterEachInsert}")
            validateLagData(lagAfterEachInsert)
        }
        
        def expectedTotal = initialRowCount.size() + 3 
        assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName}", expectedTotal, 10))
        
        def finalLag = helper.get_job_lag(tableName)
        logger.info("Final lag after rapid operations: ${finalLag}")
        validateLagData(finalLag)
    }

    {
        
        def recycleTableName = "t_lag_recycle_" + helper.randomSuffix()
        
        sql """ DROP TABLE IF EXISTS ${recycleTableName} """
        target_sql """DROP TABLE IF EXISTS ${recycleTableName}"""
        
        sql """
            CREATE TABLE ${recycleTableName} (
                id INT,
                name VARCHAR(50),
                value DOUBLE
            )
            DISTRIBUTED BY HASH(id) BUCKETS 3
            PROPERTIES(
                "replication_num" = "1"
            )
        """
        
        helper.enableDbBinlog()
        
        helper.ccrJobCreate(recycleTableName)
        assertTrue(helper.checkRestoreFinishTimesOf("${recycleTableName}", 60))
        
        sql """
            INSERT INTO ${recycleTableName} VALUES (1, 'initial', 1.1), (2, 'initial', 2.2)
        """
        
        def initialRecycleLag = helper.get_job_lag(recycleTableName)
        logger.info("Initial lag info for recycle test: ${initialRecycleLag}")
        validateLagData(initialRecycleLag)
        def prevRecycleSeq = initialRecycleLag.last_commit_seq
        logger.info("Previous commit sequence for recycle test: ${prevRecycleSeq}")
        
        assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${recycleTableName}", 2, 10))
        
        logger.info("Disabling database binlog to simulate recycling...")
        sql """
            ALTER DATABASE ${context.dbName} SET PROPERTIES("binlog.enable" = "false")
        """
        
        sleep(3000)
        
        def checkBinlogDisabled = sql """
            SHOW CREATE DATABASE ${context.dbName}
        """
        logger.info("Database properties after disabling binlog: ${checkBinlogDisabled}")
        
        sql """
            INSERT INTO ${recycleTableName} VALUES (3, 'no_binlog', 3.3), (4, 'no_binlog', 4.4)
        """
        
        sleep(2000)
        
        def lagWhileDisabled = helper.get_job_lag(recycleTableName)
        validateLagData(lagWhileDisabled)
        
        logger.info("Dropping and recreating table to force binlog sequence reset...")
        sql """ DROP TABLE IF EXISTS ${recycleTableName} """
        
        sql """
            ALTER DATABASE ${context.dbName} SET PROPERTIES("binlog.enable" = "true")
        """
        
        sleep(3000)
        
        sql """
            CREATE TABLE ${recycleTableName} (
                id INT,
                name VARCHAR(50),
                value DOUBLE
            )
            DISTRIBUTED BY HASH(id) BUCKETS 3
            PROPERTIES(
                "replication_num" = "1", 
                "binlog.enable" = "true"
            )
        """
        
        sql """
            INSERT INTO ${recycleTableName} VALUES (5, 'new_binlog', 5.5), (6, 'new_binlog', 6.6)
        """
        
        sleep(3000)
        
        def lagAfterReset = helper.get_job_lag(recycleTableName)
        logger.info("Lag info after table reset: ${lagAfterReset}")
        validateLagData(lagAfterReset)
        
        def newNextCommitSeq = lagAfterReset.next_commit_seq
        logger.info("New next commit sequence after recycling: ${newNextCommitSeq}")
        logger.info("Previous last commit sequence: ${prevRecycleSeq}")
        logger.info("Testing if nextBinlog.getCommitSeq() > prevCommitSeq: ${newNextCommitSeq} > ${prevRecycleSeq}")
        
        assertTrue(newNextCommitSeq > prevRecycleSeq, 
            "Expected next commit sequence (${newNextCommitSeq}) to be greater than previous sequence (${prevRecycleSeq})")
        
        assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${recycleTableName}", 4, 10))
    }
}