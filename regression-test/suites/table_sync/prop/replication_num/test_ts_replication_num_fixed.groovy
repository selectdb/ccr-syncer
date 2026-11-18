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

suite('test_ts_replication_num_fixed') {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", 'helper.groovy'))

    def suffix = helper.randomSuffix()
    def tableName = 'tbl_' + suffix
    
    helper.enableDbBinlog()
    
    // Create table with replication_num = 3 on source cluster
    sql """
        CREATE TABLE if NOT EXISTS ${tableName}
        (
            `id` INT,
            `name` VARCHAR(128),
            `value` INT
        )
        ENGINE=OLAP
        UNIQUE KEY(`id`)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 3",
            "binlog.enable" = "true"
        )
    """
    
    // Insert some initial data
    sql """ INSERT INTO ${tableName} VALUES (1, 'test1', 100) """
    sql """ INSERT INTO ${tableName} VALUES (2, 'test2', 200) """
    
    helper.ccrJobDelete(tableName)
    
    // Create CCR job with replication_num = 1 (override source's 3 replicas)
    helper.ccrJobCreateWithReplicationNum(tableName, 1)
    
    // Wait for restore to finish
    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 60))
    
    // Check table exists on target
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}\"", 
        { res -> res.size() == 1 }, 60, 'target'))
    
    // Verify data sync
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName}", 2, 60))
    
    // Get and verify replication_num on target cluster
    def targetSql = target_sql "SHOW CREATE TABLE ${tableName}"
    logger.info("Target table DDL: ${targetSql}")
    assertTrue(targetSql.toString().contains("tag.location.default: 1"), 
        "Target table should have replication_num = 1")
    
    // Note: Table Sync only syncs the specified table, not new tables created later
    // For new table sync testing, see DB Sync tests instead
    
    // Test ALTER TABLE PROPERTIES - should be ignored when replication_num is set
    sql """
        ALTER TABLE ${tableName} 
        SET ("default.replication_allocation" = "tag.location.default: 2")
    """
    
    // Wait a bit for binlog sync
    Thread.sleep(5000)
    
    // Verify target table's replication_num remains 1 (not changed to 2)
    def afterAlterSql = target_sql "SHOW CREATE TABLE ${tableName}"
    logger.info("After ALTER target table DDL: ${afterAlterSql}")
    assertTrue(afterAlterSql.toString().contains("tag.location.default: 1"), 
        "Target table should still have replication_num = 1 after ALTER")
    
    // Continue to test DML operations
    sql """ INSERT INTO ${tableName} VALUES (3, 'test3', 300) """
    sql """ UPDATE ${tableName} SET value = 150 WHERE id = 1 """
    sql """ DELETE FROM ${tableName} WHERE id = 2 """
    
    // Verify DML operations synced correctly
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName} WHERE id = 1 AND value = 150", 
        1, 60))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName} WHERE id = 3", 1, 60))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName} WHERE id = 2", 0, 60))
    
    logger.info("Test passed: replication_num override works correctly")
    
    // Cleanup: delete CCR job
    helper.ccrJobDelete(tableName)
}

