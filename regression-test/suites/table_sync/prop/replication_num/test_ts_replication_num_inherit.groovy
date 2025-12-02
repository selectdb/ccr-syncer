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

suite('test_ts_replication_num_inherit') {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", 'helper.groovy'))

    logger.info("Disable since ccrJobCreateWithReplicationNum is not implemented yet")

    def suffix = helper.randomSuffix()
    def tableName = 'tbl_' + suffix
    
    helper.enableDbBinlog()
    
    // Create table with replication_num = 1 on source cluster
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
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
    """
    
    // Insert some initial data
    sql """ INSERT INTO ${tableName} VALUES (1, 'test1', 100) """
    sql """ INSERT INTO ${tableName} VALUES (2, 'test2', 200) """
    
    helper.ccrJobDelete(tableName)
    
    // Create CCR job with replication_num = -1 (inherit from source, default behavior)
    helper.ccrJobCreateWithReplicationNum(tableName, -1)
    
    // Wait for restore to finish
    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 60))
    
    // Check table exists on target
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}\"", 
        { res -> res.size() == 1 }, 60, 'target'))
    
    // Verify data sync
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName}", 2, 60))
    
    // Get and verify replication_num on target cluster (should inherit from source: 1)
    def targetSql = target_sql "SHOW CREATE TABLE ${tableName}"
    logger.info("Target table DDL: ${targetSql}")
    assertTrue(targetSql.toString().contains("tag.location.default: 1"), 
        "Target table should inherit replication_num = 1 from source")
    
    // Note: Table Sync only syncs the specified table, not new tables created later
    // For new table sync testing, see DB Sync tests instead
    
    // Test ALTER TABLE PROPERTIES - should sync to target when in inherit mode
    // Change replication_num from 1 to 2 to verify inheritance behavior
    sql """
        ALTER TABLE ${tableName} 
        SET ("default.replication_allocation" = "tag.location.default: 2")
    """
    
    // Wait for binlog sync
    Thread.sleep(5000)
    
    // Verify target table's replication_num follows the change from 1 to 2
    def afterAlterSql = target_sql "SHOW CREATE TABLE ${tableName}"
    logger.info("After ALTER target table DDL: ${afterAlterSql}")
    assertTrue(afterAlterSql.toString().contains("tag.location.default: 2"), 
        "Target table should follow source's replication_num change (1 -> 2) in inherit mode")
    
    // Continue to test DML operations
    sql """ INSERT INTO ${tableName} VALUES (3, 'test3', 300) """
    sql """ UPDATE ${tableName} SET value = 150 WHERE id = 1 """
    
    // Verify DML operations synced correctly
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName} WHERE id = 1 AND value = 150", 
        1, 60))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName} WHERE id = 3", 1, 60))
    
    logger.info("Test passed: replication_num inherit mode works correctly")
    
    // Cleanup: delete CCR job
    helper.ccrJobDelete(tableName)
}

