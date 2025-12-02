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

suite('test_ds_replication_num_fixed') {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", 'helper.groovy'))
            
    logger.info("Disable since ccrJobCreateWithReplicationNum is not implemented yet")
    return

    def suffix = helper.randomSuffix()
    def tableName1 = 'tbl1_' + suffix
    def tableName2 = 'tbl2_' + suffix
    def dbName = context.dbName
    
    helper.enableDbBinlog()
    
    // Create first table with replication_num = 3 on source cluster
    sql """
        CREATE TABLE if NOT EXISTS ${tableName1}
        (
            `id` INT,
            `name` VARCHAR(128)
        )
        ENGINE=OLAP
        UNIQUE KEY(`id`)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 3",
            "binlog.enable" = "true"
        )
    """
    
    sql """ INSERT INTO ${tableName1} VALUES (1, 'test1') """
    
    helper.ccrJobDelete()
    
    // Create DB Sync CCR job with replication_num = 1
    helper.ccrJobCreateWithReplicationNum("", 1)
    
    // Wait for first table to sync
    assertTrue(helper.checkRestoreFinishTimesOf("${tableName1}", 60))
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName1}\"", 
        { res -> res.size() == 1 }, 60, 'target'))
    
    // Verify data sync
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName1}", 1, 60))
    
    // Verify first table's replication_num on target (should be 1, not 3)
    def targetSql1 = target_sql "SHOW CREATE TABLE ${tableName1}"
    logger.info("Target table1 DDL: ${targetSql1}")
    assertTrue(targetSql1.toString().contains("tag.location.default: 1"), 
        "Target table should have replication_num = 1")
    
    // Create second table with different replication_num on source
    sql """
        CREATE TABLE if NOT EXISTS ${tableName2}
        (
            `id` INT,
            `value` INT
        )
        ENGINE=OLAP
        UNIQUE KEY(`id`)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 2",
            "binlog.enable" = "true"
        )
    """
    
    sql """ INSERT INTO ${tableName2} VALUES (100, 200) """
    
    // Wait for second table to sync
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName2}\"", 
        { res -> res.size() == 1 }, 60, 'target'))
    
    // Verify second table's data
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName2}", 1, 60))
    
    // Verify second table's replication_num on target (should also be 1, not 2)
    def targetSql2 = target_sql "SHOW CREATE TABLE ${tableName2}"
    logger.info("Target table2 DDL: ${targetSql2}")
    assertTrue(targetSql2.toString().contains("tag.location.default: 1"), 
        "Target table2 should have replication_num = 1")
    
    // Test ALTER TABLE PROPERTIES - should be ignored
    sql """
        ALTER TABLE ${tableName1} 
        SET ("default.replication_allocation" = "tag.location.default: 3")
    """
    
    // Wait for binlog sync
    Thread.sleep(5000)
    
    // Verify target table's replication_num remains 1
    def afterAlterSql = target_sql "SHOW CREATE TABLE ${tableName1}"
    logger.info("After ALTER target table1 DDL: ${afterAlterSql}")
    assertTrue(afterAlterSql.toString().contains("tag.location.default: 1"), 
        "Target table should still have replication_num = 1 after ALTER")
    
    // Test DML on both tables
    sql """ INSERT INTO ${tableName1} VALUES (2, 'test2') """
    sql """ INSERT INTO ${tableName2} VALUES (101, 201) """
    
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName1}", 2, 60))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName2}", 2, 60))
    
    logger.info("Test passed: DB sync with replication_num override works correctly")
    
    // Cleanup: delete CCR job
    helper.ccrJobDelete()
}

