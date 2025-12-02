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

suite('test_ds_replication_num_inherit') {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", 'helper.groovy'))

    logger.info("Disable since ccrJobCreateWithReplicationNum is not implemented yet")

    def suffix = helper.randomSuffix()
    def tableName1 = 'tbl1_' + suffix
    def tableName2 = 'tbl2_' + suffix
    def dbName = context.dbName
    
    helper.enableDbBinlog()
    
    // Create first table with replication_num = 1 on source cluster
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
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
    """
    
    sql """ INSERT INTO ${tableName1} VALUES (1, 'test1') """
    
    helper.ccrJobDelete()
    
    // Create DB Sync CCR job with replication_num = -1 (inherit from source)
    helper.ccrJobCreateWithReplicationNum("", -1)
    
    // Wait for first table to sync
    assertTrue(helper.checkRestoreFinishTimesOf("${tableName1}", 60))
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName1}\"", 
        { res -> res.size() == 1 }, 60, 'target'))
    
    // Verify data sync
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName1}", 1, 60))
    
    // Verify first table's replication_num on target (should inherit from source: 1)
    def targetSql1 = target_sql "SHOW CREATE TABLE ${tableName1}"
    logger.info("Target table1 DDL: ${targetSql1}")
    assertTrue(targetSql1.toString().contains("tag.location.default: 1"), 
        "Target table should inherit replication_num = 1 from source")
    
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
    
    // Verify second table's replication_num on target (should inherit from source: 2, not 1)
    def targetSql2 = target_sql "SHOW CREATE TABLE ${tableName2}"
    logger.info("Target table2 DDL: ${targetSql2}")
    assertTrue(targetSql2.toString().contains("tag.location.default: 2"), 
        "Target table2 should inherit replication_num = 2 from source")
    
    // Test ALTER TABLE PROPERTIES - should sync to target when in inherit mode
    // Change table1's replication_num from 1 to 3 to verify inheritance behavior
    sql """
        ALTER TABLE ${tableName1} 
        SET ("default.replication_allocation" = "tag.location.default: 3")
    """
    
    // Wait for binlog sync
    Thread.sleep(5000)
    
    // Verify target table's replication_num follows the change from 1 to 3
    def afterAlterSql = target_sql "SHOW CREATE TABLE ${tableName1}"
    logger.info("After ALTER target table1 DDL: ${afterAlterSql}")
    assertTrue(afterAlterSql.toString().contains("tag.location.default: 3"), 
        "Target table should follow source's replication_num change (1 -> 3) in inherit mode")
    
    // Test DML on both tables
    sql """ INSERT INTO ${tableName1} VALUES (2, 'test2') """
    sql """ INSERT INTO ${tableName2} VALUES (101, 201) """
    
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName1}", 2, 60))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName2}", 2, 60))
    
    logger.info("Test passed: DB sync with replication_num inherit mode works correctly")
    
    // Cleanup: delete CCR job
    helper.ccrJobDelete()
}


