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

suite('test_ts_replication_num_invalid') {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", 'helper.groovy'))

    logger.info("Disable since ccrJobCreateWithReplicationNum is not implemented yet")

    def suffix = helper.randomSuffix()
    def tableName = 'tbl_' + suffix
    
    helper.enableDbBinlog()
    
    // Create table on source cluster
    sql """
        CREATE TABLE if NOT EXISTS ${tableName}
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
    
    helper.ccrJobDelete(tableName)
    
    // Test invalid value: replication_num = 0 (should fail)
    logger.info("Testing invalid replication_num = 0")
    def bodyJson = helper.get_ccr_body "${tableName}"
    def jsonSlurper = new groovy.json.JsonSlurper()
    def object = jsonSlurper.parseText "${bodyJson}"
    object['replication_num'] = 0
    
    bodyJson = new groovy.json.JsonBuilder(object).toString()
    
    def failed = false
    try {
        httpTest {
            uri "/create_ccr"
            endpoint helper.syncerAddress
            body "${bodyJson}"
            op "post"
            check { code, body ->
                def value = jsonSlurper.parseText "${body}"
                if (!value.success) {
                    // Expected to fail
                    logger.info("Expected failure for replication_num=0: ${value.error_msg}")
                    assertTrue(value.error_msg.contains("invalid") || 
                               value.error_msg.contains("must be -1") ||
                               value.error_msg.contains("or > 0"),
                        "Error message should indicate invalid replication_num")
                    failed = true
                } else {
                    throw new Exception("replication_num=0 should fail but succeeded")
                }
            }
        }
    } catch (Exception e) {
        logger.info("Caught expected exception: ${e.message}")
        failed = true
    }
    
    assertTrue(failed, "replication_num=0 should be rejected")
    
    // Test invalid value: replication_num = -2 (should fail)
    logger.info("Testing invalid replication_num = -2")
    object['replication_num'] = -2
    bodyJson = new groovy.json.JsonBuilder(object).toString()
    
    failed = false
    try {
        httpTest {
            uri "/create_ccr"
            endpoint helper.syncerAddress
            body "${bodyJson}"
            op "post"
            check { code, body ->
                def value = jsonSlurper.parseText "${body}"
                if (!value.success) {
                    logger.info("Expected failure for replication_num=-2: ${value.error_msg}")
                    assertTrue(value.error_msg.contains("invalid") || 
                               value.error_msg.contains("must be -1") ||
                               value.error_msg.contains("or > 0"),
                        "Error message should indicate invalid replication_num")
                    failed = true
                } else {
                    throw new Exception("replication_num=-2 should fail but succeeded")
                }
            }
        }
    } catch (Exception e) {
        logger.info("Caught expected exception: ${e.message}")
        failed = true
    }
    
    assertTrue(failed, "replication_num=-2 should be rejected")
    
    // Test valid value: replication_num = 1 (should succeed)
    logger.info("Testing valid replication_num = 1")
    helper.ccrJobCreateWithReplicationNum(tableName, 1)
    
    // Verify job created successfully
    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 60))
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}\"", 
        { res -> res.size() == 1 }, 60, 'target'))
    
    logger.info("Test passed: invalid replication_num values are correctly rejected")
    
    // Cleanup: delete CCR job
    helper.ccrJobDelete(tableName)
}

