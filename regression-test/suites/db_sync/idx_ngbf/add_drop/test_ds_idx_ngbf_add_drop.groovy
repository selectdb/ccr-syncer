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

suite('test_ds_ng_bf_fullsync') {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", 'helper.groovy'))

    def tableName = 'tbl_' + helper.randomSuffix()
    def test_num = 0
    def insert_num = 5

    helper.enableDbBinlog()
    
    sql "DROP TABLE IF EXISTS ${tableName}"
    target_sql "DROP TABLE IF EXISTS ${tableName}"
    
    sql """
         
        CREATE TABLE if NOT EXISTS ${tableName}
        (
            `id` LARGEINT NOT NULL,
            `test` STRING NOT NULL,
            `value` STRING DEFAULT "",
            INDEX `idx_value` (`value`) USING NGRAM_BF PROPERTIES ("gram_size"="3", "bf_size"="1024")
        )
        ENGINE=OLAP
        UNIQUE KEY(`id`)
        PARTITION BY RANGE(`id`)
        (   
            PARTITION p1 VALUES LESS THAN ("10"),
            PARTITION p2 VALUES LESS THAN ("20"),
            PARTITION p3 VALUES LESS THAN ("30")
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
    """
    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${tableName} VALUES (${test_num}, "test_${index}", "${index}_test")
            """
    }
    sql 'sync'

    logger.info('=== Test 1: full update NGRAM_BF filter ===')
	
	helper.ccrJobDelete()
    helper.ccrJobCreate()

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 30))
    def checkNGFilter = { inputRes -> Boolean
        for (List<Object> row : inputRes) {
            if ((row[1] as String).contains('NGRAM_BF')) {
                return true
            }
        }
        return false
    }
    assertTrue(helper.checkShowTimesOf("""
                                SHOW CREATE TABLE TEST_${context.dbName}.${tableName}
                                """,
                                checkNGFilter, 30, 'target'))

    logger.info('=== Test 2: drop NGRAM_BF filter ===')
   
    sql """
        ALTER TABLE ${context.dbName}.${tableName} DROP INDEX idx_value;
        """
        
    sql 'sync'
    
    def checkNGFilterDrop = { inputRes -> Boolean
        for (List<Object> row : inputRes) {
            if ((row[1] as String).contains('NGRAM_BF')) {
                return false
            }
        }
        return true
    }
    
    assertTrue(helper.checkShowTimesOf("""
                                SHOW CREATE TABLE ${context.dbName}.${tableName}
                                """,
                                checkNGFilterDrop, 30, 'sql'))
    
    assertTrue(helper.checkShowTimesOf("""
                                SHOW CREATE TABLE TEST_${context.dbName}.${tableName}
                                """,
                                checkNGFilterDrop, 30, 'target'))
                                
    logger.info('=== Test 3: add NGRAM_BF filter ===')
    
    sql """
        ALTER TABLE ${context.dbName}.${tableName} ADD INDEX `idx_value` (`value`) USING NGRAM_BF PROPERTIES ("gram_size"="3", "bf_size"="1024") COMMENT 'username ngram_bf index';
        """
        
    assertTrue(helper.checkShowTimesOf("""
                                SHOW CREATE TABLE TEST_${context.dbName}.${tableName}
                                """,
                                checkNGFilter, 30, 'target'))    
								
	
}
