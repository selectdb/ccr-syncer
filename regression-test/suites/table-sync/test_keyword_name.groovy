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

suite("test_keyword_name") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def tableName = "roles"
    def newTableName = "test-hyphen"
    def test_num = 0
    def insert_num = 5
    def opPartitonName = "less0"

    def exist = { res -> Boolean
        return res.size() != 0
    }
    def notExist = { res -> Boolean
        return res.size() == 0
    }
    def has_count = { count ->
        return { res -> Boolean
            res.size() == count
        }
    }

    sql "DROP TABLE IF EXISTS `${tableName}` FORCE"
    target_sql "DROP TABLE IF EXISTS `${tableName}` FORCE"
    sql """
        CREATE TABLE `${tableName}` (
            `role`       INT,
            occupation    VARCHAR(32),
            camp          VARCHAR(32),
            register_time DATE
        )
        UNIQUE KEY(`role`)
        PARTITION BY RANGE (`role`)
        (
            PARTITION p1 VALUES LESS THAN ("10")
        )
        DISTRIBUTED BY HASH(`role`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        );
    """

    sql "DROP TABLE IF EXISTS `${newTableName}` FORCE"
    target_sql "DROP TABLE IF EXISTS `${newTableName}` FORCE"
    sql """
        CREATE TABLE `${newTableName}` (
            id       INT,
            name    VARCHAR(10)
        )
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        );
    """ 

    sql """
        INSERT INTO `${tableName}` VALUES
        (0, 'who am I', NULL, NULL),
        (1, 'mage', 'alliance', '2018-12-03 16:11:28'),
        (2, 'paladin', 'alliance', '2018-11-30 16:11:28'),
        (3, 'rogue', 'horde', '2018-12-01 16:11:28'),
        (4, 'priest', 'alliance', '2018-12-02 16:11:28'),
        (5, 'shaman', 'horde', NULL),
        (6, 'warrior', 'alliance', NULL),
        (7, 'warlock', 'horde', '2018-12-04 16:11:28'),
        (8, 'hunter', 'horde', NULL);
     """
    sql """
        INSERT INTO `${newTableName}` VALUES
        (1, 'a'),
        (2, 'b'),
        (3, 'c');
     """   

    // delete the exists ccr job first.
    helper.ccrJobDelete(tableName)
    helper.ccrJobCreate(tableName)

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 30))

    helper.ccrJobDelete(newTableName)
    helper.ccrJobCreate(newTableName)
    assertTrue(helper.checkRestoreFinishTimesOf("${newTableName}", 30))

    logger.info("=== Test 1: Check keyword name table ===")
    // def checkShowTimesOf = { sqlString, myClosure, times, func = "sql" -> Boolean
    assertTrue(helper.checkShowTimesOf("""
                                SHOW CREATE TABLE `TEST_${context.dbName}`.`${tableName}`
                                """,
                                exist, 30, "target"))

    assertTrue(helper.checkShowTimesOf("""
                                SHOW CREATE TABLE `TEST_${context.dbName}`.`${newTableName}`
                                """,
                                exist, 30, "target"))

    logger.info("=== Test 2: Add new partition ===")
    sql """
    ALTER TABLE `${tableName}` ADD PARTITION p2
    VALUES LESS THAN ("20")
    """

    sql """
        INSERT INTO `${tableName}` VALUES
        (11, 'who am I', NULL, NULL),
        (12, 'mage', 'alliance', '2018-12-03 16:11:28');
     """

    def checkNewPartition = { inputRes -> Boolean
        for (List<Object> row : inputRes) {
            if ((row[1] as String).contains("PARTITION p2")) {
                return true
            }
        }
        return false
    }
    assertTrue(helper.checkShowTimesOf("""
                                SHOW CREATE TABLE `TEST_${context.dbName}`.`${tableName}`
                                """,
                                checkNewPartition, 30, "target"))

    logger.info("=== Test 3: Truncate table ===")
    sql "TRUNCATE TABLE `${tableName}`"

    assertTrue(helper.checkShowTimesOf("""
                                SELECT * FROM `TEST_${context.dbName}`.`${tableName}`
                                """,
                                notExist, 30, "target"))

    logger.info("=== Test 4: Add column with keyword name ===")
    // index is a keyword
    sql "ALTER TABLE `${tableName}` ADD COLUMN `index` INT DEFAULT \"0\""
    sql "sync"

    assertTrue(helper.checkShowTimesOf("""
                                SHOW ALTER TABLE COLUMN
                                FROM ${context.dbName}
                                WHERE TableName = "${tableName}" AND State = "FINISHED"
                                """,
                                has_count(1), 30))

    def has_column_index = { res -> Boolean
        // Field == 'index' && 'Key' == 'NO'
        return res[4][0] == 'index' && (res[4][3] == 'NO' || res[4][3] == 'false')
    }

    assertTrue(helper.checkShowTimesOf("SHOW COLUMNS FROM `${tableName}`", has_column_index, 60, "target_sql"))
}
