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

suite('test_ts_sequence_column') {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", 'helper.groovy'))

    def tableName = 'tbl_' + helper.randomSuffix()
    def test_num = 0
    def insert_num = 5

    def exist = { res -> Boolean
        return res.size() != 0
    }

    def checkValue = { res, index, expect -> Boolean
        return res[index][3] == expect
    }

    def checkShowResult = { target_res, property -> Boolean
        if(!target_res[0][1].contains(property)){
            logger.info("don't contains {}", property)
            return false
        }
            return true 
    }

    def existSC = { res -> Boolean
        return checkShowResult(res, "\"function_column.sequence_col\" = \"modify_date\"")
    }

    helper.enableDbBinlog()

    sql """
        CREATE TABLE ${tableName}
        (
            user_id bigint,
            date date,
            modify_date datetimev2,
            user_name VARCHAR(128)
        )
        UNIQUE KEY(user_id, date)
        DISTRIBUTED BY HASH (user_id) BUCKETS 32
        PROPERTIES(
            "function_column.sequence_col" = "modify_date",
            "replication_num" = "1",
            "binlog.enable" = "true"
        )
    """

    logger.info("=== Test 1: check sequence column exist with create table sql ===")
    helper.ccrJobDelete(tableName)
    helper.ccrJobCreate(tableName)
    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 30))
    
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}\"", exist, 60, "sql"))

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}\"", exist, 60, "target"))

    assertTrue(helper.checkShowTimesOf("SHOW CREATE TABLE ${context.dbName}.${tableName}",
                                existSC, 30, 'sql'))
    assertTrue(helper.checkShowTimesOf("SHOW CREATE TABLE TEST_${context.dbName}.${tableName}",
                                existSC, 30, 'target'))


    logger.info('=== Test 2: insert some data ===')
    sql " insert into ${tableName} values(1, now(), now(), 'Steve'), (2, now(), now(), 'Alex')"

    assertTrue(helper.checkShowTimesOf("SELECT * FROM ${tableName}", { r -> r.size() == 2 }, 60, "target"))

    logger.info('=== Test 3: check data correct ===')

    def res = target_sql "SELECT * FROM ${tableName} ORDER BY user_id"
    assertTrue(checkValue(res, 0, 'Steve'))
    assertTrue(checkValue(res, 1, 'Alex'))

    logger.info('=== Test 4: insert new data replace old ===')
    sql " insert into ${tableName} values(1, now(), now(), 'Creeper'), (2, now(), now(), 'Pig'), (3, now(), now(), 'Dog')"

    assertTrue(helper.checkShowTimesOf("SELECT * FROM ${tableName}", { r -> r.size() == 3 }, 60, "target"))
    logger.info('=== Test 5: check new data correct ===')

    res = target_sql "SELECT * FROM ${tableName} ORDER BY user_id"
    logger.info("{}", res)
    assertTrue(checkValue(res, 0, 'Creeper'))
    assertTrue(checkValue(res, 1, 'Pig'))
    assertTrue(checkValue(res, 2, 'Dog'))
    logger.info('=== Test 6: insert new data but sequence column less ===')
    sql " insert into ${tableName} values(3, now(), '2025-01-01 12:00:00', 'Wolf'), (4, now(), now(), 'Diamond')"

    assertTrue(helper.checkShowTimesOf("SELECT * FROM ${tableName}", { r -> r.size() == 4 }, 60, "target"))
    logger.info('=== Test 7: check data correct ===')

    res = target_sql "SELECT * FROM ${tableName} ORDER BY user_id"
    assertTrue(checkValue(res, 2, 'Dog'))
}
