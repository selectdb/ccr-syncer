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

suite("test_keyword_nema") {

    def tableName = "roles"
    def syncerAddress = "127.0.0.1:9190"
    def test_num = 0
    def insert_num = 5
    def sync_gap_time = 5000
    def opPartitonName = "less0"
    String response

    def checkSelectTimesOf = { sqlString, rowSize, times -> Boolean
        def tmpRes = target_sql "${sqlString}"
        while (tmpRes.size() != rowSize) {
            sleep(sync_gap_time)
            if (--times > 0) {
                tmpRes = target_sql "${sqlString}"
            } else {
                break
            }
        }
        return tmpRes.size() == rowSize
    }

    def checkShowTimesOf = { sqlString, myClosure, times, func = "sql" -> Boolean
        Boolean ret = false
        List<List<Object>> res
        while (times > 0) {
            try {
                if (func == "sql") {
                    res = sql "${sqlString}"
                } else {
                    res = target_sql "${sqlString}"
                }
                if (myClosure.call(res)) {
                    ret = true
                }
            } catch (Exception e) {}

            if (ret) {
                break
            } else if (--times > 0) {
                sleep(sync_gap_time)
            }
        }

        return ret
    }

    def checkRestoreFinishTimesOf = { checkTable, times -> Boolean
        Boolean ret = false
        while (times > 0) {
            def sqlInfo = target_sql "SHOW RESTORE FROM TEST_${context.dbName}"
            for (List<Object> row : sqlInfo) {
                if ((row[10] as String).contains(checkTable)) {
                    ret = (row[4] as String) == "FINISHED"
                }
            }

            if (ret) {
                break
            } else if (--times > 0) {
                sleep(sync_gap_time)
            }
        }

        return ret
    }

    def exist = { res -> Boolean
        return res.size() != 0
    }
    def notExist = { res -> Boolean
        return res.size() == 0
    }

    sql """
        CREATE TABLE `${tableName}` (
            role_id       INT,
            occupation    VARCHAR(32),
            camp          VARCHAR(32),
            register_time DATE
        )
        UNIQUE KEY(role_id)
        DISTRIBUTED BY HASH(role_id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        );
    """
    // sql """ALTER TABLE ${tableName} set ("binlog.enable" = "true")"""

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

    httpTest {
        uri "/create_ccr"
        endpoint syncerAddress
        def bodyJson = get_ccr_body "${tableName}"
        body "${bodyJson}"
        op "post"
        result response
    }

    assertTrue(checkRestoreFinishTimesOf("${tableName}", 30))



    logger.info("=== Test 1: Check keyword name table ===")
    // def checkShowTimesOf = { sqlString, myClosure, times, func = "sql" -> Boolean
    assertTrue(checkShowTimesOf("""
                                SHOW CREATE TABLE `TEST_${context.dbName}`.`${tableName}`
                                """,
                                exist, 30, "target"))

}