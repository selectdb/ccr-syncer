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

suite("test_variant_ccr") {
    def versions = sql_return_maparray "show variables like 'version_comment'"
    if (versions[0].Value.contains('doris-2.0.')) {
        logger.info("2.0 not support variant case, current version is: ${versions[0].Value}")
        return
    }

    def tableName = "test_variant_" + UUID.randomUUID().toString().replace("-", "")
    def syncerAddress = "127.0.0.1:9190"
    def insert_num = 5
    def sync_gap_time = 5000
    String response

    def checkSelectTimesOf = { sqlString, rowSize, times, func = null -> Boolean
        def tmpRes = target_sql "${sqlString}"
        while (tmpRes.size() != rowSize || (func != null && !func(tmpRes))) {
            sleep(sync_gap_time)
            if (--times > 0) {
                tmpRes = target_sql "${sqlString}"
            } else {
                break
            }
        }
        return tmpRes.size() == rowSize && (func == null || func(tmpRes))
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

    sql """
        CREATE TABLE if NOT EXISTS ${tableName}
               (
                    k bigint,
                    var variant
                )
                UNIQUE KEY(`k`)
                DISTRIBUTED BY HASH(k) BUCKETS 1
                properties("replication_num" = "1", "disable_auto_compaction" = "false");
    """
    for (int index = 0; index < insert_num; ++index) {
        sql """
            INSERT INTO ${tableName} VALUES (${index}, '{"key_${index}":"value_${index}"}')
            """
    }
    sql """ALTER TABLE ${tableName} set ("binlog.enable" = "true")"""
    sql "sync"
    httpTest {
        uri "/create_ccr"
        endpoint syncerAddress
        def bodyJson = get_ccr_body "${tableName}"
        body "${bodyJson}"
        op "post"
        result response
    }

    assertTrue(checkRestoreFinishTimesOf("${tableName}", 30))
    def res = target_sql "SHOW CREATE TABLE ${tableName}"
    def createSuccess = false
    for (List<Object> row : res) {
        def get_table_name = row[0] as String
        logger.info("get_table_name is ${get_table_name}")
        def compare_table_name = "${tableName}"
        logger.info("compare_table_name is ${compare_table_name}")
        if (get_table_name == compare_table_name) {
            createSuccess = true
            break
        }
    }
    assertTrue(createSuccess)
    def count_res = target_sql " select count(*) from ${tableName}"
    def count = count_res[0][0] as Integer
    assertTrue(count.equals(insert_num))

    (0..count-1).each {Integer i ->
        def var_reult =  target_sql " select CAST(var[\"key_${i}\"] AS TEXT) from ${tableName} where k = ${i}"
        assertTrue((var_reult[0][0] as String) == ("value_${i}" as String))

    }

}

