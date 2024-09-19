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

class Helper {
    def suite
    def context

    // the configurations about ccr syncer.
    def sync_gap_time = 5000
    def syncerAddress = "127.0.0.1:9190"

    Helper(suite) {
        this.suite = suite
        this.context = suite.context
    }

    String randomSuffix() {
        return UUID.randomUUID().toString().replace("-", "")
    }

    void ccrJobDelete(table = "") {
        def bodyJson = suite.get_ccr_body "${table}"
        suite.httpTest {
            uri "/delete"
            endpoint syncerAddress
            body "${bodyJson}"
            op "post"
        }
    }

    void ccrJobCreate(table = "") {
        def bodyJson = suite.get_ccr_body "${table}"
        suite.httpTest {
            uri "/create_ccr"
            endpoint syncerAddress
            body "${bodyJson}"
            op "post"
        }
    }

    void ccrJobCreateAllowTableExists(table = "") {
        def bodyJson = suite.get_ccr_body "${table}"
        def jsonSlurper = new groovy.json.JsonSlurper()
        def object = jsonSlurper.parseText "${bodyJson}"
        object['allow_table_exists'] = true
        suite.logger.info("json object ${object}")

        bodyJson = new groovy.json.JsonBuilder(object).toString()
        suite.httpTest {
            uri "/create_ccr"
            endpoint syncerAddress
            body "${bodyJson}"
            op "post"
        }
    }

    void ccrJobPause(table = "") {
        def bodyJson = suite.get_ccr_body "${table}"
        suite.httpTest {
            uri "/pause"
            endpoint syncerAddress
            body "${bodyJson}"
            op "post"
        }
    }

    void ccrJobResume(table = "") {
        def bodyJson = suite.get_ccr_body "${table}"
        suite.httpTest {
            uri "/resume"
            endpoint syncerAddress
            body "${bodyJson}"
            op "post"
        }
    }

    void ccrJobDesync(table = "") {
        def bodyJson = suite.get_ccr_body "${table}"
        suite.httpTest {
            uri "/desync"
            endpoint syncerAddress
            body "${bodyJson}"
            op "post"
        }
    }

    void enableDbBinlog() {
        suite.sql """
            ALTER DATABASE ${context.dbName} SET properties ("binlog.enable" = "true")
            """
    }

    Boolean checkShowTimesOf(sqlString, myClosure, times, func = "sql") {
        Boolean ret = false
        List<List<Object>> res
        while (times > 0) {
            try {
                if (func == "sql") {
                    res = suite.sql "${sqlString}"
                } else {
                    res = suite.target_sql "${sqlString}"
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

    // wait until all restore tasks of the dest cluster are finished.
    Boolean checkRestoreFinishTimesOf(checkTable, times) {
        Boolean ret = false
        while (times > 0) {
            def sqlInfo = suite.target_sql "SHOW RESTORE FROM TEST_${context.dbName}"
            for (List<Object> row : sqlInfo) {
                if ((row[10] as String).contains(checkTable)) {
                    suite.logger.info("SHOW RESTORE result: ${row}")
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

    // Check N times whether the num of rows of the downstream data is expected.
    Boolean checkSelectTimesOf(sqlString, rowSize, times) {
        def tmpRes = suite.target_sql "${sqlString}"
        while (tmpRes.size() != rowSize) {
            sleep(sync_gap_time)
            if (--times > 0) {
                tmpRes = suite.target_sql "${sqlString}"
            } else {
                break
            }
        }
        return tmpRes.size() == rowSize
    }

    Boolean checkSelectColTimesOf(sqlString, colSize, times) {
        def tmpRes = suite.target_sql "${sqlString}"
        while (tmpRes.size() == 0 || tmpRes[0].size() != colSize) {
            sleep(sync_gap_time)
            if (--times > 0) {
                tmpRes = suite.target_sql "${sqlString}"
            } else {
                break
            }
        }
        return tmpRes.size() > 0 && tmpRes[0].size() == colSize
    }

    Boolean checkData(data, beginCol, value) {
        if (data.size() < beginCol + value.size()) {
            return false
        }

        for (int i = 0; i < value.size(); ++i) {
            if ((data[beginCol + i]) as int != value[i]) {
                return false
            }
        }

        return true
    }

    Integer getRestoreRowSize(checkTable) {
        def result = suite.target_sql "SHOW RESTORE FROM TEST_${context.dbName}"
        def size = 0
        for (List<Object> row : result) {
            if ((row[10] as String).contains(checkTable)) {
                size += 1
            }
        }

        return size
    }

    Boolean checkRestoreNumAndFinishedTimesOf(checkTable, expectedRestoreRows, times) {
        while (times > 0) {
            def restore_size = getRestoreRowSize(checkTable)
            if (restore_size >= expectedRestoreRows) {
                return checkRestoreFinishTimesOf(checkTable, times)
            }
            if (--times > 0) {
                sleep(sync_gap_time)
            }
        }

        return false
    }
}

new Helper(suite)
