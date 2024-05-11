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
suite("usercases_cir_8537") {
    // Case description
    // Insert data and drop a partition, then the ccr syncer wouldn't get the partition ids from the source cluster.
    return

    def caseName = "usercases_cir_8537"
    def tableName = "${caseName}_sales" + UUID.randomUUID().toString().replace("-", "")
    def syncerAddress = "127.0.0.1:9190"
    def test_num = 0
    def insert_num = 5
    def sync_gap_time = 5000
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

    def checkRestoreFinishTimesOf = { checkTable, times -> Boolean
        Boolean ret = false
        while (times > 0) {
            def sqlInfo = target_sql "SHOW RESTORE FROM TEST_${context.dbName}"
            for (List<Object> row : sqlInfo) {
                if ((row[10] as String).contains(checkTable)) {
                    ret = row[4] == "FINISHED"
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

    def checkData = { data, beginCol, value -> Boolean
        if (data.size() < beginCol + value.size()) {
            return false
        }

        for (int i = 0; i < value.size(); ++i) {
            if ((data[beginCol + i] as int) != value[i]) {
                return false
            }
        }

        return true
    }

    sql """
    CREATE TABLE ${tableName} (
        sale_date DATE,
        id INT,
        product_id INT,
        quantity INT,
        revenue FLOAT
    )
    DUPLICATE KEY(sale_date, id)
    PARTITION BY RANGE(sale_date) (
        PARTITION p202001 VALUES [('2020-01-01'), ('2020-02-01')),
        PARTITION p202002 VALUES [('2020-02-01'), ('2020-03-01'))
    )
    DISTRIBUTED BY HASH(id) BUCKETS auto
    PROPERTIES (
        "replication_num" = "1",
        "binlog.enable" = "true"
    )
    """

    sql """
    INSERT INTO ${tableName} (id, product_id, sale_date, quantity, revenue)
    VALUES
    (3, 103, '2020-01-10', 15, 225.0),
    (4, 104, '2020-01-20', 30, 450.0);
    """

    sql "sync"

    logger.info("=== 1. create ccr ===")
    httpTest {
        uri "/create_ccr"
        endpoint syncerAddress
        def bodyJson = get_ccr_body "${tableName}"
        body "${bodyJson}"
        op "post"
        result response
    }
    assertTrue(checkRestoreFinishTimesOf("${tableName}", 60))
    qt_sql "SELECT * FROM ${tableName} ORDER BY id"
    qt_target_sql "SELECT * FROM ${tableName} ORDER BY id"

    logger.info("=== 2. pause ccr ===")
    httpTest {
        uri "/pause"
        endpoint syncerAddress
        def bodyJson = get_ccr_body "${tableName}"
        body "${bodyJson}"
        op "post"
        result response
    }

    sql """
    INSERT INTO ${tableName} (id, product_id, sale_date, quantity, revenue)
    VALUES
    (3, 103, '2020-01-10', 15, 225.0),
    (4, 104, '2020-01-20', 30, 450.0);
    """

    sql """
    ALTER TABLE ${tableName} DROP PARTITION p202001;
    """
    sql "sync"

    qt_sql "SELECT * FROM ${tableName} ORDER BY id"
    qt_target_sql "SELECT * FROM ${tableName} ORDER BY id"

    logger.info("=== 3. resume ccr ===")
    httpTest {
        uri "/resume"
        endpoint syncerAddress
        def bodyJson = get_ccr_body "${tableName}"
        body "${bodyJson}"
        op "post"
        result response
    }

    qt_sql "SELECT * FROM ${tableName} ORDER BY id"
    qt_target_sql "SELECT * FROM ${tableName} ORDER BY id"

    logger.info("=== 4. insert and query again ===")
    sql """
    INSERT INTO ${tableName} (id, product_id, sale_date, quantity, revenue)
    VALUES
    (5, 105, '2020-02-20', 50, 550.0);
    """

    assertTrue(checkSelectTimesOf("SELECT * FROM ${tableName}", 1, 30))

    qt_sql "SELECT * FROM ${tableName} ORDER BY id"
    qt_target_sql "SELECT * FROM ${tableName} ORDER BY id"
}
