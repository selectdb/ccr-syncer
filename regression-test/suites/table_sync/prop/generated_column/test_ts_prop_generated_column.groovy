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

suite("test_ts_prop_generated_column") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def dbName = context.dbName
    def tableName = "tbl_" + helper.randomSuffix()

    def exist = { res -> Boolean
        return res.size() != 0
    }

    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
    target_sql "DROP TABLE IF EXISTS TEST_${dbName}.${tableName}"

    helper.enableDbBinlog()

    sql """
            CREATE TABLE ${tableName} (
            product_id INT,
            price DECIMAL(10,2),
            quantity INT,
            total_value DECIMAL(10,2) GENERATED ALWAYS AS (price * quantity)
            ) DUPLICATE KEY(product_id) 
            DISTRIBUTED BY HASH(product_id) PROPERTIES ("replication_num" = "1")
    """

    sql """
            INSERT INTO ${tableName} VALUES(1, 10.00, 10, default);
        """

    sql """
            INSERT INTO ${tableName} (product_id, price, quantity) VALUES(1, 20.00, 10);
        """

    helper.ccrJobDelete(tableName)
    helper.ccrJobCreate(tableName)

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 30))

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}\"", exist, 60, "sql"))

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}\"", exist, 60, "target"))

    assertTrue(helper.checkShowTimesOf("SELECT * FROM ${tableName}", exist, 60, "sql"))

    assertTrue(helper.checkShowTimesOf("SELECT * FROM ${tableName}", exist, 60, "target"))

    def target_res = target_sql "SHOW CREATE TABLE ${tableName}"

    assertTrue(target_res[0][1].contains("`total_value` decimal(10,2) AS ((`price` * CAST(`quantity` AS decimalv3(10,0)))) NULL"))

    target_res = target_sql_return_maparray "select * from ${tableName} order by total_value"

    assertEquals(target_res[0].total_value,100.00)
    assertEquals(target_res[1].total_value,200.00)
}