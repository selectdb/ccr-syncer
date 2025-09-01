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

suite("test_ts_tbl_res_variant") {
    def versions = sql_return_maparray "show variables like 'version_comment'"
    if (versions[0].Value.contains('doris-2.0.')) {
        logger.info("2.0 not support variant case, current version is: ${versions[0].Value}")
        return
    }

    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def tableName = "test_" + helper.randomSuffix()
    def insert_num = 5

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
    helper.ccrJobCreate(tableName)

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 30))
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

