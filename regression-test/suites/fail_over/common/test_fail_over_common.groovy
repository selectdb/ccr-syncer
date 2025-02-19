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

suite("test_fail_over_common") {
    def versions = sql_return_maparray "show variables like 'version_comment'"
    if (versions[0].Value.contains('doris-2.0.')) {
        logger.info("2.0 not support AUTO PARTITION, current version is: ${versions[0].Value}")
        return
    }

    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def test_num = 0
    def insert_num = 5
    def date_num = "2025-01-01"

    def createDuplicateTable = { tableName ->
       sql """
            CREATE TABLE if NOT EXISTS ${tableName}
            (
                `test` INT,
                `id` INT,
                `date_time` date NOT NULL
            )
            ENGINE=OLAP
            DUPLICATE KEY(`test`, `id`, `date_time`)
            AUTO PARTITION BY RANGE (date_trunc(`date_time`, 'day'))
            (
            )
            DISTRIBUTED BY HASH(id) BUCKETS AUTO
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "estimate_partition_size" = "10G",
                "binlog.enable" = "true"
            )
        """
    }

    def exist = { res -> Boolean
        return res.size() != 0
    }
    def notExist = { res -> Boolean
        return res.size() == 0
    }

    def suffix = helper.randomSuffix()
    def tableDuplicate0 = "tbl_duplicate_0_${suffix}"

    sql "DROP TABLE IF EXISTS ${tableDuplicate0}"
    target_sql "DROP TABLE IF EXISTS ${tableDuplicate0}"

    createDuplicateTable(tableDuplicate0)
    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${tableDuplicate0} VALUES (0, 99, '${date_num}')
            """
    }

    helper.enableDbBinlog()
    helper.ccrJobDelete()
    helper.ccrJobCreate()

    assertTrue(helper.checkRestoreFinishTimesOf("${tableDuplicate0}", 30))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableDuplicate0} WHERE test=${test_num}",
                                   insert_num, 30))

    logger.info("=== Test 1: dest cluster follow source cluster case ===")
    test_num = 1

    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${tableDuplicate0} VALUES (0, 99, '${date_num}')
            """
    }

    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableDuplicate0} WHERE test=0",
                                   insert_num * (test_num + 1), 30))


    logger.info("=== Test 3: desync job and fail over===")

    helper.ccrJobPause()
    helper.ccrJobDesync()

    def desync_res = target_sql "SHOW CREATE TABLE ${tableDuplicate0}"
    assertTrue(desync_res[0][1].contains("\"is_being_synced\" = \"false\""))

    test_num = 7
    date_num = "2025-01-02"
    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${tableDuplicate0} VALUES (${test_num}, ${index}, '${date_num}')
            """
    }

    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableDuplicate0} WHERE test=${test_num}", 0, 3))
 
    target_sql """
        INSERT INTO ${tableDuplicate0} VALUES (${test_num}, 2, '2025-02-01')
    """
    
    assertTrue(helper.checkShowTimesOf("SHOW PARTITIONS FROM ${tableDuplicate0} WHERE PartitionName LIKE 'p20250201000000'",
                                exist, 3, "target"))

}
