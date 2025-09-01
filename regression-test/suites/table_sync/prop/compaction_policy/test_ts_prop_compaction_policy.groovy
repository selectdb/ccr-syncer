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

suite("test_ts_prop_compaction_policy") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def dbName = context.dbName
    def tableName = "tbl_" + helper.randomSuffix()

    def test_num = 0
    def insert_num = 5

    def exist = { res -> Boolean
        return res.size() != 0
    }

    def checkShowResult = { res, property -> Boolean
        if(!res[0][1].contains(property)){
            logger.info("don't contains {}", property)
            return false
        }
        return true 
    }

    def existCompaction = { res -> Boolean
        assertTrue(checkShowResult(res, "\"compaction_policy\" = \"time_series\""))
        assertTrue(checkShowResult(res, "\"time_series_compaction_goal_size_mbytes\" = \"2048\""))
        assertTrue(checkShowResult(res, "\"time_series_compaction_file_count_threshold\" = \"3000\""))
        assertTrue(checkShowResult(res, "\"time_series_compaction_time_threshold_seconds\" = \"4000\""))
        assertTrue(checkShowResult(res, "\"time_series_compaction_empty_rowsets_threshold\" = \"6\""))
        assertTrue(checkShowResult(res, "\"time_series_compaction_level_threshold\" = \"2\""))
        return true
    }

    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
    target_sql "DROP TABLE IF EXISTS TEST_${dbName}.${tableName}"

    helper.enableDbBinlog()

    sql """
        CREATE TABLE if NOT EXISTS ${tableName}
        (
            `test` INT,
            `id` INT
        )
        ENGINE=OLAP
        AGGREGATE KEY(`test`, `id`)
        PARTITION BY RANGE(`id`)
        (
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true",
            "compaction_policy" = "time_series",
            "time_series_compaction_goal_size_mbytes" = "2048",
            "time_series_compaction_file_count_threshold" = "3000",
            "time_series_compaction_time_threshold_seconds" = "4000",
            "time_series_compaction_empty_rowsets_threshold" = "6",
            "time_series_compaction_level_threshold" = "2"
        )
    """

    helper.ccrJobDelete(tableName)
    helper.ccrJobCreate(tableName)

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 30))

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}\"", exist, 60, "sql"))

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}\"", exist, 60, "target"))

    assertTrue(helper.checkShowTimesOf("SHOW CREATE TABLE ${tableName}", existCompaction, 60, "sql"))

}