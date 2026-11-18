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
suite("test_cds_upsert_index") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    if (!helper.is_version_supported([30006, 20111, 20099])) {
        def version = helper.upstream_version()
        logger.info("skip this suite because version is not supported, upstream version ${version}")
        return
    }

    def tableName = "tbl_" + helper.randomSuffix()

    def exist = { res -> Boolean
        return res.size() != 0
    }

    sql "DROP TABLE IF EXISTS ${tableName}"
    target_sql "DROP TABLE IF EXISTS ${tableName}"

    sql """
        CREATE TABLE if NOT EXISTS ${tableName} 
        (
            `id` INT,
            `col1` INT,
            `col2` INT,
            `col3` INT,
            `col4` INT,
        )
        ENGINE=OLAP
        DISTRIBUTED BY HASH(id) BUCKETS 1 
        PROPERTIES ( 
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
    """

    helper.enableDbBinlog()
    helper.ccrJobDelete()
    helper.ccrJobCreate()

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 30))
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}\"", exist, 60, "sql"))
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE \"${tableName}\"", exist, 60, "target"))

    def first_job_progress = helper.get_job_progress()

    sql """ INSERT INTO ${tableName} VALUES (1, 1, 1, 1, 1) """
    sql """ INSERT INTO ${tableName} VALUES (1, 1, 1, 1, 1) """
    sql """ INSERT INTO ${tableName} VALUES (1, 1, 1, 1, 1) """
    sql """ INSERT INTO ${tableName} VALUES (1, 1, 1, 1, 1) """
    sql """ INSERT INTO ${tableName} VALUES (1, 1, 1, 1, 1) """
    sql """ INSERT INTO ${tableName} VALUES (1, 1, 1, 1, 1) """
    sql """ INSERT INTO ${tableName} VALUES (1, 1, 1, 1, 1) """
    sql """ INSERT INTO ${tableName} VALUES (1, 1, 1, 1, 1) """
    sql """ INSERT INTO ${tableName} VALUES (1, 1, 1, 1, 1) """
    sql """ INSERT INTO ${tableName} VALUES (1, 1, 1, 1, 1) """

    sql """
        CREATE MATERIALIZED VIEW mtr_${tableName}_full AS
        SELECT col1 as mv_col1, col3 as mv_col3 FROM ${tableName}
        """

    sql """ INSERT INTO ${tableName} VALUES (1, 1, 1, 1, 1) """
    sql """ INSERT INTO ${tableName} VALUES (1, 1, 1, 1, 1) """
    sql """ INSERT INTO ${tableName} VALUES (1, 1, 1, 1, 1) """
    sql """ INSERT INTO ${tableName} VALUES (1, 1, 1, 1, 1) """
    sql """ INSERT INTO ${tableName} VALUES (1, 1, 1, 1, 1) """
    sql """ INSERT INTO ${tableName} VALUES (1, 1, 1, 1, 1) """
    sql """ INSERT INTO ${tableName} VALUES (1, 1, 1, 1, 1) """
    sql """ INSERT INTO ${tableName} VALUES (1, 1, 1, 1, 1) """
    sql """ INSERT INTO ${tableName} VALUES (1, 1, 1, 1, 1) """
    sql """ INSERT INTO ${tableName} VALUES (1, 1, 1, 1, 1) """

    assertTrue(helper.checkShowTimesOf("SELECT count(*) FROM ${tableName}", { r -> r[0][0] = 20 }, 60, "target"))

    def checkViewExists = { res -> Boolean
        for (List<Object> row : res) {
            if ((row[1] as String).contains("mtr_${tableName}_full")) {
                return true
            }
        }
        return false
    }
    assertTrue(helper.checkShowTimesOf("""
                                SHOW CREATE MATERIALIZED VIEW mtr_${tableName}_full
                                ON ${tableName}
                                """,
                                checkViewExists, 30, "target"))

    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName}", 20, 50))

    def last_job_progress = helper.get_job_progress()
    assertTrue(last_job_progress.full_sync_start_at == first_job_progress.full_sync_start_at)
}

