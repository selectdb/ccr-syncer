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

suite('test_ds_mv_basic') {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", 'helper.groovy'))

    def suffix = helper.randomSuffix()
    def tableName1 = "tbl_1_${suffix}"
    def tableName2 = "tbl_2_${suffix}"
    def mvName = "mv_${suffix}"

    def notExist = { result -> result.size() == 0 }

    sql """
        CREATE TABLE `${tableName1}` (
          `user_id` LARGEINT NOT NULL,
          `o_date` DATE NOT NULL,
          `num` SMALLINT NOT NULL
        ) ENGINE=OLAP
        COMMENT 'OLAP'
        AUTO PARTITION BY RANGE (date_trunc(`o_date`, 'day'))
        ()
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 2
        PROPERTIES (
            'replication_num' = '1',
            'binlog.enable' = 'true'
        )
        """
    sql """
        CREATE TABLE `${tableName2}` (
          `user_id` LARGEINT NOT NULL,
          `age` SMALLINT NOT NULL
        ) ENGINE=OLAP
        AUTO PARTITION BY LIST(`age`)
        ()
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 2
        PROPERTIES (
            'replication_num' = '1',
            'binlog.enable' = 'true'
        );
    """

    sql """ INSERT INTO ${tableName1} VALUES (1, "2017-01-01", 100) """
    sql """ INSERT INTO ${tableName1} VALUES (2, "2017-01-02", 101) """

    sql """ INSERT INTO ${tableName2} VALUES (1, '1') """
    sql """ INSERT INTO ${tableName2} VALUES (2, '2') """

    helper.enableDbBinlog()
    helper.ccrJobDelete()
    helper.ccrJobCreate()

    assertTrue(helper.checkRestoreFinishTimesOf(tableName1, 60))
    def first_job_progress = helper.get_job_progress()

    sql """
    CREATE MATERIALIZED VIEW ${mvName}
    BUILD DEFERRED REFRESH AUTO ON MANUAL
    partition by(`age`)
    DISTRIBUTED BY RANDOM BUCKETS 2
    PROPERTIES (
    'replication_num' = '1'
    )
    AS
    SELECT
        ${tableName1}.o_date as order_date,
        ${tableName2}.user_id as user_id,
        ${tableName1}.num,
        ${tableName2}.age
    FROM ${tableName1} join ${tableName2} on ${tableName1}.user_id=${tableName2}.user_id;
    """
    sql """ REFRESH MATERIALIZED VIEW ${mvName} AUTO """

    sql """ INSERT INTO ${tableName1} VALUES (3, "2017-02-03", 300) """
    sql """ INSERT INTO ${tableName2} VALUES (3, '3') """
    sql """ REFRESH MATERIALIZED VIEW ${mvName} AUTO """

    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName1}", 3, 30))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName2}", 3, 30))

    // TODO: we don't support sync materialized view yet
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE '${mvName}'", notExist, 30, 'target'))

    sql """ PAUSE MATERIALIZED VIEW JOB ON ${mvName} """
    sql """ INSERT INTO ${tableName1} VALUES (4, "2017-04-05", 400) """
    sql """ INSERT INTO ${tableName2} VALUES (4, '4') """

    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName1}", 4, 30))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName2}", 4, 30))

    sql """ RESUME MATERIALIZED VIEW JOB ON ${mvName} """

    sql """ REFRESH MATERIALIZED VIEW ${mvName} AUTO """

    sql """ ALTER MATERIALIZED VIEW ${mvName} RENAME new_${mvName} """

    sql """ INSERT INTO ${tableName1} VALUES (5, "2017-06-05", 500) """
    sql """ INSERT INTO ${tableName2} VALUES (5, '5') """
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName1}", 5, 30))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName2}", 5, 30))

    sql """
    CREATE MATERIALIZED VIEW ${mvName}
    BUILD DEFERRED REFRESH AUTO ON MANUAL
    partition by(`age`)
    DISTRIBUTED BY RANDOM BUCKETS 2
    PROPERTIES (
    'replication_num' = '1'
    )
    AS
    SELECT
        ${tableName1}.o_date as order_date,
        ${tableName2}.user_id as user_id,
        ${tableName1}.num,
        ${tableName2}.age
    FROM ${tableName1} join ${tableName2} on ${tableName1}.user_id=${tableName2}.user_id;
    """

    sql """
    ALTER MATERIALIZED VIEW ${mvName}
    REPLACE WITH MATERIALIZED VIEW new_${mvName} PROPERTIES ('swap' = 'true')
    """
    sql """ INSERT INTO ${tableName1} VALUES (7, "2017-07-05", 700) """
    sql """ INSERT INTO ${tableName2} VALUES (7, '7') """
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName1}", 6, 30))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName2}", 6, 30))

    sql """ ALTER MATERIALIZED VIEW new_${mvName} SET ("grace_period"="3000"); """
    sql """ INSERT INTO ${tableName1} VALUES (8, "2017-08-05", 800) """
    sql """ INSERT INTO ${tableName2} VALUES (8, '8') """
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName1}", 7, 30))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName2}", 7, 30))

    sql """ DROP MATERIALIZED VIEW new_${mvName} """
    sql """ INSERT INTO ${tableName1} VALUES (9, "2017-09-05", 900) """
    sql """ INSERT INTO ${tableName2} VALUES (9, '9') """
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName1}", 8, 30))
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName2}", 8, 30))

    // try drop base table partitions
    def partitions = sql_return_maparray """ SHOW PARTITIONS FROM ${tableName2} """
    def partitionName = partitions[0].PartitionName
    sql """ ALTER TABLE ${tableName2} DROP PARTITION ${partitionName} """
    assertTrue(helper.checkShowTimesOf(
        """ SHOW PARTITIONS FROM ${tableName2} WHERE PartitionName = "${partitionName}" """, notExist, 30))
    def result = sql """ SELECT * FROM ${tableName2} """
    def count = result.size()
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName2}", count, 30))

    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE 'new_${mvName}'", notExist, 30))
    assertTrue(helper.checkShowTimesOf("SHOW TABLES LIKE 'new_${mvName}'", notExist, 30, 'target'))

    // no fullsync are triggered
    def last_job_progress = helper.get_job_progress()
    assertTrue(last_job_progress.full_sync_start_at == first_job_progress.full_sync_start_at)
}

