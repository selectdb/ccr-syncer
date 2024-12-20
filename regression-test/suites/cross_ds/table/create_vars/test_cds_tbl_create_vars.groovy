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
suite('test_cds_tbl_create_vars') {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", 'helper.groovy'))

    def tableName = 'tbl_' + helper.randomSuffix()
    def exist = { res -> Boolean
        return res.size() != 0
    }
    def notExist = { res -> Boolean
        return res.size() == 0
    }

    helper.enableDbBinlog()
    helper.ccrJobDelete()
    helper.ccrJobCreate()

    try {
        logger.info('create table with agg state')
        sql 'set enable_agg_state=true'
        sql """
        CREATE TABLE ${tableName}_agg (
          `k1` largeint NOT NULL,
          `k2` varchar(20) NULL,
          `v_sum` bigint SUM NULL DEFAULT "0",
          `v_max` int MAX NULL DEFAULT "0",
          `v_min` int MIN NULL DEFAULT "99999",
          `v_generic` agg_state<avg(int null)> GENERIC NOT NULL,
          `v_hll` hll HLL_UNION NOT NULL,
          `v_bitmap` bitmap BITMAP_UNION NOT NULL DEFAULT BITMAP_EMPTY,
          `v_quantile_union` quantile_state QUANTILE_UNION NOT NULL
        ) ENGINE=OLAP
        AGGREGATE KEY(`k1`, `k2`)
        COMMENT 'OLAP'
        DISTRIBUTED BY RANDOM BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "min_load_replica_num" = "-1",
        "light_schema_change" = "true",
        "binlog.enable" = "true"
        )
        """

        assertTrue(helper.checkShowTimesOf("SHOW CREATE TABLE ${tableName}_agg", exist, 30, 'target'))
    } catch (Exception e) {
        logger.warn("create table with agg state failed: ${e}")
    }

    try {
        logger.info('create table with decimal 256')
        sql 'set enable_decimal256 = true'
        sql """
        CREATE TABLE ${tableName}_decimal_256 (
          `id1` int NULL,
          `id2` int NULL,
          `result` decimal(76,20) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id1`, `id2`)
        DISTRIBUTED BY HASH(`id1`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "min_load_replica_num" = "-1",
        "light_schema_change" = "true",
        "binlog.enable" = "true"
        )
        """
        assertTrue(helper.checkShowTimesOf("SHOW CREATE TABLE ${tableName}_decimal_256", exist, 30, 'target'))
    } catch (Exception e) {
        logger.warn("create table with decimal 256 failed: ${e}")
    }

    try {
        logger.info('create table with unicode column name')
        sql '''set enable_unicode_name_support=true'''
        sql """
        CREATE TABLE ${tableName}_unicode_column (
            `k1` int NULL,
            `名称` text NULL,
            `k3` char(50) NULL,
            `k4` varchar(200) NULL,
            `k5` datetime NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "light_schema_change" = "true",
        "binlog.enable" = "true"
        );
        """
        assertTrue(helper.checkShowTimesOf("SHOW CREATE TABLE ${tableName}_unicode_column", exist, 30, 'target'))
    } catch (Exception e) {
        logger.warn("create table with unicode column failed: ${e}")
    }
}
