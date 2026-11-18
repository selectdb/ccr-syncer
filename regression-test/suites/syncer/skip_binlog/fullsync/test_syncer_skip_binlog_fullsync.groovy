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

suite('test_syncer_skip_binlog_fullsync') {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", 'helper.groovy'))

    def tableName = 'tbl_' + helper.randomSuffix()
    def insert_num = 10

    def exist = { res -> Boolean
        return res.size() != 0
    }
    def notExist = { res -> Boolean
        return res.size() == 0
    }

    sql """
        CREATE TABLE if NOT EXISTS ${tableName}
        (
            `test` INT,
            `id` INT
        )
        ENGINE=OLAP
        UNIQUE KEY(`test`, `id`)
        PARTITION BY RANGE(`id`)
        (
            PARTITION p0 VALUES LESS THAN ("0"),
            PARTITION p1 VALUES LESS THAN ("10"),
            PARTITION p2 VALUES LESS THAN ("20")
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
    """

    List<String> values = []
    for (int index = 0; index < insert_num; index++) {
        values.add("(0, ${index})")
    }

    sql """ INSERT INTO ${tableName} VALUES ${values.join(',')} """

    sql 'sync'

    helper.removeFailpoint('handle_binlog_failed')
    helper.enableDbBinlog()
    helper.ccrJobDelete()
    helper.ccrJobCreate()

    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 60))

    sql """ INSERT INTO ${tableName} VALUES (1, 10) """
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName}", insert_num + 1, 5))

    helper.ccrJobPause()
    helper.addFailpoint('handle_binlog_failed', 'true')

    def first_job_progress = helper.get_job_progress()

    sql """ INSERT INTO ${tableName} VALUES (2, 10) """

    helper.ccrJobResume()
    assertFalse(helper.checkSelectTimesOf("SELECT * FROM ${tableName}", insert_num + 2, 5))

    helper.forceSkipBinlogBy('fullsync')
    assertTrue(helper.checkSelectTimesOf("SELECT * FROM ${tableName}", insert_num + 2, 30))
    helper.removeFailpoint('handle_binlog_failed')

    // A snapshot are triggered by the full sync
    def last_job_progress = helper.get_job_progress()
    assertTrue(last_job_progress.full_sync_start_at > first_job_progress.full_sync_start_at)
}
