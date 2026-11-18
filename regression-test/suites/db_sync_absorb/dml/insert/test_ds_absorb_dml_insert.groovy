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
suite("test_ds_absorb_dml_insert") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def tableName = "tbl_" + helper.randomSuffix()
    def test_num = 0
    def insert_num = 5

    def exist = { res -> Boolean
        return res.size() != 0
    }

    helper.enableDbBinlog()
    sql "DROP TABLE IF EXISTS ${tableName}"
    target_sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE if NOT EXISTS ${tableName}
        (
            `id` INT,
            `col1` INT,
            `col2` INT,
            `col3` INT,
            `col4` INT
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

    assertTrue(helper.checkShowTimesOf(""" SHOW TABLES LIKE "${tableName}" """, exist, 60, "target"))
    // 0. Insert N data
    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${tableName} VALUES (${index}, ${index+10}, ${index+20}, ${index+30}, ${index+40})
            """
    }

    assertTrue(helper.checkShowTimesOf(""" select * from ${tableName} """, { r -> r.size() == insert_num }, 60, "target"))
    // 1. Pause ccr job
    helper.ccrJobPause()

    // 2. Insert N data
    for (int index = insert_num; index < insert_num * 2; index++) {
        sql """
            INSERT INTO ${tableName} VALUES (${index}, ${index+10}, ${index+20}, ${index+30}, ${index+40})
            """
    }

    assertTrue(helper.checkShowTimesOf(""" select * from ${tableName} """, { r -> r.size() == insert_num * 2 }, 60, "sql"))

    // 3. Do operation & wait it finishes upstream - INSERT OVERWRITE
    sql """
        INSERT OVERWRITE TABLE ${tableName} VALUES 
        (100, 110, 120, 130, 140),
        (101, 111, 121, 131, 141),
        (102, 112, 122, 132, 142)
        """
    
    // Verify the data is overwritten on source
    assertTrue(helper.checkShowTimesOf(""" select * from ${tableName} """, { r -> r.size() == 3 }, 60, "sql"))
    
    // Verify the specific overwritten values exist
    assertTrue(helper.checkShowTimesOf(""" select * from ${tableName} where id = 100 """, { r -> r.size() == 1 }, 60, "sql"))

    // 4. Insert additional data after overwrite
    for (int index = 200; index < 200 + insert_num; index++) {
        sql """
            INSERT INTO ${tableName} VALUES (${index}, ${index+10}, ${index+20}, ${index+30}, ${index+40})
            """
    }

    assertTrue(helper.checkShowTimesOf(""" select * from ${tableName} """, { r -> r.size() == 3 + insert_num}, 60, "sql"))

    // 5. Force trigger fullsnapshot
    helper.force_fullsync()

    // 6. Resume ccr job
    helper.ccrJobResume()
  
    // 7. Verify data and operation are synced downstream
    assertTrue(helper.checkShowTimesOf(""" select * from ${tableName} """, { r -> r.size() == 3 + insert_num}, 60, "target"))
    
    // Verify overwritten data is properly synced
    assertTrue(helper.checkShowTimesOf(""" select * from ${tableName} where id = 100 """, { r -> r.size() == 1 }, 60, "target"))
    
    // Verify original data (that was overwritten) doesn't exist
    assertTrue(helper.checkShowTimesOf(""" select * from ${tableName} where id = 0 """, { r -> r.size() == 0 }, 60, "target"))
    
    // Verify additional data after overwrite
    assertTrue(helper.checkShowTimesOf(""" select * from ${tableName} where id = 200 """, { r -> r.size() == 1 }, 60, "target"))
}