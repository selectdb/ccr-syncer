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
suite("test_ts_col_drop") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def tableName = "tbl_" + helper.randomSuffix()
    def test_num = 0
    def insert_num = 5

    def exist = { res -> Boolean
        return res.size() != 0
    }

    def has_count = { count ->
        return { res -> Boolean
            res.size() == count
        }
    }

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE if NOT EXISTS ${tableName}
        (
            `test` INT,
            `id` INT,
            `value` INT
        )
        ENGINE=OLAP
        DUPLICATE KEY(`test`, `id`)
        DISTRIBUTED BY HASH(test) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
    """

    def values = [];
    for (int index = 0; index < insert_num; index++) {
        values.add("(${test_num}, ${index}, ${index})")
    }
    sql """
        INSERT INTO ${tableName} VALUES ${values.join(",")}
        """
    sql "sync"

    helper.ccrJobCreate(tableName)
    assertTrue(helper.checkRestoreFinishTimesOf("${tableName}", 30))

    logger.info("=== Test 1: drop key column ===")
    // binlog type: ALTER_JOB, binlog data:
    //  {
    //      "type":"SCHEMA_CHANGE",
    //      "dbId":11049,
    //      "tableId":11058,
    //      "tableName":"tbl_add_column6ab3b514b63c4368aa0a0149da0acabd",
    //      "jobId":11076,
    //      "jobState":"FINISHED",
    //      "rawSql": "ALTER TABLE `regression_test_schema_change`.`tbl_add_column6ab3b514b63c4368aa0a0149da0acabd` DROP COLUMN `id`"
    //  }
    sql """
        ALTER TABLE ${tableName}
        DROP COLUMN `id`
        """
    sql "sync"

    assertTrue(helper.checkShowTimesOf("""
                                SHOW ALTER TABLE COLUMN
                                FROM ${context.dbName}
                                WHERE TableName = "${tableName}" AND State = "FINISHED"
                                """,
                                has_count(1), 30))

    def id_column_not_exists = { res -> Boolean
        def not_exists = true
        for (int i = 0; i < res.size(); i++) {
            if (res[i][0] == 'id') {
                not_exists = false
            }
        }
        return not_exists
    }

    assertTrue(helper.checkShowTimesOf("SHOW COLUMNS FROM `${tableName}`", id_column_not_exists, 60, "target_sql"))

    logger.info("=== Test 2: drop value column ===")
    // binlog type: MODIFY_TABLE_ADD_OR_DROP_COLUMNS, binlog data:
    // {
    //   "dbId": 11049,
    //   "tableId": 11415,
    //   "indexSchemaMap": {
    //     "11433": [
    //       {
    //         "name": "test",
    //         "type": {
    //           "clazz": "ScalarType",
    //           "type": "INT",
    //           "len": -1,
    //           "precision": 0,
    //           "scale": 0
    //         },
    //         "isAggregationTypeImplicit": false,
    //         "isKey": true,
    //         "isAllowNull": true,
    //         "isAutoInc": false,
    //         "autoIncInitValue": -1,
    //         "comment": "",
    //         "stats": {
    //           "avgSerializedSize": -1.0,
    //           "maxSize": -1,
    //           "numDistinctValues": -1,
    //           "numNulls": -1
    //         },
    //         "children": [],
    //         "visible": true,
    //         "uniqueId": 0,
    //         "clusterKeyId": -1,
    //         "hasOnUpdateDefaultValue": false,
    //         "gctt": []
    //       }
    //     ]
    //   },
    //   "indexes": [],
    //   "jobId": 11444,
    //   "rawSql": "ALTER TABLE `regression_test_schema_change`.`tbl_drop_columnc84979beb0484120a5057fb2a3eeee6b` DROP COLUMN `value`"
    // }
    sql """
        ALTER TABLE ${tableName}
        DROP COLUMN `value`
        """
    sql "sync"

    assertTrue(helper.checkShowTimesOf("""
                                SHOW ALTER TABLE COLUMN
                                FROM ${context.dbName}
                                WHERE TableName = "${tableName}" AND State = "FINISHED"
                                """,
                                has_count(2), 30))

    def value_column_not_exists = { res -> Boolean
        def not_exists = true
        for (int i = 0; i < res.size(); i++) {
            if (res[i][0] == 'value') {
                not_exists = false
            }
        }
        return not_exists
    }

    assertTrue(helper.checkShowTimesOf("SHOW COLUMNS FROM `${tableName}`", value_column_not_exists, 60, "target_sql"))
}

