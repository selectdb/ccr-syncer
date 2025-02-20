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

suite("test_ds_view_modify_comment") {
    def helper = new GroovyShell(new Binding(['suite': delegate]))
            .evaluate(new File("${context.config.suitePath}/../common", "helper.groovy"))

    def viewName = "test_ds_view_modify_comment_view"

    def exist = { res -> Boolean
        return res.size() != 0
    }
    def notExist = { res -> Boolean
        return res.size() == 0
    }

    def existNewComment = { res -> Boolean
        return res[0][1].contains("COMMENT 'doris_modify_view_comment'")
    }

    sql """DROP VIEW IF EXISTS ${viewName}"""
    target_sql """DROP VIEW IF EXISTS ${viewName}"""

    sql """ 
        create view ${viewName} as select 1,to_base64(AES_ENCRYPT('doris','doris')); 
    """

    helper.enableDbBinlog()

    helper.ccrJobDelete()
    helper.ccrJobCreate()

    assertTrue(helper.checkRestoreFinishTimesOf("${viewName}", 30))

    assertTrue(helper.checkShowTimesOf("SHOW VIEWS LIKE \"${viewName}\"", exist, 30, "target_sql"))

    sql "ALTER VIEW ${viewName} MODIFY COMMENT \"doris_modify_view_comment\""

    assertTrue(helper.checkShowTimesOf("SHOW CREATE TABLE ${viewName}", existNewComment, 30, "target_sql"))
}
