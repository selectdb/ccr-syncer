# Regression Test Considerations
## Steps to Run Tests
### １. Copy Test and CCR Interface Libraries
The regression tests for CCR require the regression test framework from doris/regression-test. Therefore, when running tests, we need to move the tests and CCR interfaces to the doris/regression-test directory.

Create a folder named ccr-syncer-test under the doris/regression-test/suites directory and copy the test files into this folder. Next, copy the files from ccr-syncer/regression-test/common to doris/regression-test/common. The framework for the tests is now set up.
### 2. Configure regression-conf.groovy (doris)
Add and configure the following in the configuration file based on the actual situation:
```bash
// JDBC configuration
jdbcUrl = "jdbc:mysql://127.0.0.1:9030/?"
targetJdbcUrl = "jdbc:mysql://127.0.0.1:9190/?
jdbcUser = "root"
jdbcPassword = ""

feSourceThriftAddress = "127.0.0.1:9220"
feTargetThriftAddress = "127.0.0.1:9220"
syncerAddress = "127.0.0.1:9190"
feSyncerUser = "root"
feSyncerPassword = ""
feHttpAddress = "127.0.0.1:8330"

// CCR configuration
ccrDownstreamUrl = "jdbc:mysql://172.19.0.2:9131/?"

ccrDownstreamUser = "root"

ccrDownstreamPassword = ""

ccrDownstreamFeThriftAddress = "127.0.0.1:9020"
```
### 3. Run the Tests
Ensure that at least one BE and FE are deployed for Doris and that CCR-Syncer is deployed before running the tests.
```bash
Run the tests using the Doris script
# --Run test cases with suiteName sql_action, currently suiteName equals the prefix of the file name, the example corresponds to the test file sql_action.groovy
./run-regression-test.sh --run sql_action
```
The steps to run the tests are now complete.
## Steps to Write Test Cases
### 1. Create Test Files
Navigate to the ccr-syncer/regression-test/suites directory and create folders based on the synchronization level. For example, for the DB level, go to the db_sync folder. Further divide the folders based on the synchronization object. For example, for the column object, go to the column folder. Divide the folders based on the actions on the object. For example, for the rename action, create a rename folder. Create the test file in this folder with a name prefixed by test followed by the sequence of directories entered, e.g., test_ds_col_rename represents the synchronization test for renaming a column at the DB level.

**Ensure there is only one test file in each smallest folder.**
### 2. Write the Test
CCR Interface Explanation:
```
    // Enable Binlog
    helper.enableDbBinlog()

    // Functions for creating, deleting, pausing, and resuming tasks support an optional parameter.
    // For example, to create a task. If empty, it defaults to creating a DB-level synchronization task with the target database as context.dbName.
    helper.ccrJobCreate()

    // If not empty, it creates a table-level synchronization task with the target database as context.dbName, target table as tableName.
    helper.ccrJobCreate(tableName)

    // Check if the SQL execution result matches the res_func function, where sql_type is "sql" (source cluster) or "target_sql" (target cluster), and time_out is the timeout duration.
    helper.checkShowTimesOf(sql, res_func, time_out, sql_type)
```
**注意事项**
```
1. Two clusters will be created during the test: SQL is sent to the upstream cluster, and target_sql is sent to the downstream cluster. Use target_sql for operations involving the target cluster.

2. Ensure the source database is not empty when creating a task, otherwise, the task creation will fail.

3. Perform checks on both upstream and downstream before and after modifying objects to ensure correctness.

4. Ensure the length of the automatically created dbName does not exceed 64.
```