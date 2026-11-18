# 回归测试注意事项
## 运行测试的步骤
### １. 复制测试及 ccr 接口库
CCR 的回归测试需要用到 doris/regression-test 的回归测试框架, 所以我们运行测试时需要将测试和 ccr 接口迁移到doris/regression-test 目录下<br>
在 doris/regression-test/suites 目录下建立文件夹 ccr-syncer-test, 将测试文件复制到此文件夹, 其次将 ccr-syncer/regression-test/common 下的文件复制到 doris/regression-test/comman 目录下, 至此测试前的框架已经搭好
### 2. 配置 regression-conf.groovy
根据实际情况在配置文件中添加如下并配置 jdbc fe ccr
```bash
// Jdbc配置
jdbcUrl = "jdbc:mysql://127.0.0.1:9030/?"
targetJdbcUrl = "jdbc:mysql://127.0.0.1:9190/?
jdbcUser = "root"
jdbcPassword = ""

feSourceThriftAddress = "127.0.0.1:9020"
feTargetThriftAddress = "127.0.0.1:9020"
syncerAddress = "127.0.0.1:9190"
feSyncerUser = "root"
feSyncerPassword = ""
feHttpAddress = "127.0.0.1:8030"

// ccr配置
ccrDownstreamUrl = "jdbc:mysql://172.19.0.2:9131/?"

ccrDownstreamUser = "root"

ccrDownstreamPassword = ""

ccrDownstreamFeThriftAddress = "127.0.0.1:9020"
```
### 3. 运行测试
在运行测试前确保 doris 至少一个 be, fe 部署完成, 确保 ccr-syncer 部署完成
```bash
使用 doris 脚本运行测试
# --测试suiteName为sql_action的用例, 目前suiteName等于文件名前缀, 例子对应的用例文件是sql_action.groovy
./run-regression-test.sh --run sql_action
```
至此运行测试的步骤已完成
## 编写测试用例的步骤
### 1. 创建测试文件
进入 ccr-syncer/regressioon-test/suites 目录, 根据同步级别划分文件夹, 以db级别为例, 进入 db_sync 文件夹, 根据同步对象划分文件夹, 以 column 为例, 进入 column 文件夹, 根据对对象的行为划分文件夹, 以rename为例, 创建 rename 文件夹, 在此文件夹下创建测试, 文件名为 test 前缀加依次进入目录的顺序, 例如 test_ds_col_rename 代表在db级别下 rename column 的同步测试
**确保在每个最小文件夹下只有一个测试文件**
### 2. 编写测试
ccr 接口说明
```
    // 开启Binlog
    helper.enableDbBinlog()

    // 创建、删除、暂停、恢复任务等函数支持一个可选参数。
    // 以创建任务为例, 参数为 tableName, 参数为空时, 默认创建db级别同步任务, 目标数据库为context.dbName
    helper.ccrJobCreate()

    // 不为空时创建 tbl 级别同步任务, 目标数据库为context.dbName, 目标表为 tableName
    helper.ccrJobCreate(tableName)

    // 检测 sql 运行结果是否符合 res_func函数, sql_type 为 "sql" (源集群) 或 "target_sql" (目标集群), time_out 为超时时间
    helper.checkShowTimesOf(sql, res_func, time_out, sql_type)
```
**注意事项**
```
1. 测试时会建两个集群, sql 发给上游集群, target_sql 发给下游集群, 涉及到目标集群的需要用 target_sql

2. 创建任务时确保源数据库不为空, 否则创建任务会失败

3. 在修改对象前后都需要对上下游进行 check 保证结果正确

4. 确保测试自动创建的 dbName 的长度不超过 64
```