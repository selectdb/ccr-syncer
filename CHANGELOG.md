# 更新日志

### Fix

## 3.0.4/2.1.8

注意：从这个版本开始 doris 和 ccr-syncer 的 2.0 版本将不再更新，需要使用 ccr-syncer 的需要先升级到 2.1 及以上版本。


这次引入了一个 behaviour change: 创建同步 JOB，需要上游的表开启 `light_schema_change` 属性 (selectdb/ccr-syncer#283)。

### Fix

- 过滤 partial sync 期间删除的 table (selectdb/ccr-syncer#330)
- 过滤依赖 UDF 的建表语句 (selectdb/ccr-syncer#328)
- 修复 view signature not matched 导致 fullsync 无法继续的问题 (selectdb/ccr-syncer#329)
- 修复 create table sql 的语法错误 (selectdb/ccr-syncer#292)
- 修复 syncer crash 导致 database deadlock 的问题 (selectdb/ccr-syncer#294)
- 修复 alter column default value CURRENT_TIMESTAMP 的语法错误 (selectdb/ccr-syncer#293)
- 修复 inverted index 上下游 ID 不一致的问题，需要修改 doris 配置 `restore_reset_index_id=false` (selectdb/ccr-syncer#306, selectdb/ccr-syncer#332)
- 修复 RPC 连接泄漏的问题 (selectdb/ccr-syncer#299)
- 修复 fullsync with views commit seq 没有更新的问题 (selectdb/ccr-syncer#297)
- 支持同时 add/drop 多个 inverted index (selectdb/ccr-syncer#296)
- 通过 partial sync 同步部分依赖 session variable 的 create table sql (selectdb/ccr-syncer#286,selectdb/ccr-syncer#331)
- 修复 create table 语句 infinity partition key 语法错误的问题 (selectdb/ccr-syncer#284)
- 修复处理 upsert binlog 时因 fe meta 变化而触发全量同步的问题 (selectdb/ccr-syncer#282)
- 修复 table name 中带 `-` 无法同步的问题 (selectdb/ccr-syncer#168)
- 修复部分同步下可能同步多次增量数据的问题 (selectdb/ccr-syncer#186)
- 修复 create 又立即 drop 的情况下无法找到 table 的问题 (selectdb/ccr-syncer#188)
- 跳过不支持的 table 类型，比如 ES TABLE
- 避免在同步快照、binlog 期间对上游 name 产生依赖 (selectdb/ccr-syncer#205, selectdb/ccr-syncer#239)
- 修复全量同步期间 view 的别名问题 (selectdb/ccr-syncer#207)
- 修复 add partition with keyword name 的问题 (selectdb/ccr-syncer#212)
- 跳过 drop tmp partition (selectdb/ccr-syncer#214)
- 修复快照过期的问题，过期后会重做 (selectdb/ccr-syncer#229)
- 修复 rename 导致的上下游 index name 无法匹配的问题 (selectdb/ccr-syncer#235)
- 修复并行创建 table/backup 时 table 丢失的问题 (selectdb/ccr-syncer#237)
- 修复 partial snapshot 期间，上游 table/partition 已经被删除/重命名/替换的问题 (selectdb/ccr-syncer#240, selectdb/ccr-syncer#241, selectdb/ccr-syncer#249, selectdb/ccr-syncer#255)
- 检查 database connection 错误 (selectdb/ccr-syncer#247)
- 过滤已经被删除的 table (selectdb/ccr-syncer#248)
- 修复 create table 时下游 table 已经存在的问题 (selectdb/ccr-syncer#161)

### Feature

- 支持在创建 job 时设置上 reuse_binlog_label，ingest 时会直接使用上游的 label (selectdb/ccr-syncer#324)
- 支持在创建 job 时设置上 private/public IP 的映射 (selectdb/ccr-syncer#288)
- 支持 atomic restore，全量同步期间下游仍然可读 (selectdb/ccr-syncer#166)
- 支持处理包装在 barrier log 中的其他 binlog （主要用于在 2.0/2.1 上增加新增的 binlog 类型）(selectdb/ccr-syncer#208)
- 支持 rename table (2.1) (selectdb/ccr-syncer#209)
- 跳过 modify partition binlog (selectdb/ccr-syncer#213)
- 支持 modify comment binlog (selectdb/ccr-syncer#140)
- 支持 replace table binlog (selectdb/ccr-syncer#245)
- 支持 drop view binlog (selectdb/ccr-syncer#138)
- 支持 modify view def binlog (selectdb/ccr-syncer#184)
- 支持 inverted index 相关 binlog (selectdb/ccr-syncer#252)
- 支持 table sync 下的 txn insert (WIP) (selectdb/ccr-syncer#234, selectdb/ccr-syncer#259)
- 支持 rename partition/rollup binlogs (selectdb/ccr-syncer#268)
- 支持 add/drop rollup binlogs (selectdb/ccr-syncer#269)
- 支持 modify view/comment in 2.1 (selectdb/ccr-syncer#270, selectdb/ccr-syncer#273)
- 支持 table sync 下的 replace table (selectdb/ccr-syncer#279)

### Improve

- 允许设置 mysql/doris connection 数量限制 (selectdb/ccr-syncer#305,selectdb/ccr-syncer#314,selectdb/ccr-syncer#317)
- 优化 /get_lag 接口，避免阻塞 (selectdb/ccr-syncer#311)
- 支持同步 rename column，需要 doris xxxx (selectdb/ccr-syncer#139)
- 支持在全量同步过程中，遇到 table signature 不匹配时，使用 alias 替代 drop (selectdb/ccr-syncer#179)
- 增加 monitor，在日志中 dump 内存使用率 (selectdb/ccr-syncer#181)
- 过滤 schema change 删除的 indexes，避免全量同步 (selectdb/ccr-syncer#185)
- 过滤 schema change 创建的 shadow indexes 的更新，避免全量同步 (selectdb/ccr-syncer#187)
- 增加 `mysql_max_allowed_packet` 参数，控制 mysql sdk 允许发送的 packet 大小 (selectdb/ccr-syncer#196)
- 限制一个 JOB 中单个 BE 的 ingest 并发数，减少对 BE 的连接数和文件描述符消耗 (selectdb/ccr-syncer#195)
- 避免在获取 job status 等待锁 (selectdb/ccr-syncer#198)
- 避免 backup/restore 任务阻塞查询 ccr job progress (selectdb/ccr-syncer#201, selectdb/ccr-syncer#206)
- 避免将 snapshot job info 和 meta （这两个数据可能非常大）持久化到 mysql 中 (selectdb/ccr-syncer#204)
- 上游 db 中没有 table 时，打印 info 而不是 error (selectdb/ccr-syncer#211)
- 在 ccr syncer 重启后，复用由当前 job 发起的 backup/restore job (selectdb/ccr-syncer#218, selectdb/ccr-syncer#224, selectdb/ccr-syncer#226)
- 支持读取压缩后的快照/恢复快照时压缩，避免碰到 thrift max message size 限制 (selectdb/ccr-syncer#223)
- API job_progress 避免返回 persist data (selectdb/ccr-syncer#271)

## 2.0.15/2.1.6

### Fix

- 修复 `REPLACE_IF_NOT_NULL` 语句的默认值语法不兼容问题 (selectdb/ccr-syncer#180)
- 修复 table sync 下 partial snapshot 没有更新 dest table id 的问题 (selectdb/ccr-syncer#178)
- **修复 table sync with alias 时，lightning schema change 找不到 table 的问题** (selectdb/ccr-syncer#176)
- 修复 db sync 下 partial snapshot table 为空的问题 (selectdb/ccr-syncer#173)
- 修复 create table 时下游 view 已经存在的问题（先删除 view），feature gate: `feature_create_view_drop_exists` (selectdb/ccr-syncer#170,selectdb/ccr-syncer#171)
- 修复 table not found 时没有 rollback binlog 的问题
- **修复下游删表后重做 snapshot 是 table mapping 过期的问题 (selectdb/ccr-syncer#162,selectdb/ccr-syncer#163,selectdb/ccr-syncer#164)**
- 修复 full sync 期间 view already exists 的问题，如果 signature 不匹配会先删除 (selectdb/ccr-syncer#152)
- 修复 2.0 中 get view 逻辑，兼容 default_cluster 语法 (selectdb/ccr-syncer#149)
- 修复 job state 变化时仍然更新了 job progress 的问题，对之前的逻辑无影响，主要用于支持 partial sync (selectdb/ccr-syncer#124)
- 修复 get_lag 接口中不含 lag 的问题 (selectdb/ccr-syncer#126)
- 修复下游 restore 时未清理 orphan tables/partitions 的问题 (selectdb/ccr-syncer#128)
    - 备注： 暂时禁用，因为 doris 侧发现了 bug (selectdb/ccr-syncer#153,selectdb/ccr-syncer#161)
- **修复下游删表后重做 snapshot 时 dest meta cache 过期的问题 (selectdb/ccr-syncer#132)**

### Feature

- 增加 `/force_fullsync` 用于强制触发 fullsync (selectdb/ccr-syncer#167)
- 增加 `/features` 接口，用于列出当前有哪些 feature 以及是否打开 (selectdb/ccr-syncer#175)
- 支持同步 drop view（drop table 失败后使用 drop view 重试）(selectdb/ccr-syncer#169)
- 支持同步 rename 操作 (selectdb/ccr-syncer#147)
- schema change 使用 partial sync 而不是 fullsync (selectdb/ccr-syncer#151)
- partial sync 使用 rename 而不是直接修改 table，因此表的读写在同步过程中不受影响 (selectdb/ccr-syncer#148)
- 支持 partial sync，减少需要同步的数据量 (selectdb/ccr-syncer#125)
- 添加参数 `allowTableExists`，允许在下游 table 存在时，仍然创建 ccr job（如果 schema 不一致，会自动删表重建）(selectdb/ccr-syncer#136)

### Improve

- 日志输出 milliseconds (selectdb/ccr-syncer#182)
- 如果下游表的 schema 不一致，则将表移动到 RecycleBin 中（之前是强制删除）(selectdb/ccr-syncer#137)

## 2.0.14/2.1.5

### Fix

- 过滤已经删除的 partitions，避免 full sync，需要 doris 2.0.14/2.1.5 (selectdb/ccr-syncer#117)
- 过滤已经删除的 tables，避免 full sync (selectdb/ccr-syncer#123)
- 兼容 doris 3.0 alternative json name，doris 3.0 必须使用该版本的 CCR syncer (selectdb/ccr-syncer#121)
- 修复 list jobs 接口在高可用环境下不可用的问题 (selectdb/ccr-syncer#120)

## 2.0.11

对应 doris 2.0.11。

### Feature

- 支持以 postgresql 作为 ccr-syncer 的元数据库 (selectdb/ccr-syncer#77)
- 支持 insert overwrite 相关操作 (selectdb/ccr-syncer#97,selectdb/ccr-syncer#99)

### Fix

- 修复 drop partition 后因找不到 partition id 而无法继续同步的问题 (selectdb/ccr-syncer#82)
- 修复高可用模式下接口无法 redirect 的问题 (selectdb/ccr-syncer#81)
- 修复 binlog 可能因同步失败而丢失的问题 (selectdb/ccr-syncer#86,selectdb/ccr-syncer#91)
- 修改 connect 和 rpc 超时时间默认值，connect 默认 10s，rpc 默认 30s (selectdb/ccr-syncer#94,selectdb/ccr-syncer#95)
- 修复 view 和 materialized view 使用造成空指针问题 (selectdb/ccr-syncer#100)
- 修复 add partition sql 错误的问题 (selectdb/ccr-syncer#99)


## 2.1.3/2.0.3.10

### Fix

- 修复因与上下游 FE 网络中断而触发 full sync 的问题

### Feature

- 增加 `/job_progress` 接口用于获取 JOB 进度
- 增加 `/job_details` 接口用于获取 JOB 信息
- 保留 job 状态变更的各个时间点，并在 `/job_progress` 接口中展示

### Fix

- 修复若干 keywords 没有 escape 的问题

## 2.0.3.9

配合 doris 2.0.9 版本

### Feature

- 添加选项以启动 pprof server
- 允许配置 rpc 合 connection 超时

### Fix

- restore 每次重试时使用不同的 label 名
- update table 失败时（目标表不存在）会触发快照同步
- 修复同步 sql 中包含关键字的问题
- 如果恢复时碰到表 schema 发生变化，会先删表再重试恢复

## 0.5

### 支持高可用
- 现在可以部署多个Syncer节点来保证CCR功能的高可用。
- db是Syncer集群划分的依据，同一个集群下的Syncer共用一个db。
- Syncer集群采用对称设计，每个Syncer都会相对独立的执行被分配到的job。在某个Syncer节点down掉后，它的jobs会依据负载均衡算法被分给其他Syncer节点。

## 0.4
* 增加 enable_db_binlog.sh 方便用户对整库开启binlog

## 0.3

### LOG

- 更新日志格式，提高日志可读性，现在日志的格式如下，其中hook只会在 `log_level > info`的时候打印：

  ```bash
  #        time         level        msg                  hooks
  [2023-07-18 16:30:18] TRACE This is trace type. ccrName=xxx line=xxx
  [2023-07-18 16:30:18] DEBUG This is debug type. ccrName=xxx line=xxx
  [2023-07-18 16:30:18]  INFO This is info type. ccrName=xxx line=xxx
  [2023-07-18 16:30:18]  WARN This is warn type. ccrName=xxx line=xxx
  [2023-07-18 16:30:18] ERROR This is error type. ccrName=xxx line=xxx
  [2023-07-18 16:30:18] FATAL This is fatal type. ccrName=xxx line=xxx
  ```
- 现在可以指定log的等级和log文件的路径  
  `--log_level <level>`：  
  level可以是trace、debug、info、warn、error、fatal，log的数量依次递减。默认值为 `info`  
  
  `--log_dir </PATH/TO/LOG/FILE>`：  
  log文件路径包括路径+文件名，如：/var/myfile.log，默认值为 `log/ccr-syncer.log`  
  
  例：  

  ```bash
  sh start_syncer.sh --daemon --log_level trace --log_dir /PATH/TO/LOG/FILE
  ```
- 非守护进程状态下会在日志打印到终端的同时利用tee将其保存在 `log_dir`中
- 在日志中屏蔽了用户的敏感信息

### BD

- 现在可以指定syncer持久化DB的文件路径  
  `--db_dir </PATH/TO/DB/FILE>`：  
  DB文件路径包括路径+文件名，如：/var/myccr.db，默认值为 `db/ccr.db`  
  
  例：  

  ```bash
  sh start_syncer.sh --daemon --db_dir /PATH/TO/DB/FILE
  ```
