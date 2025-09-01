# 启动说明
根据配置选项启动Syncer，并且在默认或指定路径下保存一个pid文件，pid文件的命名方式为`host_port.pid`。  
## 输出路径下的文件结构
在编译完成后的输出路径下，文件结构大致如下所示：
```
output_dir
    bin
        ccr_syncer
        enable_db_binlog.sh
        start_syncer.sh
        stop_syncer.sh
    db
        [ccr.db] # 默认配置下运行后生成
    log
        [ccr_syncer.log] # 默认配置下运行后生成
```
**后文中的start_syncer.sh指的是该路径下的start_syncer.sh！！！**
## 启动选项
### --daemon   
后台运行Syncer，默认为false
```bash
bash bin/start_syncer.sh --daemon
```

### --config_file
该配置文件指定CCR Syncer元数据库的信息，文件内容包括db_type，db_host，db_port，db_user，db_password。默认值是db.conf
使用该选项启动CCR Syncer进程后，**不应再使用db_type，db_host，db_port，db_user，db_password来指定元数据信息**
```bash
bash bin/start_syncer.sh --config_file /path/to/db.conf
```

### --db_type  
Syncer目前能够使用两种数据库来保存自身的元数据，分别为`sqlite3`（对应本地存储）和`mysql` 或者`postgresql`（本地或远端存储）
```bash
bash bin/start_syncer.sh --db_type mysql
```
默认值为sqlite3  
在使用mysql或者postgresql存储元数据时，Syncer会使用`CREATE IF NOT EXISTS`来创建一个名为`ccr`的库，ccr相关的元数据表都会保存在其中

### --db_dir  
**这个选项仅在db使用`sqlite3`时生效**  
可以通过此选项来指定sqlite3生成的db文件名及路径。  
```bash
bash bin/start_syncer.sh --db_dir /path/to/ccr.db
```
默认路径为`SYNCER_OUTPUT_DIR/db`，文件名为`ccr.db`
### --db_host & db_port & db_user & db_password
**这个选项仅在db使用`mysql`或者`postgresql`时生效**  
```bash
bash bin/start_syncer.sh --db_host 127.0.0.1 --db_port 3306 --db_user root --db_password "qwe123456"
```
db_host、db_port的默认值如例子中所示，db_user、db_password默认值为空
### --log_dir  
日志的输出路径  
```bash
bash bin/start_syncer.sh --log_dir /path/to/ccr_syncer.log
```
默认路径为`SYNCER_OUTPUT_DIR/log`，文件名为`ccr_syncer.log`
### --log_level  
用于指定Syncer日志的输出等级。
```bash
bash bin/start_syncer.sh --log_level info
```

```
#        time         level        msg                  hooks
[2023-07-18 16:30:18] TRACE This is trace type. ccrName=xxx line=xxx
[2023-07-18 16:30:18] DEBUG This is debug type. ccrName=xxx line=xxx
[2023-07-18 16:30:18]  INFO This is info type. ccrName=xxx line=xxx
[2023-07-18 16:30:18]  WARN This is warn type. ccrName=xxx line=xxx
[2023-07-18 16:30:18] ERROR This is error type. ccrName=xxx line=xxx
[2023-07-18 16:30:18] FATAL This is fatal type. ccrName=xxx line=xxx
```
在--daemon下，log_level默认值为`info`  
在前台运行时，log_level默认值为`trace`，同时日志会通过 tee 来保存到log_dir

### --host && --port  
用于指定Syncer的host和port，其中host只起到在集群中的区分自身的作用，可以理解为Syncer的name，集群中Syncer的名称为`host:port`  
```bash
bash bin/start_syncer.sh --host 127.0.0.1 --port 9190
```
host默认值为127.0.0.1，port的默认值为9190  

### --pid_dir  
用于指定pid文件的保存路径  
pid文件是stop_syncer.sh脚本用于关闭Syncer的凭据，里面保存了对应Syncer的进程号，为了方便Syncer的集群化管理，可以指定pid文件的保存路径  
```bash
bash bin/start_syncer.sh --pid_dir /path/to/pids
```
默认值为`SYNCER_OUTPUT_DIR/bin`

### --commit_txn_timeout
用于指定提交事务超时时间
```bash
bash bin/start_syncer.sh --commit_txn_timeout 33s
```
默认值为33s

### --connect_timeout duration
用于指定连接超时时间
```bash
bash bin/start_syncer.sh --connect_timeout 10s
```
默认值为1s

### --local_repo_name string
用于指定本地仓库名称
```bash
bash bin/start_syncer.sh --local_repo_name "repo_name"
```
默认值为""

### --rpc_timeout duration
用于指定rpc超时时间
```bash
bash bin/start_syncer.sh --rpc_timeout 30s
```
默认值为3s
