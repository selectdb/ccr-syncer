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
### --db_dir  
Syncer会使用sqlite3来保存自身元数据，可以通过此选项来指定生成的db文件名及路径。  
默认路径为`SYNCER_OUTPUT_DIR/db`，文件名为`ccr.db`
```bash
bash bin/start_syncer.sh --db_dir /path/to/ccr.db
```
### --log_dir  
日志的输出路径  
默认路径为`SYNCER_OUTPUT_DIR/log`，文件名为`ccr_syncer.log`
```bash
bash bin/start_syncer.sh --log_dir /path/to/ccr_syncer.log
```
### --log_level  
用于指定Syncer日志的输出等级。
```bash
bash bin/start_syncer.sh --log_level info
```
日志的格式如下，其中hook只会在`log_level > info`的时候打印：
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