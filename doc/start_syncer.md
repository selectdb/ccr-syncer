# 启动选项
### 输出路径下的文件结构
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
### --daemon  
后台运行Syncer，默认为false
```bash
sh bin/start_syncer.sh --daemon
```
### --db_dir
Syncer会使用sqlite3来保存自身元数据，可以通过此选项来指定生成的db文件名及路径。  
默认路径为`SYNCER_OUTPUT_DIR/db`，文件名为`ccr.db`
```bash
sh bin/start_syncer.sh --db_dir /path/to/ccr.db
```
### --log_dir
日志的输出路径  
默认路径为`SYNCER_OUTPUT_DIR/log`，文件名为`ccr_syncer.log`
```bash
sh bin/start_syncer.sh --log_dir /path/to/ccr_syncer.log
```
### --log_level
用于指定Syncer日志的输出等级。
```bash
sh bin/start_syncer.sh --log_level info
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
