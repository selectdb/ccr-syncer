# 停止说明
根据默认或指定路径下pid文件中的进程号关闭对应Syncer，pid文件的命名方式为`host_port.pid`。  
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
**后文中的stop_syncer.sh指的是该路径下的stop_syncer.sh！！！**
## 停止选项
有三种关闭方法：  
1. 关闭目录下单个Syncer  
    指定要关闭Syncer的host && port，注意要与start_syncer时指定的host一致
2. 批量关闭目录下指定Syncer  
    指定要关闭的pid文件名，以空格分隔，用`" "`包裹
3. 关闭目录下所有Syncer  
    默认即可

### --pid_dir  
指定pid文件所在目录，上述三种关闭方法都依赖于pid文件的所在目录执行
```bash
bash bin/stop_syncer.sh --pid_dir /path/to/pids
```
例子中的执行效果就是关闭`/path/to/pids`下所有pid文件对应的Syncers（**方法3**），`--pid_dir`可与上面三种关闭方法组合使用。  

默认值为`SYNCER_OUTPUT_DIR/bin`
### --host && --port  
关闭pid_dir路径下host:port对应的Syncer
```bash
bash bin/stop_syncer.sh --host 127.0.0.1 --port 9190
```
host的默认值为127.0.0.1，port默认值为空  
即，单独指定host时**方法1**不生效，会退化为**方法3**。  
host与port都不为空时**方法1**才能生效
### --files  
关闭pid_dir路径下指定pid文件名对应的Syncer
```bash
bash bin/stop_syncer.sh --files "127.0.0.1_9190.pid 127.0.0.1_9191.pid"
```
文件之间用空格分隔，整体需要用`" "`包裹住