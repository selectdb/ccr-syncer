# 开启库中所有表的binlog
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
**后文中的enable_db_binlog.sh指的是该路径下的enable_db_binlog.sh！！！**
### 使用说明
```bash
bash bin/enable_db_binlog.sh -h host -p port -u user -P password -d db
```