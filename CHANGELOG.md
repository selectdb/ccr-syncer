# 更新日志

## v 0.3

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
