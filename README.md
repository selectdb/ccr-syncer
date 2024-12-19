# CCR Syncer

CCR（Cross Cluster Replication）也就是跨集群数据复制，能够在库/表级别将源集群的数据变更同步到目标集群，可用于提升在线服务的数据可用性、隔离在离线负载、建设两地三中心等。

## 原理
### 名词解释

**源集群 (srcCluster)**：业务写入数据的集群
**目标集群 (destCluster)**：跨集群复制的目标集群
**binlog**：源集群变更日志，记录了源集群的数据修改和操作，是目标集群数据重放和恢复的凭据
**Syncer**：一个轻量的CCR任务控制节点，可以单节点部署，也可以多节点高可用部署

### 架构说明

![framework](doc/pic/framework.png)
Syncer从源集群批量获取库/表的binlog，并根据binlog中的信息在目标集群重放，从而实现数据的全量/增量复制。
如果binlog是数据变更，则通知目标集群从源集群拉取数据。
如果binlog是元数据变更，则在目标集群发起对应的操作。

## 使用说明

1. 在fe.conf、be.conf中打开binlog feature配置项
    ```bash
    enable_feature_binlog = true
    ```
2. 部署源、目标doris集群
3. 部署Syncer
    ```bash
    git clone https://github.com/selectdb/ccr-syncer
    cd ccr-syncer

    # -j 开启多线程编译
    # --output指定输出的路径名称，默认名称为output
    bash build.sh <-j NUM_OF_THREAD> <--output SYNCER_OUTPUT_DIR>

    cd SYNCER_OUTPUT_DIR

    # 启动syncer，加上--daemon使syncer在后台运行
    bash bin/start_syncer.sh --daemon

    # 停止syncer
    bash bin/stop_syncer.sh
    ```
    更多启动选项相见[启动配置](doc/start_syncer.md)，停止Syncer请见[stop说明](doc/stop_syncer.md)
4. 打开源集群中被同步库/表的binlog
    ```sql
    -- enable database binlog
    ALTER DATABASE db_name SET properties ("binlog.enable" = "true");

    -- enable table binlog
    ALTER TABLE table_name SET ("binlog.enable" = "true");
    ```
    如果是库同步，则需要打开库中所有表的`binlog.enable`，这个过程可以通过脚本快速完成，脚本的使用方法见[脚本说明文档](doc/db_enable_binlog.md)
5. 向Syncer发起同步任务
    ```bash
    curl -X POST -H "Content-Type: application/json" -d '{
        "name": "ccr_test",
        "src": {
        "host": "localhost",
        "port": "9030",
        "thrift_port": "9020",
        "user": "root",
        "password": "",
        "database": "demo",
        "table": "example_tbl"
        },
        "dest": {
        "host": "localhost",
        "port": "9030",
        "thrift_port": "9020",
        "user": "root",
        "password": "",
        "database": "ccrt",
        "table": "copy"
        }
    }' http://127.0.0.1:9190/create_ccr
    ```
    - name: CCR同步任务的名称，唯一即可
    - host、port：对应集群master的host和mysql(jdbc) 的端口
    - thrift_port：对应FE的rpc_port
    - user、password：syncer以何种身份去开启事务、拉取数据等
    - database、table：
        - 如果是db级别的同步，则填入dbName，tableName为空
        - 如果是表级别同步，则需要填入dbName、tableName

    其他操作详见[操作列表](doc/operations.md)