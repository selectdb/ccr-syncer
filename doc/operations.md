# Syncer操作列表

### 请求的通用模板

```bash
curl -X POST -H "Content-Type: application/json" -d {json_body} http://ccr_syncer_host:ccr_syncer_port/operator
```
- json_body: 以json的格式发送操作所需信息
- operator：对应Syncer的不同操作

### operators

- `version`
    查看 ccr syncer 的版本
- `create_ccr`
    创建CCR任务，详见[README](../README.md)。
- `get_lag`
    查看同步进度
    ```bash
    curl -X POST -L --post303 -H "Content-Type: application/json" -d '{
        "name": "job_name"
    }' http://ccr_syncer_host:ccr_syncer_port/get_lag
    ```
    其中job_name是create_ccr时创建的name
- `pause`
    暂停同步任务
    ```bash
    curl -X POST -L --post303 -H "Content-Type: application/json" -d '{
        "name": "job_name"
    }' http://ccr_syncer_host:ccr_syncer_port/pause
    ```
- `resume`
    恢复同步任务
    ```bash
    curl -X POST -L --post303 -H "Content-Type: application/json" -d '{
        "name": "job_name"
    }' http://ccr_syncer_host:ccr_syncer_port/resume
    ```
- `delete`
    删除同步任务
    ```bash
    curl -X POST -L --post303 -H "Content-Type: application/json" -d '{
        "name": "job_name"
    }' http://ccr_syncer_host:ccr_syncer_port/delete
    ```
- `list_jobs`
    列出所有job名称
    ```bash
    curl -X POST -L --post303 -H "Content-Type: application/json" -d '{}' http://ccr_syncer_host:ccr_syncer_port/list_jobs
    ```
- `job_detail`
    展示job的详细信息
    ```bash
    curl -X POST -L --post303 -H "Content-Type: application/json" -d '{
        "name": "job_name"
    }' http://ccr_syncer_host:ccr_syncer_port/job_detail
    ```
- `job_progress`
    展示job的详细进度信息
    ```bash
    curl -X POST -L --post303 -H "Content-Type: application/json" -d '{
        "name": "job_name"
    }' http://ccr_syncer_host:ccr_syncer_port/job_progress
    ```
- `job_status`
    展示job状态
    ```
    curl -X POST -L --post303 -H "Content-Type: application/json" -d '{
        "name": "job_name"
    }' http://ccr_syncer_host:ccr_syncer_port/job_status
    ```
    返回结果：
    ```json
    {
        "name": "job_name",
        "state": "running", // or paused
        "progress_state": "progress_state"
    }
    ```
    其中 progress_state 有下面几种情况：
    - DBFullSync
    - DBTablesIncrementalSync
    - DBIncrementalSync
    - DBPartialSync
    - TableFullSync
    - TableIncrementalSync
    - TablePartialSync
    full sync 和 partial sync 分别表示通过快照同步全量/部分 table；incremental sync 表示通过 binlog 同步增量变更；一个比较特殊的时 DBTablesIncrementalSync，表示已经完成了全量/部分同步，由于某些 table 的进度比其他 table 快，因此增量同步期间需要跳过这部分已经同步完成的 binlog。
- `metrics`
    获取golang以及ccr job的metrics信息
    ```bash
    curl -L --post303 http://ccr_syncer_host:ccr_syncer_port/metrics
    ```
- `update_host_mapping`
    更新上游 FE/BE 集群 private ip 到 public ip 的映射；如果参数中的 public ip 为空，则删除该 private 的映射
    ```bash
    curl -X POST -L --post303 -H "Content-Type: application/json" -d '{
        "name": "job_name",
        "src_host_mapping": {
            "172.168.1.1": "10.0.10.1",
            "172.168.1.2": "10.0.10.2",
            "172.168.1.3": "10.0.10.3",
            "172.168.1.5": ""
        },
        "dest_host_mapping": {
            ...
        }
    }' http://ccr_syncer_host:ccr_syncer_port/add_host_mapping
    ```
    更新上游 172.168.1.1-3 的映射，同时删除 172.168.1.5 的映射。
    - `src_host_mapping`: 上游映射
    - `dest_host_mapping`: 下游映射
- `job_skip_binlog`
    当同步出错时进行快速恢复，该接口主要用于异常处理。目前支持两种方式：
    1. `silence`：直接跳过一条下游执行出错的 binlog，这种方式主要用于处理 binlog 类型不支持/下游环境（session variable，config）不支持等情况导致的同步中断，使用时需要指定 binlog 的 commit seq。
    2. `fullsync`：触发一次全量同步。这种方式主要用于处理如建表等无法直接跳过的 binlog，此外该方法还可以用于在发现上下游同步数据不一致时，强制下游通过快照恢复到与上游数据一致的状态。
    比如需要直接跳过 commit seq 为 1001 的 binlog：
    ```bash
    curl -X POST -L --post303 -H "Content-Type: application/json" -d '{
        "name": "job_name",
        "skip_by": "silence",
        "skip_commit_seq": 1001
    }
    ```
    如果要强制全量同步：
    ```bash
    curl -X POST -L --post303 -H "Content-Type: application/json" -d '{
        "name": "job_name",
        "skip_by": "silence"
    }
    ```

### 一些特殊场景

#### 上下游通过公网 IP 进行同步

ccr syncer 支持将上下游部署到不同的网络环境中，并通过公网 IP 进行数据同步。

具体方案：每个 job 会记录下上游 private IP 到 public IP 的映射关系（由用户提供），并在下游载入 binlog 前，将上游集群 FE/BE 的 private 转换成对应的 public IP。

使用方式：创建 ccr job 时增加一个参数：
```bash
curl -X POST -H "Content-Type: application/json" -d '{
    "name": "ccr_test",
    "src": {
        "host_mapping": {
            "172.168.1.1": "10.0.10.1",
            "172.168.1.2": "10.0.10.2",
            "172.168.1.3": "10.0.10.3"
        },
        ...
    },
    "dest": {
        "host_mapping": {
            "172.168.2.3": "10.0.10.9",
            "172.168.2.4": ""
        },
        ...
    },
}' http://127.0.0.1:9190/create_ccr
```

`host_mapping` 用法与 `/update_host_mapping` 接口一致。

> 注意：即使增加了 host_mapping 字段，**src/dest 中的 host 字段仍需要设置为 public ip**。

相关操作：
- 修改/删除/增加新映射，使用 `/update_host_mapping` 接口
- 查看 job 的所有映射，使用 `/job_detail` 接口
