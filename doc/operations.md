# Syncer操作列表

### 请求的通用模板

```bash
curl -X POST -H "Content-Type: application/json" -d {json_body} http://ccr_syncer_host:ccr_syncer_port/operator
```
- json_body: 以json的格式发送操作所需信息
- operator：对应Syncer的不同操作

### operators

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
