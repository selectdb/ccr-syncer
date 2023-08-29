# Syncer操作列表
### 请求的通用模板
```bash
curl -X POST -H "Content-Type: application/json" -d {json_body} http://ccr_syncer_host:ccr_syncer_port/operator
```
json_body: 以json的格式发送操作所需信息  
operator：对应Syncer的不同操作
### operators
- create_ccr  
    创建CCR任务，详见[README](../README.md)
- get_lag
    查看同步进度
    ```bash
    curl -X POST -H "Content-Type: application/json" -d '{
        "name": "job_name"
    }' http://ccr_syncer_host:ccr_syncer_port/get_lag
    ```
    其中job_name是create_ccr时创建的name
- pause
    暂停同步任务
    ```bash
    curl -X POST -H "Content-Type: application/json" -d '{
        "name": "job_name"
    }' http://ccr_syncer_host:ccr_syncer_port/pause
    ```
- resume
    恢复同步任务
    ```bash
    curl -X POST -H "Content-Type: application/json" -d '{
        "name": "job_name"
    }' http://ccr_syncer_host:ccr_syncer_port/resume
    ```
- delete
    删除同步任务
    ```bash
    curl -X POST -H "Content-Type: application/json" -d '{
        "name": "job_name"
    }' http://ccr_syncer_host:ccr_syncer_port/delete
    ```