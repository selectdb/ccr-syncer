# Cluster CCR 功能使用指南

## 概述

Cluster CCR 功能为 CCR Syncer 提供了集群级别的数据同步能力，允许用户一次性同步整个集群的所有数据库，并支持动态调整监控间隔。

## 主要功能

### 1. 集群级别CCR同步
- 自动发现源集群中的所有数据库
- 为每个数据库创建独立的同步任务
- 动态监控数据库的新增和删除
- 自动创建/删除对应的同步任务

### 2. 动态监控间隔配置
- 运行时调整数据库监控频率
- 无需重启服务即可生效
- 线程安全的配置更新

## API 接口

### 创建集群级同步任务

**接口**: `POST /create_ccr`

**请求参数**:
```json
{
  "name": "cluster_sync_job",
  "src": {
    "host": "source-cluster-host",
    "port": 9030,
    "user": "root",
    "password": "password",
    "database": ""
  },
  "dest": {
    "host": "dest-cluster-host",
    "port": 9030,
    "user": "root", 
    "password": "password",
    "database": ""
  },
  "skip_error": false,
  "allow_table_exists": false,
  "reuse_binlog_label": false,
  "cluster_sync": true
}
```

**参数说明**:
- `cluster_sync`: 设置为 `true` 启用集群级同步
- `name`: 同步任务的基础名称，实际任务名会加上数据库名后缀
- `src/dest`: 源集群和目标集群的连接信息
- 其他参数与单数据库同步相同

**响应示例**:
```json
{
  "success": true
}
```

### 更新监控间隔

**接口**: `POST /update_monitor_interval`

**请求参数**:
```json
{
  "interval_seconds": 300
}
```

**参数说明**:
- `interval_seconds`: 监控间隔时间（秒），必须大于0

**响应示例**:
```json
{
  "success": true
}
```

### 获取当前监控间隔

**接口**: `GET /get_monitor_interval`

**响应示例**:
```json
{
  "success": true,
  "interval_seconds": 120
}
```

## 使用示例

### 1. 创建集群级同步任务

```bash
curl -X POST http://localhost:9190/create_ccr \
  -H "Content-Type: application/json" \
  -d '{
    "name": "prod_cluster_sync",
    "src": {
      "host": "10.1.1.100",
      "port": 9030,
      "user": "root",
      "password": "source_password"
    },
    "dest": {
      "host": "10.1.2.100",
      "port": 9030,
      "user": "root",
      "password": "dest_password"
    },
    "cluster_sync": true,
    "skip_error": false,
    "allow_table_exists": true
  }'
```

### 2. 调整监控间隔为5分钟

```bash
curl -X POST http://localhost:9190/update_monitor_interval \
  -H "Content-Type: application/json" \
  -d '{"interval_seconds": 300}'
```

### 3. 查看当前监控间隔

```bash
curl -X GET http://localhost:9190/get_monitor_interval
```

### 4. 查看创建的同步任务

```bash
curl -X GET http://localhost:9190/list_jobs
```

## 工作原理

### 集群同步流程

1. **初始化阶段**:
   - 连接源集群，获取所有数据库列表
   - 为每个数据库创建独立的同步任务
   - 任务命名格式：`{基础名称}_{数据库名}`

2. **监控阶段**:
   - 启动后台守护进程，定期检查数据库变化
   - 默认检查间隔：2分钟（可通过API调整）
   - 检测新增数据库并自动创建同步任务
   - 检测删除的数据库并自动删除对应任务

3. **同步阶段**:
   - 每个数据库的同步任务独立运行
   - 支持失败重试机制
   - 详细的日志记录和错误处理

### 任务管理

创建集群同步后，会生成多个独立的同步任务：

```
原始任务名: prod_cluster_sync
生成的任务:
- prod_cluster_sync_db1
- prod_cluster_sync_db2  
- prod_cluster_sync_db3
...
```

每个任务可以独立管理：
- 暂停/恢复：`/pause`, `/resume`
- 查看状态：`/job_status`
- 查看进度：`/job_progress`
- 删除任务：`/delete`

## 监控和运维

### 查看集群同步状态

```bash
# 查看所有任务
curl -X GET http://localhost:9190/list_jobs

# 查看特定任务状态
curl -X POST http://localhost:9190/job_status \
  -H "Content-Type: application/json" \
  -d '{"name": "prod_cluster_sync_db1"}'

# 查看任务详情
curl -X POST http://localhost:9190/job_detail \
  -H "Content-Type: application/json" \
  -d '{"name": "prod_cluster_sync_db1"}'
```

### 调整监控频率

根据集群规模和变化频率调整监控间隔：

```bash
# 高频监控（1分钟）- 适用于频繁变化的开发环境
curl -X POST http://localhost:9190/update_monitor_interval \
  -d '{"interval_seconds": 60}'

# 中频监控（5分钟）- 适用于一般生产环境  
curl -X POST http://localhost:9190/update_monitor_interval \
  -d '{"interval_seconds": 300}'

# 低频监控（30分钟）- 适用于稳定的生产环境
curl -X POST http://localhost:9190/update_monitor_interval \
  -d '{"interval_seconds": 1800}'
```

### 日志监控

关键日志信息：

```
# 集群同步启动
Starting database monitor daemon, task name prefix: prod_cluster_sync

# 发现数据库变化
Found 2 new databases: [new_db1, new_db2]
Successfully created sync task for new database new_db1

# 监控间隔调整
Database monitor interval updated from 2m0s to 5m0s
Database monitor interval changed from 2m0s to 5m0s

# 数据库删除检测
Found 1 deleted databases: [old_db]
Successfully removed sync task for deleted database old_db
```

## 最佳实践

### 1. 监控间隔设置

- **开发环境**: 1-2分钟，快速响应数据库变化
- **测试环境**: 2-5分钟，平衡响应速度和资源消耗
- **生产环境**: 5-30分钟，根据数据库变化频率调整

### 2. 任务命名

使用有意义的任务名称，便于管理：
```json
{
  "name": "prod_to_backup_cluster",
  "cluster_sync": true
}
```

### 3. 错误处理

启用错误跳过，避免单个数据库问题影响整体同步：
```json
{
  "skip_error": true,
  "cluster_sync": true
}
```

### 4. 表存在处理

对于可能存在表冲突的场景：
```json
{
  "allow_table_exists": true,
  "cluster_sync": true
}
```

## 故障排查

### 常见问题

1. **权限不足**
   ```
   错误: Failed to get database list from source cluster
   解决: 确保用户有SHOW DATABASES权限
   ```

2. **连接失败**
   ```
   错误: Failed to connect to source cluster
   解决: 检查网络连接和防火墙设置
   ```

3. **任务创建失败**
   ```
   错误: Failed to create sync task for database xxx
   解决: 检查目标集群连接和权限
   ```

### 调试命令

```bash
# 查看所有任务状态
curl -X GET http://localhost:9190/view?type=table

# 查看特定任务的详细信息
curl -X POST http://localhost:9190/job_progress \
  -d '{"name": "cluster_sync_db1"}'

# 查看当前监控间隔
curl -X GET http://localhost:9190/get_monitor_interval
```

## 性能优化

### 1. 锁优化
- 使用channel通信替代频繁锁操作
- 减少监控循环中的锁竞争
- 提升整体监控性能

### 2. 资源管理
- 合理设置监控间隔，避免过度消耗资源
- 监控任务数量，避免创建过多并发任务
- 定期清理已删除数据库的相关资源

### 3. 网络优化
- 使用连接池减少连接开销
- 合理设置超时时间
- 考虑网络延迟对监控间隔的影响

## 版本兼容性

- **向后兼容**: 现有单数据库同步功能完全保持不变
- **默认行为**: `cluster_sync` 参数默认为 `false`
- **API兼容**: 所有现有API接口保持兼容

## 安全考虑

1. **权限控制**: 确保同步用户只有必要的数据库权限
2. **网络安全**: 使用安全的网络连接，考虑VPN或专线
3. **密码管理**: 避免在日志中记录敏感信息
4. **访问控制**: 限制监控间隔配置API的访问权限

---

更多详细信息请参考 `CLUSTER_CCR_CHANGES.md` 技术文档。