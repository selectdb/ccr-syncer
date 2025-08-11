# Cluster CCR Feature Changes

## 概述
本次更改为CCR Syncer添加了集群级别同步功能，允许用户一次性同步整个集群的所有数据库，并提供了动态配置监控间隔的功能。

## 主要更改

### 1. 新增ClusterSync参数

#### 文件：`pkg/service/http_service.go`

**新增字段：**
```go
type CreateCcrRequest struct {
    // ... 其他字段
    // Whether it's cluster-level sync, if true, will get all databases from source cluster and create sync task for each database
    ClusterSync bool `json:"cluster_sync"`
}
```

**新增函数：**
- `createClusterCcr()` - 创建集群级别的CCR同步任务
- `startDatabaseMonitor()` - 启动数据库监控守护进程
- `monitorDatabaseChanges()` - 检测数据库变化并处理
- `handleNewDatabases()` - 处理新增数据库
- `handleDeletedDatabases()` - 处理删除的数据库
- `getDatabaseList()` - 获取数据库列表

**功能特性：**
- 自动获取源集群所有数据库
- 为每个数据库创建独立的同步任务
- 支持动态监控新增/删除的数据库
- 自动创建/删除对应的同步任务
- 支持失败重试机制

### 2. 动态监控间隔配置

#### 新增全局变量：
```go
var (
    databaseMonitorInterval time.Duration = 2 * time.Minute
    intervalMutex           sync.RWMutex
    intervalUpdateChan      = make(chan time.Duration, 1)
)
```

#### 新增HTTP Handler：

**`/update_monitor_interval` - 更新监控间隔**
- 请求方法：POST
- 请求格式：
```json
{
  "interval_seconds": 300
}
```
- 功能：动态更新数据库监控的检查间隔

**`/get_monitor_interval` - 获取当前监控间隔**
- 请求方法：GET
- 响应格式：
```json
{
  "success": true,
  "interval_seconds": 120
}
```
- 功能：获取当前的监控间隔设置

### 3. 性能优化

**锁优化：**
- 使用channel通信替代频繁的锁操作
- 减少了`startDatabaseMonitor`函数中的锁竞争
- 提高了监控性能

**实现细节：**
- 使用`select`语句监听ticker和interval更新
- 非阻塞的channel通信
- 只在间隔真正改变时才重置ticker

## API使用示例

### 创建集群级同步任务
```bash
curl -X POST http://localhost:9190/create_ccr \
  -H "Content-Type: application/json" \
  -d '{
    "name": "cluster_sync_job",
    "src": {
      "host": "source-cluster",
      "port": 9030,
      "user": "root",
      "password": "password"
    },
    "dest": {
      "host": "dest-cluster", 
      "port": 9030,
      "user": "root",
      "password": "password"
    },
    "cluster_sync": true
  }'
```

### 更新监控间隔
```bash
curl -X POST http://localhost:9190/update_monitor_interval \
  -H "Content-Type: application/json" \
  -d '{"interval_seconds": 300}'
```

### 获取监控间隔
```bash
curl -X GET http://localhost:9190/get_monitor_interval
```

## 技术特点

1. **线程安全**：使用mutex保护全局变量
2. **高性能**：优化锁使用，减少竞争
3. **动态配置**：支持运行时修改监控间隔
4. **容错性**：支持失败重试和错误处理
5. **可观测性**：详细的日志记录
6. **向后兼容**：不影响现有的单数据库同步功能

## 测试建议

1. 测试集群级同步功能
2. 测试动态监控间隔更新
3. 测试新增/删除数据库的自动处理
4. 测试并发场景下的性能
5. 测试错误恢复机制

## 注意事项

1. 集群级同步会为每个数据库创建独立的同步任务
2. 任务命名格式：`{原任务名}_{数据库名}`
3. 监控间隔最小值应大于0
4. 建议在生产环境中谨慎设置监控间隔