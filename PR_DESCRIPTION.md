# Add Cluster-level CCR Sync and Dynamic Monitor Interval Configuration

## 🚀 Feature Overview

This PR introduces two major enhancements to the CCR Syncer:

1. **Cluster-level CCR Synchronization** - Enables synchronizing entire clusters with a single command
2. **Dynamic Monitor Interval Configuration** - Allows runtime adjustment of database monitoring frequency

## 📋 Changes Summary

### 1. Cluster-level CCR Sync (`ClusterSync` parameter)

**New API Parameter:**
```json
{
  "cluster_sync": true  // New boolean parameter
}
```

**Key Features:**
- ✅ Automatically discovers all databases in source cluster
- ✅ Creates individual sync tasks for each database  
- ✅ Monitors for new/deleted databases dynamically
- ✅ Auto-creates/removes sync tasks as databases change
- ✅ Supports retry mechanisms for failed operations
- ✅ Maintains backward compatibility (defaults to `false`)

**Implementation Details:**
- `createClusterCcr()` - Main cluster sync orchestration
- `startDatabaseMonitor()` - Background daemon for database monitoring
- `monitorDatabaseChanges()` - Detects and handles database changes
- `handleNewDatabases()` / `handleDeletedDatabases()` - Process database additions/removals

### 2. Dynamic Monitor Interval Configuration

**New HTTP Endpoints:**

#### `POST /update_monitor_interval`
```json
{
  "interval_seconds": 300
}
```

#### `GET /get_monitor_interval`
```json
{
  "success": true,
  "interval_seconds": 120
}
```

**Key Features:**
- ✅ Runtime configuration without service restart
- ✅ Thread-safe implementation using channels
- ✅ Optimized lock usage for better performance
- ✅ Non-blocking updates with immediate effect

## 🔧 Technical Implementation

### Performance Optimizations
- **Before**: Frequent lock acquisition in monitoring loop
- **After**: Channel-based communication with minimal locking
- **Result**: Reduced lock contention and improved monitoring performance

### Thread Safety
- Uses `sync.RWMutex` for protecting shared state
- Implements channel-based notifications for interval updates
- Non-blocking channel operations to prevent deadlocks

### Error Handling
- Comprehensive retry mechanisms with exponential backoff
- Detailed error logging for troubleshooting
- Graceful handling of partial failures in cluster operations

## 📖 Usage Examples

### Create Cluster-level Sync Job
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

### Update Monitor Interval to 5 minutes
```bash
curl -X POST http://localhost:9190/update_monitor_interval \
  -H "Content-Type: application/json" \
  -d '{"interval_seconds": 300}'
```

### Get Current Monitor Interval
```bash
curl -X GET http://localhost:9190/get_monitor_interval
```

## 🧪 Testing

### Test Scenarios Covered:
- [x] Single database sync (existing functionality)
- [x] Cluster-level sync with multiple databases
- [x] Dynamic database addition/removal detection
- [x] Monitor interval updates during runtime
- [x] Concurrent operations and thread safety
- [x] Error recovery and retry mechanisms
- [x] Backward compatibility verification

### Performance Testing:
- [x] Lock contention reduction verified
- [x] Memory usage optimization confirmed
- [x] Response time improvements measured

## 🔄 Backward Compatibility

- ✅ All existing single-database sync functionality preserved
- ✅ Default behavior unchanged (`cluster_sync: false`)
- ✅ No breaking changes to existing APIs
- ✅ Existing configurations continue to work

## 📝 Documentation Updates

- [x] API documentation for new endpoints
- [x] Usage examples and best practices
- [x] Performance tuning guidelines
- [x] Migration guide for cluster sync adoption

## 🎯 Benefits

1. **Operational Efficiency**: Sync entire clusters with single command
2. **Dynamic Configuration**: Adjust monitoring without downtime
3. **Better Performance**: Optimized locking reduces resource contention
4. **Enhanced Monitoring**: Automatic detection of database changes
5. **Production Ready**: Comprehensive error handling and retry logic

## 🔍 Code Quality

- ✅ Comprehensive error handling
- ✅ Detailed logging for observability
- ✅ Thread-safe implementation
- ✅ Performance optimizations
- ✅ Clean, maintainable code structure
- ✅ Follows existing code conventions

## 📊 Impact Assessment

- **Risk Level**: Low (backward compatible, well-tested)
- **Performance Impact**: Positive (reduced lock contention)
- **Maintenance Overhead**: Minimal (follows existing patterns)
- **User Experience**: Significantly improved for cluster operations

---

## 🤝 Reviewer Notes

This PR introduces significant new functionality while maintaining full backward compatibility. The implementation follows established patterns in the codebase and includes comprehensive error handling and performance optimizations.

Key areas for review:
1. Thread safety implementation in `startDatabaseMonitor()`
2. Channel-based communication for interval updates
3. Error handling in cluster sync operations
4. API design for new endpoints

Ready for community review and feedback! 🚀