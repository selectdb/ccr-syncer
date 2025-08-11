# Commit Message Template

## Title
feat: Add cluster-level CCR sync and dynamic monitor interval configuration

## Description
This commit introduces cluster-level CCR synchronization functionality and dynamic monitor interval configuration capabilities.

### New Features:

1. **Cluster-level CCR Sync**
   - Add `ClusterSync` parameter to `CreateCcrRequest`
   - Implement `createClusterCcr()` function for cluster-wide database sync
   - Add automatic database monitoring with `startDatabaseMonitor()`
   - Support dynamic detection of new/deleted databases
   - Auto-create/remove sync tasks for database changes

2. **Dynamic Monitor Interval Configuration**
   - Add `/update_monitor_interval` HTTP endpoint
   - Add `/get_monitor_interval` HTTP endpoint  
   - Support runtime modification of database monitoring frequency
   - Implement thread-safe interval updates using channels

3. **Performance Optimizations**
   - Optimize lock usage in `startDatabaseMonitor()` 
   - Replace frequent lock operations with channel communication
   - Reduce lock contention and improve monitoring performance

### API Endpoints:
- `POST /update_monitor_interval` - Update monitoring interval
- `GET /get_monitor_interval` - Get current monitoring interval

### Backward Compatibility:
- All existing single-database sync functionality remains unchanged
- New `cluster_sync` parameter defaults to `false`

### Technical Details:
- Thread-safe implementation using `sync.RWMutex`
- Non-blocking channel communication for interval updates
- Comprehensive error handling and retry mechanisms
- Detailed logging for observability

Closes: #[issue_number]