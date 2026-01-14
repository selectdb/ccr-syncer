// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License

package ccr

import (
	"fmt"
	"strings"
	"time"

	"github.com/selectdb/ccr_syncer/pkg/utils"
	"github.com/selectdb/ccr_syncer/pkg/xerror"

	tstatus "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/status"

	log "github.com/sirupsen/logrus"
)

const (
	// Prefix for temporary table name
	insertBootstrapTempTablePrefix = "_ccr_ib_tmp_"

	// Timeout for INSERT operation (24 hours)
	insertBootstrapInsertTimeout = 24 * time.Hour

	// Interval for checking INSERT progress
	insertBootstrapCheckInterval = 10 * time.Second

	// Interval for checking temp table sync progress
	insertBootstrapSyncCheckInterval = 5 * time.Second
)

// runInsertBootstrap executes the InsertBootstrap mode for cross-version migration.
// This mode uses INSERT INTO ... SELECT to bootstrap data instead of backup/restore.
//
// The workflow is:
// 1. Record current binlog position (BootstrapCommitSeq)
// 2. Create a temp table in source cluster (with same schema as original table)
// 3. Execute INSERT INTO temp_table SELECT FROM original_table
// 4. Wait for INSERT to complete
// 5. Sync temp table to dest cluster via CCR (reusing existing sync logic)
// 6. Rename tables in dest cluster (temp -> original)
// 7. Cleanup temp resources
// 8. Switch to incremental sync from recorded position
func (j *Job) runInsertBootstrap() error {
	log.Infof("[InsertBootstrap] ========================================")
	log.Infof("[InsertBootstrap] Starting InsertBootstrap mode for job: %s", j.Name)
	log.Infof("[InsertBootstrap] [CrossVersionMigration] Cross-version migration mode enabled")
	log.Infof("[InsertBootstrap] Source: %s.%s", j.Src.Database, j.Src.Table)
	log.Infof("[InsertBootstrap] Dest: %s.%s", j.Dest.Database, j.Dest.Table)
	log.Infof("[InsertBootstrap] [CrossVersionMigration] This mode supports Doris 2.1 -> 4.0 migration")
	log.Infof("[InsertBootstrap] [CrossVersionMigration] Workflow:")
	log.Infof("[InsertBootstrap] [CrossVersionMigration]   1. Record old table binlog offset (v1)")
	log.Infof("[InsertBootstrap] [CrossVersionMigration]   2. Create old_tmp table based on old table")
	log.Infof("[InsertBootstrap] [CrossVersionMigration]   3. Execute INSERT INTO old_tmp SELECT FROM old")
	log.Infof("[InsertBootstrap] [CrossVersionMigration]   4. Sync old_tmp binlogs to dst old table")
	log.Infof("[InsertBootstrap] [CrossVersionMigration]   5. Start incremental sync from v1 offset for old table")
	log.Infof("[InsertBootstrap] ========================================")

	// Initialize state if not exists
	if j.InsertBootstrapState == nil {
		j.InsertBootstrapState = &InsertBootstrapState{
			Phase: "init",
		}
		log.Infof("[InsertBootstrap] Initialized new InsertBootstrapState")
	} else {
		log.Infof("[InsertBootstrap] Recovering from existing state: phase=%s, commitSeq=%d, tempTable=%s",
			j.InsertBootstrapState.Phase, j.InsertBootstrapState.BootstrapCommitSeq, j.InsertBootstrapState.TempTableName)
	}

	// Run state machine
	for {
		if j.hasInterruptSignal() {
			log.Infof("[InsertBootstrap] Received interrupt signal, pausing...")
			return nil
		}

		switch j.progress.SubSyncState {
		case Done, IBRecordBinlogPosition:
			if err := j.ibRecordBinlogPosition(); err != nil {
				return err
			}

		case IBCreateTempTable:
			if err := j.ibCreateTempTable(); err != nil {
				return err
			}

		case IBExecuteInsert:
			if err := j.ibExecuteInsert(); err != nil {
				return err
			}

		case IBWaitInsertDone:
			if err := j.ibWaitInsertDone(); err != nil {
				return err
			}

		case IBSyncTempTable:
			if err := j.ibSyncTempTable(); err != nil {
				return err
			}

		case IBWaitTempTableSync:
			if err := j.ibWaitTempTableSync(); err != nil {
				return err
			}

		case IBRenameTables:
			if err := j.ibRenameTables(); err != nil {
				return err
			}

		case IBCleanup:
			if err := j.ibCleanup(); err != nil {
				return err
			}

		case IBSwitchToIncremental:
			if err := j.ibSwitchToIncremental(); err != nil {
				return err
			}
			// InsertBootstrap completed, exit the loop
			log.Infof("[InsertBootstrap] ========================================")
			log.Infof("[InsertBootstrap] InsertBootstrap mode completed successfully!")
			log.Infof("[InsertBootstrap] Now switching to incremental sync from commitSeq: %d", j.progress.CommitSeq)
			log.Infof("[InsertBootstrap] ========================================")
			return nil

		default:
			return xerror.Errorf(xerror.Normal, "[InsertBootstrap] Unknown SubSyncState: %s", j.progress.SubSyncState)
		}
	}
}

// ibRecordBinlogPosition records the current binlog position before INSERT operation
//
// CRITICAL: This is the key step for data consistency.
// The BootstrapCommitSeq recorded here determines the incremental sync starting point.
//
// Flow:
// 1. Record LastCommitSeq = X (this is the last committed binlog)
// 2. Later, INSERT INTO tmp SELECT FROM old will read data as of this snapshot
// 3. After bootstrap, incremental sync will start from X to catch up new changes
//
// For UNIQUE KEY tables: duplicates are automatically deduplicated (idempotent)
// For DUPLICATE KEY tables: there may be duplicates if writes happen during INSERT
func (j *Job) ibRecordBinlogPosition() error {
	log.Infof("[InsertBootstrap] ========================================")
	log.Infof("[InsertBootstrap] Phase 1: Recording current binlog position")
	log.Infof("[InsertBootstrap] ========================================")
	log.Infof("[InsertBootstrap] Source table: %s.%s (ID: %d)", j.Src.Database, j.Src.Table, j.Src.TableId)

	j.InsertBootstrapState.Phase = "record_binlog_position"

	// Get current binlog position from source cluster
	// IMPORTANT: We use the ORIGINAL table's Spec to get its binlog position
	src := &j.Src
	srcRpc, err := j.factory.NewFeRpc(src)
	if err != nil {
		log.Errorf("[InsertBootstrap] Failed to create FE RPC client: %+v", err)
		return err
	}

	// Get the latest binlog commit seq for the source table
	// We pass 0 to get the lag from the beginning
	resp, err := srcRpc.GetBinlogLag(src, 0)
	if err != nil {
		log.Errorf("[InsertBootstrap] Failed to get binlog lag for source table: %+v", err)
		return err
	}

	// Record the last commit seq as our bootstrap position
	// This is the position from which incremental sync will start AFTER bootstrap
	bootstrapCommitSeq := resp.GetLastCommitSeq()
	j.InsertBootstrapState.BootstrapCommitSeq = bootstrapCommitSeq

	log.Infof("[InsertBootstrap] ----------------------------------------")
	log.Infof("[InsertBootstrap] Source table binlog position recorded:")
	log.Infof("[InsertBootstrap]   Table: %s.%s (ID: %d)", j.Src.Database, j.Src.Table, j.Src.TableId)
	log.Infof("[InsertBootstrap]   FirstCommitSeq: %d (earliest available)", resp.GetFirstCommitSeq())
	log.Infof("[InsertBootstrap]   LastCommitSeq: %d (latest committed)", resp.GetLastCommitSeq())
	log.Infof("[InsertBootstrap]   *** BootstrapCommitSeq (v1): %d ***", bootstrapCommitSeq)
	log.Infof("[InsertBootstrap]   Current Lag: %d binlogs", resp.GetLag())
	
	// Log timestamp information if available
	if resp.GetLastBinlogTimestamp() != -1 {
		lastBinlogTime := time.Unix(0, resp.GetLastBinlogTimestamp()*int64(time.Millisecond))
		log.Infof("[InsertBootstrap]   LastBinlogTimestamp: %s", lastBinlogTime.Format(time.RFC3339))
	}
	if resp.GetFirstBinlogTimestamp() != -1 {
		firstBinlogTime := time.Unix(0, resp.GetFirstBinlogTimestamp()*int64(time.Millisecond))
		log.Infof("[InsertBootstrap]   FirstBinlogTimestamp: %s", firstBinlogTime.Format(time.RFC3339))
	}
	
	log.Infof("[InsertBootstrap] ----------------------------------------")
	log.Infof("[InsertBootstrap] [CrossVersionMigration] Binlog offset v1 = %d (recorded before INSERT)", bootstrapCommitSeq)
	log.Infof("[InsertBootstrap] NOTE: INSERT SELECT will capture data snapshot at this point")
	log.Infof("[InsertBootstrap] NOTE: Incremental sync will later start from commitSeq=%d (v1 offset)", bootstrapCommitSeq)
	log.Infof("[InsertBootstrap] ----------------------------------------")

	// Generate temp table name with timestamp for uniqueness
	timestamp := time.Now().Unix()
	j.InsertBootstrapState.TempTableName = fmt.Sprintf("%s%s_%d", insertBootstrapTempTablePrefix, j.Src.Table, timestamp)
	log.Infof("[InsertBootstrap] Generated temp table name: %s", j.InsertBootstrapState.TempTableName)

	// Persist state and move to next phase
	j.persistInsertBootstrapState()
	j.progress.NextWithPersist(j.progress.CommitSeq, TableInsertBootstrap, IBCreateTempTable, "")

	log.Infof("[InsertBootstrap] Phase 1 completed successfully")
	log.Infof("[InsertBootstrap] Moving to Phase 2: Create temp table")
	return nil
}

// ibCreateTempTable creates a temporary table in source cluster with same schema as original table
func (j *Job) ibCreateTempTable() error {
	log.Infof("[InsertBootstrap] Phase 2: Creating temp table in source cluster...")
	log.Infof("[InsertBootstrap]   Temp table name: %s", j.InsertBootstrapState.TempTableName)

	j.InsertBootstrapState.Phase = "create_temp_table"

	// Get the CREATE TABLE statement of the original table
	createTableSql, err := j.ISrc.GetCreateTableSql(j.Src.Table)
	if err != nil {
		log.Errorf("[InsertBootstrap] Failed to get CREATE TABLE SQL for %s: %+v", j.Src.Table, err)
		return err
	}
	log.Infof("[InsertBootstrap] Original table CREATE SQL (first 500 chars): %.500s...", createTableSql)

	// Modify the CREATE TABLE SQL to use temp table name
	// Replace the table name in CREATE TABLE statement
	tempTableSql := j.modifyCreateTableForTempTable(createTableSql)
	log.Infof("[InsertBootstrap] Temp table CREATE SQL (first 500 chars): %.500s...", tempTableSql)

	// Execute the CREATE TABLE for temp table
	if err := j.ISrc.Exec(tempTableSql); err != nil {
		log.Errorf("[InsertBootstrap] Failed to create temp table: %+v", err)
		return err
	}

	log.Infof("[InsertBootstrap] Temp table created successfully: %s", j.InsertBootstrapState.TempTableName)

	// Get temp table ID
	tempTableId, err := j.srcMeta.GetTableId(j.InsertBootstrapState.TempTableName)
	if err != nil {
		log.Errorf("[InsertBootstrap] Failed to get temp table ID: %+v", err)
		return err
	}
	j.InsertBootstrapState.TempTableId = tempTableId
	log.Infof("[InsertBootstrap] Temp table ID: %d", tempTableId)

	// Persist state and move to next phase
	j.persistInsertBootstrapState()
	j.progress.NextWithPersist(j.progress.CommitSeq, TableInsertBootstrap, IBExecuteInsert, "")

	log.Infof("[InsertBootstrap] Phase 2 completed. Moving to Phase 3: Execute INSERT")
	return nil
}

// modifyCreateTableForTempTable modifies the CREATE TABLE SQL to create a temp table
func (j *Job) modifyCreateTableForTempTable(createTableSql string) string {
	// Replace table name in CREATE TABLE statement
	// Pattern: CREATE TABLE `table_name` -> CREATE TABLE `temp_table_name`
	oldTableName := utils.FormatKeywordName(j.Src.Table)
	newTableName := utils.FormatKeywordName(j.InsertBootstrapState.TempTableName)

	// Also need to handle the case with database prefix
	dbName := utils.FormatKeywordName(j.Src.Database)
	oldFullName := fmt.Sprintf("%s.%s", dbName, oldTableName)
	newFullName := fmt.Sprintf("%s.%s", dbName, newTableName)

	result := strings.Replace(createTableSql, oldFullName, newFullName, 1)
	if result == createTableSql {
		// Try without database prefix
		result = strings.Replace(createTableSql, "CREATE TABLE "+oldTableName, "CREATE TABLE "+newFullName, 1)
	}

	return result
}

// ibExecuteInsert executes INSERT INTO temp_table SELECT FROM original_table
func (j *Job) ibExecuteInsert() error {
	log.Infof("[InsertBootstrap] Phase 3: Executing INSERT INTO ... SELECT ...")
	log.Infof("[InsertBootstrap]   Source table: %s.%s", j.Src.Database, j.Src.Table)
	log.Infof("[InsertBootstrap]   Temp table: %s.%s", j.Src.Database, j.InsertBootstrapState.TempTableName)

	j.InsertBootstrapState.Phase = "execute_insert"
	j.InsertBootstrapState.InsertStartTime = time.Now().Unix()

	// Build INSERT SQL
	dbName := utils.FormatKeywordName(j.Src.Database)
	srcTable := utils.FormatKeywordName(j.Src.Table)
	tempTable := utils.FormatKeywordName(j.InsertBootstrapState.TempTableName)

	insertSql := fmt.Sprintf("INSERT INTO %s.%s SELECT * FROM %s.%s", dbName, tempTable, dbName, srcTable)
	log.Infof("[InsertBootstrap] Executing INSERT SQL: %s", insertSql)
	log.Infof("[InsertBootstrap] [CrossVersionMigration] Starting INSERT INTO tmp SELECT FROM old")
	log.Infof("[InsertBootstrap] [CrossVersionMigration] This operation captures data snapshot at binlog offset v1=%d", j.InsertBootstrapState.BootstrapCommitSeq)

	// Execute INSERT (this may take a long time for large tables)
	startTime := time.Now()
	log.Infof("[InsertBootstrap] [Progress] INSERT operation started at %s", startTime.Format(time.RFC3339))
	if err := j.ISrc.Exec(insertSql); err != nil {
		log.Errorf("[InsertBootstrap] Failed to execute INSERT: %+v", err)
		return err
	}
	duration := time.Since(startTime)
	endTime := time.Now()

	log.Infof("[InsertBootstrap] ----------------------------------------")
	log.Infof("[InsertBootstrap] INSERT completed successfully!")
	log.Infof("[InsertBootstrap]   Start Time: %s", startTime.Format(time.RFC3339))
	log.Infof("[InsertBootstrap]   End Time: %s", endTime.Format(time.RFC3339))
	log.Infof("[InsertBootstrap]   Duration: %v (%.2f seconds)", duration, duration.Seconds())
	log.Infof("[InsertBootstrap] [CrossVersionMigration] INSERT INTO tmp SELECT FROM old completed")
	log.Infof("[InsertBootstrap] ----------------------------------------")

	// Persist state and move to next phase
	j.persistInsertBootstrapState()
	j.progress.NextWithPersist(j.progress.CommitSeq, TableInsertBootstrap, IBWaitInsertDone, "")

	log.Infof("[InsertBootstrap] Phase 3 completed. Moving to Phase 4: Wait for INSERT binlog to be ready")
	return nil
}

// ibWaitInsertDone waits for INSERT operation to generate binlog
func (j *Job) ibWaitInsertDone() error {
	log.Infof("[InsertBootstrap] Phase 4: Waiting for INSERT binlog to be ready...")

	j.InsertBootstrapState.Phase = "wait_insert_done"

	// Give some time for binlog to be generated
	time.Sleep(3 * time.Second)

	// Verify that the temp table has data by checking if we can get binlog
	src := &j.Src
	srcRpc, err := j.factory.NewFeRpc(src)
	if err != nil {
		return err
	}

	// Check if there are new binlogs after our bootstrap position
	resp, err := srcRpc.GetBinlogLag(src, j.InsertBootstrapState.BootstrapCommitSeq)
	if err != nil {
		log.Warnf("[InsertBootstrap] Failed to get binlog lag, will retry: %+v", err)
		time.Sleep(insertBootstrapCheckInterval)
		return nil // Return nil to retry
	}

	lag := resp.GetLag()
	lastCommitSeq := resp.GetLastCommitSeq()
	firstCommitSeq := resp.GetFirstCommitSeq()

	log.Infof("[InsertBootstrap] ----------------------------------------")
	log.Infof("[InsertBootstrap] Binlog status after INSERT:")
	log.Infof("[InsertBootstrap]   BootstrapCommitSeq (v1): %d", j.InsertBootstrapState.BootstrapCommitSeq)
	log.Infof("[InsertBootstrap]   FirstCommitSeq: %d", firstCommitSeq)
	log.Infof("[InsertBootstrap]   LastCommitSeq: %d", lastCommitSeq)
	log.Infof("[InsertBootstrap]   Lag: %d binlogs", lag)
	log.Infof("[InsertBootstrap]   Binlog Range: [%d, %d]", firstCommitSeq, lastCommitSeq)
	
	if resp.GetLastBinlogTimestamp() != -1 {
		lastBinlogTime := time.Unix(0, resp.GetLastBinlogTimestamp()*int64(time.Millisecond))
		log.Infof("[InsertBootstrap]   LastBinlogTimestamp: %s", lastBinlogTime.Format(time.RFC3339))
	}
	log.Infof("[InsertBootstrap] ----------------------------------------")

	if lag > 0 {
		log.Infof("[InsertBootstrap] [Progress] INSERT binlog is ready, %d new binlogs found", lag)
		log.Infof("[InsertBootstrap] [CrossVersionMigration] Temp table now has binlogs from offset %d to %d", 
			j.InsertBootstrapState.BootstrapCommitSeq+1, lastCommitSeq)

		// Persist state and move to next phase
		j.persistInsertBootstrapState()
		j.progress.NextWithPersist(j.progress.CommitSeq, TableInsertBootstrap, IBSyncTempTable, "")

		log.Infof("[InsertBootstrap] Phase 4 completed. Moving to Phase 5: Sync temp table")
	} else {
		log.Infof("[InsertBootstrap] No new binlogs yet, waiting...")
		time.Sleep(insertBootstrapCheckInterval)
	}

	return nil
}

// ibSyncTempTable syncs the temp table to dest cluster using CCR
func (j *Job) ibSyncTempTable() error {
	log.Infof("[InsertBootstrap] Phase 5: Syncing temp table to dest cluster...")
	log.Infof("[InsertBootstrap]   Temp table: %s (ID: %d)", j.InsertBootstrapState.TempTableName, j.InsertBootstrapState.TempTableId)
	log.Infof("[InsertBootstrap]   Starting from commitSeq: %d", j.InsertBootstrapState.BootstrapCommitSeq)

	j.InsertBootstrapState.Phase = "sync_temp_table"

	// We need to sync the temp table from BootstrapCommitSeq
	// The binlogs after BootstrapCommitSeq should contain the INSERT data

	// First, we need to create the temp table in dest cluster
	// Get the CREATE TABLE statement
	createTableSql, err := j.ISrc.GetCreateTableSql(j.InsertBootstrapState.TempTableName)
	if err != nil {
		log.Errorf("[InsertBootstrap] Failed to get temp table CREATE SQL: %+v", err)
		return err
	}

	// Modify for dest cluster (change database name if needed)
	destCreateSql := j.modifyCreateTableForDest(createTableSql)
	log.Infof("[InsertBootstrap] Creating temp table in dest cluster...")
	log.Infof("[InsertBootstrap] SQL (first 500 chars): %.500s...", destCreateSql)

	// Check if temp table already exists in dest
	exists, err := j.IDest.CheckTableExistsByName(j.InsertBootstrapState.TempTableName)
	if err != nil {
		log.Warnf("[InsertBootstrap] Failed to check if temp table exists in dest: %+v", err)
	}

	if !exists {
		if err := j.IDest.Exec(destCreateSql); err != nil {
			log.Errorf("[InsertBootstrap] Failed to create temp table in dest: %+v", err)
			return err
		}
		log.Infof("[InsertBootstrap] Temp table created in dest cluster")
	} else {
		log.Infof("[InsertBootstrap] Temp table already exists in dest cluster, skipping creation")
	}

	// Get dest temp table ID
	destTempTableId, err := j.destMeta.GetTableId(j.InsertBootstrapState.TempTableName)
	if err != nil {
		log.Errorf("[InsertBootstrap] Failed to get dest temp table ID: %+v", err)
		return err
	}
	log.Infof("[InsertBootstrap] Dest temp table ID: %d", destTempTableId)

	// Set up table mapping for temp table
	j.progress.TableMapping = map[int64]int64{
		j.InsertBootstrapState.TempTableId: destTempTableId,
	}
	log.Infof("[InsertBootstrap] Table mapping: src %d -> dest %d",
		j.InsertBootstrapState.TempTableId, destTempTableId)

	// Set the commit seq to start from BootstrapCommitSeq
	j.progress.CommitSeq = j.InsertBootstrapState.BootstrapCommitSeq
	j.progress.PrevCommitSeq = j.InsertBootstrapState.BootstrapCommitSeq

	// Persist state and move to next phase
	j.persistInsertBootstrapState()
	j.progress.NextWithPersist(j.progress.CommitSeq, TableInsertBootstrap, IBWaitTempTableSync, "")

	log.Infof("[InsertBootstrap] Phase 5 setup completed. Moving to Phase 6: Wait for sync to complete")
	return nil
}

// modifyCreateTableForDest modifies CREATE TABLE SQL for dest cluster
func (j *Job) modifyCreateTableForDest(createTableSql string) string {
	// Replace source database name with dest database name if different
	srcDbName := utils.FormatKeywordName(j.Src.Database)
	destDbName := utils.FormatKeywordName(j.Dest.Database)

	result := strings.Replace(createTableSql, srcDbName+".", destDbName+".", -1)

	// Also replace default_cluster prefix if present
	srcOldStyle := fmt.Sprintf("`default_cluster:%s`.", j.Src.Database)
	destOldStyle := fmt.Sprintf("`default_cluster:%s`.", j.Dest.Database)
	result = strings.Replace(result, srcOldStyle, destOldStyle, -1)

	return result
}

// ibWaitTempTableSync waits for temp table sync to complete by processing binlogs
func (j *Job) ibWaitTempTableSync() error {
	log.Infof("[InsertBootstrap] ========================================")
	log.Infof("[InsertBootstrap] Phase 6: Waiting for temp table sync to complete...")
	log.Infof("[InsertBootstrap]   Current commitSeq: %d", j.progress.CommitSeq)
	log.Infof("[InsertBootstrap]   BootstrapCommitSeq: %d", j.InsertBootstrapState.BootstrapCommitSeq)
	log.Infof("[InsertBootstrap]   TempTableName: %s", j.InsertBootstrapState.TempTableName)
	log.Infof("[InsertBootstrap]   TempTableId: %d", j.InsertBootstrapState.TempTableId)
	log.Infof("[InsertBootstrap] ========================================")

	j.InsertBootstrapState.Phase = "wait_temp_table_sync"

	// IMPORTANT: Create a temporary Spec for the temp table to get its binlogs
	// The original j.Src points to the old table, we need to get binlogs for the temp table
	tempTableSpec := j.Src // Copy the base spec
	tempTableSpec.Table = j.InsertBootstrapState.TempTableName
	tempTableSpec.TableId = j.InsertBootstrapState.TempTableId

	log.Infof("[InsertBootstrap] Created temp table Spec: db=%s, table=%s, tableId=%d",
		tempTableSpec.Database, tempTableSpec.Table, tempTableSpec.TableId)

	srcRpc, err := j.factory.NewFeRpc(&tempTableSpec)
	if err != nil {
		log.Errorf("[InsertBootstrap] Failed to create FE RPC for temp table: %+v", err)
		return err
	}

	// Process binlogs until we catch up
	processedBinlogCount := 0
	startSyncTime := time.Now()
	
	for !j.hasInterruptSignal() {
		commitSeq := j.progress.CommitSeq

		log.Infof("[InsertBootstrap] [Progress] Fetching temp table binlog from commitSeq: %d", commitSeq)
		log.Infof("[InsertBootstrap] [CrossVersionMigration] Syncing tmp table binlog to dst old table, current offset: %d", commitSeq)

		// Get binlogs for the temp table specifically
		getBinlogResp, err := srcRpc.GetBinlog(&tempTableSpec, commitSeq, flagBinlogBatchSize)
		if err != nil {
			log.Errorf("[InsertBootstrap] Failed to get temp table binlog: %+v", err)
			return err
		}

		status := getBinlogResp.GetStatus()
		log.Infof("[InsertBootstrap] GetBinlog response status: %v", status.StatusCode)

		switch status.StatusCode {
		case tstatus.TStatusCode_OK:
			// Process binlogs
			binlogs := getBinlogResp.GetBinlogs()
			log.Infof("[InsertBootstrap] [Progress] Got %d temp table binlogs to process", len(binlogs))
			log.Infof("[InsertBootstrap] [CrossVersionMigration] Processing batch of %d binlogs, starting from offset %d", 
				len(binlogs), commitSeq)

			for i, binlog := range binlogs {
				binlogCommitSeq := binlog.GetCommitSeq()
				binlogType := binlog.GetType()
				tableId := binlog.GetTableRef()

				log.Infof("[InsertBootstrap] [Progress] Processing binlog [%d/%d in batch]: commitSeq=%d, type=%v, tableId=%d",
					i+1, len(binlogs), binlogCommitSeq, binlogType, tableId)
				log.Infof("[InsertBootstrap] [CrossVersionMigration] Binlog offset: %d, type: %v", binlogCommitSeq, binlogType)

				// Verify this binlog is for our temp table (should always be true since we filtered by table)
				if tableId != j.InsertBootstrapState.TempTableId {
					log.Warnf("[InsertBootstrap] Unexpected: binlog tableId=%d doesn't match tempTableId=%d, skipping",
						tableId, j.InsertBootstrapState.TempTableId)
					j.progress.CommitSeq = binlogCommitSeq
					j.progress.PrevCommitSeq = binlogCommitSeq
					continue
				}

				// Handle the binlog using existing logic
				// Note: The table mapping should already be set in ibSyncTempTable
				if err, back := j.handleBinlog(binlog); err != nil {
					log.Errorf("[InsertBootstrap] Failed to handle binlog at offset %d: %+v", binlogCommitSeq, err)
					return err
				} else if back {
					log.Infof("[InsertBootstrap] handleBinlog requested back, breaking...")
					break
				}

				// Update progress
				processedBinlogCount++
				j.progress.CommitSeq = binlogCommitSeq
				j.progress.PrevCommitSeq = binlogCommitSeq
				
				// Calculate progress percentage if we know the range
				progressInfo := fmt.Sprintf("Processed %d binlogs, current offset: %d", processedBinlogCount, j.progress.CommitSeq)
				if j.progress.CommitSeq > j.InsertBootstrapState.BootstrapCommitSeq {
					processedRange := j.progress.CommitSeq - j.InsertBootstrapState.BootstrapCommitSeq
					progressInfo += fmt.Sprintf(", processed range: [%d, %d] (%d binlogs)", 
						j.InsertBootstrapState.BootstrapCommitSeq+1, j.progress.CommitSeq, processedRange)
				}
				log.Infof("[InsertBootstrap] [Progress] Binlog processed successfully: %s", progressInfo)
				log.Infof("[InsertBootstrap] [CrossVersionMigration] Binlog offset updated: %d", j.progress.CommitSeq)
			}
			
			// Log batch completion
			elapsed := time.Since(startSyncTime)
			log.Infof("[InsertBootstrap] [Progress] Batch completed: processed %d binlogs in this batch, total processed: %d, elapsed: %v",
				len(binlogs), processedBinlogCount, elapsed)

		case tstatus.TStatusCode_BINLOG_TOO_OLD_COMMIT_SEQ:
			log.Warnf("[InsertBootstrap] Binlog too old, commitSeq: %d", commitSeq)
			// The binlog we're looking for is too old, this shouldn't happen in normal case
			return xerror.Errorf(xerror.Normal, "binlog too old: commitSeq %d", commitSeq)

		case tstatus.TStatusCode_BINLOG_NOT_FOUND_DB:
			log.Warnf("[InsertBootstrap] Database not found in binlog: %v", status)
			return xerror.Errorf(xerror.Normal, "binlog database not found: %v", status.ErrorMsgs)

		case tstatus.TStatusCode_BINLOG_NOT_FOUND_TABLE:
			// This is expected when temp table has no more binlogs or table was just created
			log.Infof("[InsertBootstrap] Temp table binlog not found (status: %v), this may be normal", status.StatusCode)
			log.Infof("[InsertBootstrap] Checking if we have synced enough...")

			// Check if we've already processed some binlogs
			if j.progress.CommitSeq > j.InsertBootstrapState.BootstrapCommitSeq {
				elapsed := time.Since(startSyncTime)
				log.Infof("[InsertBootstrap] [Progress] Already processed binlogs up to commitSeq: %d", j.progress.CommitSeq)
				log.Infof("[InsertBootstrap] [Progress] Total binlogs processed: %d, duration: %v", processedBinlogCount, elapsed)
				log.Infof("[InsertBootstrap] [CrossVersionMigration] Temp table sync completed, processed range: [%d, %d]",
					j.InsertBootstrapState.BootstrapCommitSeq+1, j.progress.CommitSeq)
				j.InsertBootstrapState.TempTableSyncCompleted = true
				j.persistInsertBootstrapState()
				j.progress.NextWithPersist(j.progress.CommitSeq, TableInsertBootstrap, IBRenameTables, "")
				log.Infof("[InsertBootstrap] Phase 6 completed. Moving to Phase 7: Rename tables")
				return nil
			}

			// If we haven't processed any binlogs yet, wait a bit and retry
			log.Infof("[InsertBootstrap] No binlogs processed yet, waiting for INSERT binlog to appear...")
			time.Sleep(insertBootstrapCheckInterval)
			continue

		case tstatus.TStatusCode_BINLOG_DISABLED:
			log.Warnf("[InsertBootstrap] Binlog disabled")
			return xerror.Errorf(xerror.Normal, "binlog disabled")

		case tstatus.TStatusCode_BINLOG_TOO_NEW_COMMIT_SEQ:
			// No more binlogs available, we've caught up
			elapsed := time.Since(startSyncTime)
			log.Infof("[InsertBootstrap] ----------------------------------------")
			log.Infof("[InsertBootstrap] No more temp table binlogs (BINLOG_TOO_NEW_COMMIT_SEQ)")
			log.Infof("[InsertBootstrap] Temp table sync completed!")
			log.Infof("[InsertBootstrap]   Final commitSeq: %d", j.progress.CommitSeq)
			log.Infof("[InsertBootstrap]   Total binlogs processed: %d", processedBinlogCount)
			log.Infof("[InsertBootstrap]   Sync duration: %v (%.2f seconds)", elapsed, elapsed.Seconds())
			log.Infof("[InsertBootstrap]   Binlog range synced: [%d, %d]", 
				j.InsertBootstrapState.BootstrapCommitSeq+1, j.progress.CommitSeq)
			log.Infof("[InsertBootstrap] ----------------------------------------")
			log.Infof("[InsertBootstrap] [CrossVersionMigration] Temp table binlog sync to dst old table completed!")
			log.Infof("[InsertBootstrap] [CrossVersionMigration] Final binlog offset: %d", j.progress.CommitSeq)

			j.InsertBootstrapState.TempTableSyncCompleted = true

			// Check lag to confirm
			lagResp, err := srcRpc.GetBinlogLag(&tempTableSpec, j.progress.CommitSeq)
			if err == nil {
				log.Infof("[InsertBootstrap] [Progress] Temp table binlog lag: %d", lagResp.GetLag())
				if lagResp.GetLag() == 0 {
					log.Infof("[InsertBootstrap] [CrossVersionMigration] Confirmed: all temp table binlogs have been synced (lag=0)")
				}
			}

			// Move to next phase
			j.persistInsertBootstrapState()
			j.progress.NextWithPersist(j.progress.CommitSeq, TableInsertBootstrap, IBRenameTables, "")

			log.Infof("[InsertBootstrap] Phase 6 completed. Moving to Phase 7: Rename tables")
			return nil

		default:
			// Unexpected status, treat as caught up
			log.Infof("[InsertBootstrap] Unexpected binlog status: %v, treating as caught up", status.StatusCode)
			j.InsertBootstrapState.TempTableSyncCompleted = true

			// Move to next phase
			j.persistInsertBootstrapState()
			j.progress.NextWithPersist(j.progress.CommitSeq, TableInsertBootstrap, IBRenameTables, "")

			log.Infof("[InsertBootstrap] Phase 6 completed. Moving to Phase 7: Rename tables")
			return nil
		}

		// Small delay between batches
		time.Sleep(100 * time.Millisecond)
	}

	log.Infof("[InsertBootstrap] Phase 6 interrupted by signal")
	return nil
}

// ibRenameTables renames tables in dest cluster
func (j *Job) ibRenameTables() error {
	log.Infof("[InsertBootstrap] Phase 7: Renaming tables in dest cluster...")

	j.InsertBootstrapState.Phase = "rename_tables"

	destDb := utils.FormatKeywordName(j.Dest.Database)
	originalTable := utils.FormatKeywordName(j.Dest.Table)
	tempTable := utils.FormatKeywordName(j.InsertBootstrapState.TempTableName)
	backupTableName := fmt.Sprintf("_ccr_ib_backup_%s_%d", j.Dest.Table, time.Now().Unix())
	backupTable := utils.FormatKeywordName(backupTableName)

	// Step 1: Check if original table exists in dest
	exists, err := j.IDest.CheckTableExistsByName(j.Dest.Table)
	if err != nil {
		log.Errorf("[InsertBootstrap] Failed to check if original table exists: %+v", err)
		return err
	}

	if exists {
		// Rename original table to backup
		renameSql := fmt.Sprintf("ALTER TABLE %s.%s RENAME %s", destDb, originalTable, backupTable)
		log.Infof("[InsertBootstrap] Backing up original table: %s", renameSql)
		if err := j.IDest.Exec(renameSql); err != nil {
			log.Errorf("[InsertBootstrap] Failed to backup original table: %+v", err)
			return err
		}
		j.InsertBootstrapState.OriginalTableBackedUp = true
		log.Infof("[InsertBootstrap] Original table backed up as: %s", backupTableName)
	} else {
		log.Infof("[InsertBootstrap] Original table does not exist in dest, no backup needed")
	}

	// Step 2: Rename temp table to original table name
	renameSql := fmt.Sprintf("ALTER TABLE %s.%s RENAME %s", destDb, tempTable, originalTable)
	log.Infof("[InsertBootstrap] Renaming temp table to original: %s", renameSql)
	if err := j.IDest.Exec(renameSql); err != nil {
		log.Errorf("[InsertBootstrap] Failed to rename temp table: %+v", err)
		return err
	}
	log.Infof("[InsertBootstrap] Temp table renamed successfully")

	// Persist state and move to next phase
	j.persistInsertBootstrapState()
	j.progress.NextWithPersist(j.progress.CommitSeq, TableInsertBootstrap, IBCleanup, "")

	log.Infof("[InsertBootstrap] Phase 7 completed. Moving to Phase 8: Cleanup")
	return nil
}

// ibCleanup cleans up temporary resources
func (j *Job) ibCleanup() error {
	log.Infof("[InsertBootstrap] Phase 8: Cleaning up temporary resources...")

	j.InsertBootstrapState.Phase = "cleanup"

	// Drop temp table in source cluster
	srcDb := utils.FormatKeywordName(j.Src.Database)
	tempTable := utils.FormatKeywordName(j.InsertBootstrapState.TempTableName)
	dropSql := fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", srcDb, tempTable)
	log.Infof("[InsertBootstrap] Dropping temp table in source: %s", dropSql)
	if err := j.ISrc.Exec(dropSql); err != nil {
		log.Warnf("[InsertBootstrap] Failed to drop temp table in source (non-fatal): %+v", err)
		// Non-fatal error, continue
	}

	// Optionally drop backup table in dest cluster
	// For now, we keep the backup table for safety
	log.Infof("[InsertBootstrap] Backup table in dest cluster is kept for safety")

	// Persist state and move to next phase
	j.persistInsertBootstrapState()
	j.progress.NextWithPersist(j.progress.CommitSeq, TableInsertBootstrap, IBSwitchToIncremental, "")

	log.Infof("[InsertBootstrap] Phase 8 completed. Moving to Phase 9: Switch to incremental sync")
	return nil
}

// ibSwitchToIncremental switches to incremental sync from the recorded position
func (j *Job) ibSwitchToIncremental() error {
	log.Infof("[InsertBootstrap] ========================================")
	log.Infof("[InsertBootstrap] Phase 9: Switching to incremental sync...")
	log.Infof("[InsertBootstrap] ========================================")

	j.InsertBootstrapState.Phase = "switch_to_incremental"

	// Refresh dest meta cache to get updated table info after rename
	j.destMeta.ClearTablesCache()

	// Get the dest table ID (now it's the renamed temp table)
	destTableId, err := j.destMeta.GetTableId(j.Dest.Table)
	if err != nil {
		log.Errorf("[InsertBootstrap] Failed to get dest table ID: %+v", err)
		return err
	}
	j.Dest.TableId = destTableId
	log.Infof("[InsertBootstrap] Dest table ID after rename: %d", destTableId)

	// Set up table mapping for the ORIGINAL table (not temp table)
	// This is critical: we need to map src original table -> dest (renamed from temp)
	j.progress.TableMapping = map[int64]int64{
		j.Src.TableId: destTableId,
	}
	log.Infof("[InsertBootstrap] ----------------------------------------")
	log.Infof("[InsertBootstrap] Table mapping configured for incremental sync:")
	log.Infof("[InsertBootstrap]   Source table: %s (ID: %d)", j.Src.Table, j.Src.TableId)
	log.Infof("[InsertBootstrap]   Dest table: %s (ID: %d)", j.Dest.Table, destTableId)
	log.Infof("[InsertBootstrap] ----------------------------------------")

	// Set the commit seq to the bootstrap position
	// The incremental sync will start from the position we recorded BEFORE the INSERT
	//
	// IMPORTANT: This is critical for data consistency:
	// - BootstrapCommitSeq was recorded before INSERT INTO tmp SELECT FROM old
	// - INSERT SELECT reads a snapshot of data as of that moment
	// - So incremental sync from BootstrapCommitSeq captures all changes since then
	//
	// NOTE: For UNIQUE KEY tables, any overlapping data will be deduplicated (idempotent)
	// For DUPLICATE KEY tables, there might be some duplicate records if writes happened
	// during the INSERT SELECT execution window. This is a known limitation.
	log.Infof("[InsertBootstrap] ----------------------------------------")
	log.Infof("[InsertBootstrap] Setting incremental sync starting point:")
	log.Infof("[InsertBootstrap]   Previous CommitSeq (from temp table sync): %d", j.progress.CommitSeq)
	log.Infof("[InsertBootstrap]   BootstrapCommitSeq (v1, before INSERT): %d", j.InsertBootstrapState.BootstrapCommitSeq)
	log.Infof("[InsertBootstrap]   Binlog offset v1: %d", j.InsertBootstrapState.BootstrapCommitSeq)

	j.progress.CommitSeq = j.InsertBootstrapState.BootstrapCommitSeq
	j.progress.PrevCommitSeq = j.InsertBootstrapState.BootstrapCommitSeq

	log.Infof("[InsertBootstrap] [CrossVersionMigration] CommitSeq reset to v1 offset: %d", j.InsertBootstrapState.BootstrapCommitSeq)
	log.Infof("[InsertBootstrap] [CrossVersionMigration] Will start pulling old table binlogs from offset v1=%d", 
		j.InsertBootstrapState.BootstrapCommitSeq)
	log.Infof("[InsertBootstrap] ----------------------------------------")

	// Switch to incremental sync state
	j.progress.SyncState = TableIncrementalSync
	j.progress.SubSyncState = Done
	j.progress.IncrementalSyncStartAt = time.Now().Unix()

	// Mark InsertBootstrap as completed but keep the state for reference
	j.InsertBootstrapState.Phase = "completed"

	// Persist final state
	j.persistInsertBootstrapState()
	j.progress.Persist()

	// Update job status in DB
	j.updateJobStatus()

	log.Infof("[InsertBootstrap] ========================================")
	log.Infof("[InsertBootstrap] INSERT BOOTSTRAP COMPLETED SUCCESSFULLY!")
	log.Infof("[InsertBootstrap] ========================================")
	log.Infof("[InsertBootstrap] Summary:")
	log.Infof("[InsertBootstrap]   Job Name: %s", j.Name)
	log.Infof("[InsertBootstrap]   Source: %s.%s (ID: %d)", j.Src.Database, j.Src.Table, j.Src.TableId)
	log.Infof("[InsertBootstrap]   Dest: %s.%s (ID: %d)", j.Dest.Database, j.Dest.Table, j.Dest.TableId)
	log.Infof("[InsertBootstrap]   BootstrapCommitSeq (v1): %d", j.InsertBootstrapState.BootstrapCommitSeq)
	log.Infof("[InsertBootstrap]   TempTable used: %s", j.InsertBootstrapState.TempTableName)
	log.Infof("[InsertBootstrap]   Insert Start Time: %d", j.InsertBootstrapState.InsertStartTime)
	log.Infof("[InsertBootstrap] ----------------------------------------")
	log.Infof("[InsertBootstrap] [CrossVersionMigration] ========================================")
	log.Infof("[InsertBootstrap] [CrossVersionMigration] Cross-version migration bootstrap completed!")
	log.Infof("[InsertBootstrap] [CrossVersionMigration] Now switching to incremental sync mode")
	log.Infof("[InsertBootstrap] [CrossVersionMigration] Will start pulling old table binlogs from offset v1=%d", 
		j.progress.CommitSeq)
	log.Infof("[InsertBootstrap] [CrossVersionMigration] Incremental sync will continuously replay binlogs from this offset")
	log.Infof("[InsertBootstrap] [CrossVersionMigration] ========================================")
	log.Infof("[InsertBootstrap] Now switching to TableIncrementalSync mode")
	log.Infof("[InsertBootstrap] Incremental sync will process binlogs from commitSeq: %d", j.progress.CommitSeq)
	log.Infof("[InsertBootstrap] ========================================")

	return nil
}

// persistInsertBootstrapState persists the InsertBootstrapState to the job
func (j *Job) persistInsertBootstrapState() {
	log.Infof("[InsertBootstrap] Persisting state: phase=%s, commitSeq=%d, tempTable=%s",
		j.InsertBootstrapState.Phase, j.InsertBootstrapState.BootstrapCommitSeq, j.InsertBootstrapState.TempTableName)

	// The state is persisted as part of the job JSON
	j.updateJobStatus()
}

// isInsertBootstrapMode returns true if the job is in InsertBootstrap mode
func (j *Job) isInsertBootstrapMode() bool {
	return j.InsertBootstrap && j.progress.SyncState == TableInsertBootstrap
}
