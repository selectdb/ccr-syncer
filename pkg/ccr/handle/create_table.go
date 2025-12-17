package handle

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/selectdb/ccr_syncer/pkg/ccr"
	"github.com/selectdb/ccr_syncer/pkg/ccr/record"
	festruct "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/frontendservice"
	"github.com/selectdb/ccr_syncer/pkg/xerror"
	log "github.com/sirupsen/logrus"
)

func init() {
	ccr.RegisterJobHandle[*record.CreateTable](festruct.TBinlogType_CREATE_TABLE, &CreateTableHandle{})
}

type CreateTableHandle struct {
	IdempotentJobHandle[*record.CreateTable]
}

// Check if error message indicates storage medium or capacity related issues
func isStorageMediumError(errMsg string) bool {
	// Doris returns "Failed to find enough backend" for storage/capacity issues
	return strings.Contains(strings.ToLower(errMsg), "failed to find enough backend")
}

// Set specific property in CREATE TABLE SQL
func setPropertyInCreateTableSql(createSql string, key string, value string) string {
	// Add property to PROPERTIES clause
	pattern := `(?i)(PROPERTIES\s*\(\s*)`
	replacement := fmt.Sprintf(`${1}"%s" = "%s", `, key, value)
	createSql = regexp.MustCompile(pattern).ReplaceAllString(createSql, replacement)

	// Clean up trailing comma if PROPERTIES was empty
	return ccr.FilterTailingCommaFromCreateTableSql(createSql)
}

// Set specific storage_medium in CREATE TABLE SQL
func setStorageMediumInCreateTableSql(createSql string, medium string) string {
	// Remove existing storage_medium first
	createSql = ccr.FilterStorageMediumFromCreateTableSql(createSql)
	return setPropertyInCreateTableSql(createSql, "storage_medium", medium)
}

// Set specific medium_allocation_mode in CREATE TABLE SQL
func setMediumAllocationModeInCreateTableSql(createSql string, mode string) string {
	// Remove existing medium_allocation_mode first
	createSql = ccr.FilterMediumAllocationModeFromCreateTableSql(createSql)
	return setPropertyInCreateTableSql(createSql, "medium_allocation_mode", mode)
}

// Process CREATE TABLE SQL according to storage medium policy
func processCreateTableSqlByMediumPolicy(j *ccr.Job, createTable *record.CreateTable) {
	storageMedium := j.StorageMedium
	mediumAllocationMode := j.MediumAllocationMode

	// Process storage_medium
	switch storageMedium {
	case ccr.StorageMediumSameWithUpstream:
		// Keep upstream storage_medium unchanged
		log.Infof("using same_with_upstream storage medium, keeping original storage_medium")

	case ccr.StorageMediumHDD:
		log.Infof("using hdd storage medium, setting storage_medium to hdd")
		createTable.Sql = setStorageMediumInCreateTableSql(createTable.Sql, "hdd")

	case ccr.StorageMediumSSD:
		log.Infof("using ssd storage medium, setting storage_medium to ssd")
		createTable.Sql = setStorageMediumInCreateTableSql(createTable.Sql, "ssd")

	default:
		log.Warnf("unknown storage medium: %s, falling back to filter storage_medium", storageMedium)
		if ccr.FeatureFilterStorageMedium {
			createTable.Sql = ccr.FilterStorageMediumFromCreateTableSql(createTable.Sql)
		}
	}

	// Process medium_allocation_mode from CCR job parameter
	if mediumAllocationMode != "" {
		log.Infof("setting medium_allocation_mode to %s", mediumAllocationMode)
		createTable.Sql = setMediumAllocationModeInCreateTableSql(createTable.Sql, mediumAllocationMode)
	}
}

func (h *CreateTableHandle) Handle(j *ccr.Job, commitSeq int64, createTable *record.CreateTable) error {
	if j.SyncType != ccr.DBSync {
		return xerror.Errorf(xerror.Normal, "invalid sync type: %v", j.SyncType)
	}

	if createTable.IsCreateElasticSearch() {
		log.Warnf("create table with elasticsearch is not supported yet, skip this binlog")
		return nil
	}

	if createTable.IsCreateMaterializedView() {
		log.Warnf("create async materialized view is not supported yet, skip this binlog")
		return nil
	}

	if ccr.FeatureCreateViewDropExists {
		tableName := strings.TrimSpace(createTable.TableName)
		if createTable.IsCreateView() && len(tableName) > 0 {
			// drop view if exists
			log.Infof("feature_create_view_drop_exists is enabled, try drop view %s before creating", tableName)
			if err := j.IDest.DropView(tableName); err != nil {
				return xerror.Wrapf(err, xerror.Normal, "drop view before create view %s, table id=%d",
					tableName, createTable.TableId)
			}
		}
	}

	if createTable.IsCreateTableWithInvertedIndex() {
		log.Infof("create table %s with inverted index, force partial snapshot, commit seq : %d", createTable.TableName, commitSeq)
		// we need to force replace table to ensure the index id is consistent
		return j.NewPartialSnapshot(createTable.TableId, createTable.TableName, nil, true, false)
	}

	// Some operations, such as DROP TABLE, will be skiped in the partial/full snapshot,
	// in that case, the dest table might already exists, so we need to check it before creating.
	// If the dest table already exists, we need to do a partial snapshot.
	//
	// See test_cds_fullsync_tbl_drop_create.groovy for details
	if j.SyncType == ccr.DBSync && !createTable.IsCreateView() {
		if exists, err := j.IDest.CheckTableExistsByName(createTable.TableName); err != nil {
			return err
		} else if exists {
			log.Warnf("the dest table %s already exists, force partial snapshot, commit seq: %d",
				createTable.TableName, commitSeq)
			replace := true
			isView := false
			return j.NewPartialSnapshot(createTable.TableId, createTable.TableName, nil, replace, isView)
		}
	}

	// Process SQL according to storage medium policy
	processCreateTableSqlByMediumPolicy(j, createTable)
	createTable.Sql = ccr.FilterDynamicPartitionStoragePolicyFromCreateTableSql(createTable.Sql)

	if ccr.FeatureOverrideReplicationNum() && j.ReplicationNum > 0 {
		createTable.Sql = ccr.ResetReplicationNumFromCreateTableSql(createTable.Sql, j.ReplicationNum)
	}

	if err := j.IDest.CreateTableOrView(createTable, j.Src.Database); err != nil {
		errMsg := err.Error()

		// Skip unsupported features
		if strings.Contains(errMsg, "Can not found function") {
			log.Warnf("skip creating table/view because the UDF function is not supported yet: %s", errMsg)
			return nil
		}
		if strings.Contains(errMsg, "Can not find resource") {
			log.Warnf("skip creating table/view for the resource is not supported yet: %s", errMsg)
			return nil
		}

		// Trigger partial snapshot for recoverable errors
		if createTable.IsCreateView() && strings.Contains(errMsg, "Unknown column") {
			log.Warnf("create view but the column is not found, trigger partial snapshot, commit seq: %d, msg: %s",
				commitSeq, errMsg)
			replace := false // new view no need to replace
			isView := true
			return j.NewPartialSnapshot(createTable.TableId, createTable.TableName, nil, replace, isView)
		}
		if len(createTable.TableName) > 0 && ccr.IsSessionVariableRequired(errMsg) {
			log.Infof("a session variable is required to create table %s, force partial snapshot, commit seq: %d, msg: %s",
				createTable.TableName, commitSeq, errMsg)
			replace := false // new table no need to replace
			isView := false
			return j.NewPartialSnapshot(createTable.TableId, createTable.TableName, nil, replace, isView)
		}

		// Storage medium related error: pause job and require manual intervention
		if isStorageMediumError(errMsg) {
			log.Errorf("create table %s failed due to storage medium issue, job will be paused. "+
				"Current storage_medium=%s. Please check target cluster resources or update storage_medium via API. Error: %s",
				createTable.TableName, j.StorageMedium, errMsg)
			return xerror.Panicf(xerror.Normal,
				"Create table failed: storage medium issue for table %s. "+
					"Current storage_medium=%s. Possible causes:\n"+
					"1. Storage medium (%s) not available on target cluster\n"+
					"2. Insufficient disk capacity\n"+
					"3. Replication number exceeds available BE nodes\n"+
					"Please check target cluster configuration or update storage_medium via /update_storage_medium API. "+
					"Original error: %s",
				createTable.TableName, j.StorageMedium, j.StorageMedium, errMsg)
		}

		return xerror.Wrapf(err, xerror.Normal, "create table %d", createTable.TableId)
	}

	j.GetSrcMeta().ClearTablesCache()
	j.GetDestMeta().ClearTablesCache()

	var srcTableName string

	srcTableName = createTable.TableName
	if len(srcTableName) == 0 {
		// the field `TableName` is added after doris 2.0.3, to keep compatible, try read src table
		// name from upstream, but the result might be wrong if upstream has executed rename/replace.
		log.Infof("the table id %d is not found in the binlog record, get the name from the upstream", createTable.TableId)
		var err error
		srcTableName, err = j.GetSrcMeta().GetTableNameById(createTable.TableId)
		if err != nil {
			return xerror.Errorf(xerror.Normal, "the table with id %d is not found in the upstream cluster, create table: %s",
				createTable.TableId, createTable.String())
		}
	}

	job_progress := j.GetJobProgress()
	destTableId, err := j.GetDestMeta().GetTableId(srcTableName)
	if err != nil {
		return err
	}

	if job_progress.TableMapping == nil {
		job_progress.TableMapping = make(map[int64]int64)
	}
	job_progress.TableMapping[createTable.TableId] = destTableId
	if job_progress.TableNameMapping == nil {
		job_progress.TableNameMapping = make(map[int64]string)
	}
	job_progress.TableNameMapping[createTable.TableId] = srcTableName
	return nil
}
