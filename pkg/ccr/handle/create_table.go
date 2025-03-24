package handle

import (
	"strings"

	"github.com/cloudwego/kitex/tool/internal_pkg/log"
	"github.com/selectdb/ccr_syncer/pkg/ccr"
	"github.com/selectdb/ccr_syncer/pkg/ccr/record"
	festruct "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/frontendservice"
	"github.com/selectdb/ccr_syncer/pkg/xerror"
)

func init() {
	ccr.RegisterJobHandle[*record.CreateTable](festruct.TBinlogType_CREATE_TABLE, &CreateTableHandle{})
}

type CreateTableHandle struct {
	IdempotentJobHandle[*record.CreateTable]
}

func (h *CreateTableHandle) Handle(j *ccr.Job, commitSeq int64, createTable *record.CreateTable) error {
	if j.SyncType != ccr.DBSync {
		return xerror.Errorf(xerror.Normal, "invalid sync type: %v", j.SyncType)
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

	if ccr.FeatureFilterStorageMedium {
		createTable.Sql = ccr.FilterStorageMediumFromCreateTableSql(createTable.Sql)
	}
	createTable.Sql = ccr.FilterDynamicPartitionStoragePolicyFromCreateTableSql(createTable.Sql)

	if err := j.IDest.CreateTableOrView(createTable, j.Src.Database); err != nil {
		errMsg := err.Error()
		if strings.Contains(errMsg, "Can not found function") {
			log.Warnf("skip creating table/view because the UDF function is not supported yet: %s", errMsg)
			return nil
		} else if strings.Contains(errMsg, "Can not find resource") {
			log.Warnf("skip creating table/view for the resource is not supported yet: %s", errMsg)
			return nil
		} else if createTable.IsCreateView() && strings.Contains(errMsg, "Unknown column") {
			log.Warnf("create view but the column is not found, trigger partial snapshot, commit seq: %d, msg: %s",
				commitSeq, errMsg)
			replace := false // new view no need to replace
			isView := true
			return j.NewPartialSnapshot(createTable.TableId, createTable.TableName, nil, replace, isView)
		}
		if len(createTable.TableName) > 0 && ccr.IsSessionVariableRequired(errMsg) { // ignore doris 2.0.3
			log.Infof("a session variable is required to create table %s, force partial snapshot, commit seq: %d, msg: %s",
				createTable.TableName, commitSeq, errMsg)
			replace := false // new table no need to replace
			isView := false
			return j.NewPartialSnapshot(createTable.TableId, createTable.TableName, nil, replace, isView)
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
