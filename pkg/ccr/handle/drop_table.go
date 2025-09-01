package handle

import (
	"strings"

	"github.com/selectdb/ccr_syncer/pkg/ccr"
	"github.com/selectdb/ccr_syncer/pkg/ccr/record"
	festruct "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/frontendservice"
	"github.com/selectdb/ccr_syncer/pkg/xerror"
	log "github.com/sirupsen/logrus"
)

func init() {
	ccr.RegisterJobHandle[*record.DropTable](festruct.TBinlogType_DROP_TABLE, &DropTableHandle{})
}

type DropTableHandle struct {
	// The drop table binlog is idempotent
	IdempotentJobHandle[*record.DropTable]
}

func (h *DropTableHandle) Handle(j *ccr.Job, commitSeq int64, dropTable *record.DropTable) error {
	if j.SyncType != ccr.DBSync {
		return xerror.Errorf(xerror.Normal, "invalid sync type: %v", j.SyncType)
	}

	if !dropTable.IsView {
		if _, ok := j.GetJobProgress().TableMapping[dropTable.TableId]; !ok {
			log.Warnf("the dest table is not found, skip drop table binlog, src table id: %d, commit seq: %d",
				dropTable.TableId, commitSeq)
			// So that the sync state would convert to DBIncrementalSync,
			// see handlePartialSyncTableNotFound for details.
			delete(j.GetJobProgress().TableCommitSeqMap, dropTable.TableId)
			return nil
		}
	}

	tableName := dropTable.TableName
	// deprecated, `TableName` has been added after doris 2.0.0
	if tableName == "" {
		dirtySrcTables := j.GetSrcMeta().DirtyGetTables()
		srcTable, ok := dirtySrcTables[dropTable.TableId]
		if !ok {
			return xerror.Errorf(xerror.Normal, "table not found, tableId: %d", dropTable.TableId)
		}

		tableName = srcTable.Name
	}

	if dropTable.IsView {
		if err := j.IDest.DropView(tableName); err != nil {
			return xerror.Wrapf(err, xerror.Normal, "drop view %s", tableName)
		}
	} else {
		if err := j.IDest.DropTable(tableName, true); err != nil {
			// In apache/doris/common/ErrorCode.java
			//
			// ERR_WRONG_OBJECT(1347, new byte[]{'H', 'Y', '0', '0', '0'}, "'%s.%s' is not %s. %s.")
			if !strings.Contains(err.Error(), "is not TABLE") {
				return xerror.Wrapf(err, xerror.Normal, "drop table %s", tableName)
			} else if err = j.IDest.DropView(tableName); err != nil { // retry with drop view.
				return xerror.Wrapf(err, xerror.Normal, "drop view %s", tableName)
			}
		}
	}

	j.GetSrcMeta().ClearTablesCache()
	j.GetDestMeta().ClearTablesCache()
	delete(j.GetJobProgress().TableNameMapping, dropTable.TableId)
	delete(j.GetJobProgress().TableMapping, dropTable.TableId)
	return nil
}
