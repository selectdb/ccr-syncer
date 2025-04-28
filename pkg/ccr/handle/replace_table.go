package handle

import (
	"fmt"

	"github.com/selectdb/ccr_syncer/pkg/ccr"
	"github.com/selectdb/ccr_syncer/pkg/ccr/record"
	festruct "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/frontendservice"
	log "github.com/sirupsen/logrus"
)

func init() {
	ccr.RegisterJobHandle[*record.ReplaceTableRecord](festruct.TBinlogType_REPLACE_TABLE, &ReplaceTableHandle{})
}

type ReplaceTableHandle struct {
}

func (h *ReplaceTableHandle) IsBinlogCommitted(j *ccr.Job, r *record.ReplaceTableRecord) (bool, error) {
	// We can't determine whether the binlog is committed or not, trigger full sync.
	return true, j.NewSnapshot(j.GetJobProgress().UnknownCommitSeq, "the REPLACE_TABLE binlog state is unknown")
}

func (h *ReplaceTableHandle) IsIdempotent() bool {
	return false
}

func (h *ReplaceTableHandle) Handle(j *ccr.Job, commitSeq int64, record *record.ReplaceTableRecord) error {
	if j.SyncType == ccr.TableSync {
		info := fmt.Sprintf("replace table %s with fullsync in table sync, reset src table id from %d to %d, swap: %t",
			record.OriginTableName, record.OriginTableId, record.NewTableId, record.SwapTable)
		log.Infof("%s", info)
		j.Src.TableId = record.NewTableId
		return j.NewSnapshot(commitSeq, info)
	}

	if isAsyncMv, err := j.IsMaterializedViewTable(record.OriginTableId); err != nil {
		return err
	} else if isAsyncMv {
		log.Warnf("skip replace table for materialized view table %d", record.OriginTableId)
		return nil
	}

	if j.GetJobProgress().SyncState == ccr.DBTablesIncrementalSync {
		// if original table already committed, new partial snapshot with the new table
		// if new table already committed, new partial snapshot with the original table
		// if both table are committed, skip this binlog
		originTableSynced := j.GetJobProgress().TableCommitSeqMap[record.OriginTableId] >= commitSeq
		newTableSynced := j.GetJobProgress().TableCommitSeqMap[record.NewTableId] >= commitSeq
		if originTableSynced && newTableSynced {
			log.Infof("filter replace table binlog, both tables are synced, origin table %s id: %d, new table %s id: %d, commit seq: %d",
				record.OriginTableName, record.OriginTableId, record.NewTableName, record.NewTableId, commitSeq)
			return nil
		} else if originTableSynced && !record.SwapTable {
			log.Infof("filter replace table binlog, the origin table %s id %d already synced, commit seq: %d, swap = false",
				record.OriginTableName, record.OriginTableId, commitSeq)
			return nil
		} else if originTableSynced && record.SwapTable {
			log.Infof("force new partial snapshot, origin table %s id %d already synced, commit seq: %d",
				record.OriginTableName, record.OriginTableId, commitSeq)
			return j.NewPartialSnapshot(record.NewTableId, record.OriginTableName, nil, false, false)
		} else if newTableSynced && !record.SwapTable {
			log.Infof("filter replace table binlog, the new table %s id %d already synced, commit seq: %d, swap = false",
				record.NewTableName, record.NewTableId, commitSeq)
			return nil
		} else if newTableSynced && record.SwapTable {
			log.Infof("force new partial snapshot, new table %s id %d already synced, commit seq: %d",
				record.NewTableName, record.NewTableId, commitSeq)
			return j.NewPartialSnapshot(record.OriginTableId, record.NewTableName, nil, false, false)
		}
	}

	toName := record.OriginTableName
	fromName := record.NewTableName
	if err := j.IDest.ReplaceTable(fromName, toName, record.SwapTable); err != nil {
		return err
	}

	j.GetDestMeta().GetTables() // update id <=> name cache
	if j.GetJobProgress().TableNameMapping == nil {
		j.GetJobProgress().TableNameMapping = make(map[int64]string)
	}
	if record.SwapTable {
		// keep table mapping
		j.GetJobProgress().TableNameMapping[record.OriginTableId] = record.NewTableName
		j.GetJobProgress().TableNameMapping[record.NewTableId] = record.OriginTableName
	} else { // delete table1
		j.GetJobProgress().TableNameMapping[record.NewTableId] = record.OriginTableName
		delete(j.GetJobProgress().TableNameMapping, record.OriginTableId)
		delete(j.GetJobProgress().TableMapping, record.OriginTableId)
	}

	return nil
}
