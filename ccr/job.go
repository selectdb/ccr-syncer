package ccr

// TODO: rewrite by state machine, such as first sync, full/incremental sync

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/selectdb/ccr_syncer/ccr/base"
	"github.com/selectdb/ccr_syncer/ccr/record"
	"github.com/selectdb/ccr_syncer/rpc"
	"github.com/selectdb/ccr_syncer/storage"
	"github.com/selectdb/ccr_syncer/utils"
	u "github.com/selectdb/ccr_syncer/utils"

	bestruct "github.com/selectdb/ccr_syncer/rpc/kitex_gen/backendservice"
	festruct "github.com/selectdb/ccr_syncer/rpc/kitex_gen/frontendservice"
	tstatus "github.com/selectdb/ccr_syncer/rpc/kitex_gen/status"
	ttypes "github.com/selectdb/ccr_syncer/rpc/kitex_gen/types"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"go.uber.org/zap"

	_ "github.com/go-sql-driver/mysql"
)

const (
	SYNC_DURATION = time.Second * 5
)

type SyncType int

const (
	DBSync    SyncType = 0
	TableSync SyncType = 1
)

func (s SyncType) String() string {
	switch s {
	case DBSync:
		return "db_sync"
	case TableSync:
		return "table_sync"
	default:
		return "unknown_sync"
	}
}

type Job struct {
	SyncType          SyncType        `json:"sync_type"`
	Name              string          `json:"name"`
	Src               base.Spec       `json:"src"`
	srcMeta           *Meta           `json:"-"`
	Dest              base.Spec       `json:"dest"`
	destMeta          *Meta           `json:"-"`
	destSrcTableIdMap map[int64]int64 `json:"-"`
	progress          *JobProgress    `json:"-"`
	db                storage.DB      `json:"-"`
	stop              chan struct{}   `json:"-"`
	lock              sync.Mutex      `json:"-"`
}

// new job
func NewJobFromService(name string, src, dest base.Spec, db storage.DB) (*Job, error) {
	job := &Job{
		Name:              name,
		Src:               src,
		srcMeta:           NewMeta(&src),
		Dest:              dest,
		destMeta:          NewMeta(&dest),
		destSrcTableIdMap: make(map[int64]int64),
		progress:          nil,
		db:                db,
		stop:              make(chan struct{}),
	}

	if err := job.valid(); err != nil {
		return nil, errors.Wrap(err, "job is invalid")
	}

	if job.Src.Table == "" {
		job.SyncType = DBSync
	} else {
		job.SyncType = TableSync
	}

	return job, nil
}

func NewJobFromJson(jsonData string, db storage.DB) (*Job, error) {
	var job Job
	err := json.Unmarshal([]byte(jsonData), &job)
	if err != nil {
		return nil, errors.Wrapf(err, "unmarshal json failed, json: %s", jsonData)
	}

	job.srcMeta = NewMeta(&job.Src)
	job.destMeta = NewMeta(&job.Dest)
	job.destSrcTableIdMap = make(map[int64]int64)
	job.progress = nil
	job.db = db
	job.stop = make(chan struct{})
	return &job, nil
}

func (j *Job) valid() error {
	var err error
	if exist, err := j.db.IsJobExist(j.Name); err != nil {
		return errors.Wrap(err, "check job exist failed")
	} else if exist {
		return errors.Errorf("job %s already exist", j.Name)
	}

	if j.Name == "" {
		return errors.New("name is empty")
	}

	err = j.Src.Valid()
	if err != nil {
		return errors.Wrap(err, "src spec is invalid")
	}

	err = j.Dest.Valid()
	if err != nil {
		return errors.Wrap(err, "dest spec is invalid")
	}

	if (j.Src.Table == "" && j.Dest.Table != "") || (j.Src.Table != "" && j.Dest.Table == "") {
		return errors.New("src/dest are not both db or table sync")
	}

	return nil
}

func (j *Job) RecoverDatabaseSync() error {
	// TODO(Drogon): impl
	return nil
}

// database old data sync
func (j *Job) DatabaseOldDataSync() error {
	// TODO(Drogon): impl
	// Step 1: drop all tables
	err := j.Dest.ClearDB()
	if err != nil {
		return err
	}

	// Step 2: make snapshot

	return nil
}

// database sync
func (j *Job) DatabaseSync() error {
	// TODO(Drogon): impl
	return nil
}

func (j *Job) genExtraInfo() (*base.ExtraInfo, error) {
	meta := j.srcMeta
	masterToken, err := meta.GetMasterToken()
	if err != nil {
		return nil, err
	}

	backends, err := meta.GetBackends()
	if err != nil {
		return nil, err
	}

	log.Tracef("found backends: %v", backends)

	beNetworkMap := make(map[int64]base.NetworkAddr)
	for _, backend := range backends {
		log.Infof("backend: %v", backend)
		addr := base.NetworkAddr{
			Ip:   backend.Host,
			Port: backend.HttpPort,
		}
		beNetworkMap[backend.Id] = addr
	}

	return &base.ExtraInfo{
		BeNetworkMap: beNetworkMap,
		Token:        masterToken,
	}, nil
}

func (j *Job) fullSync() error {
	type inMemoryData struct {
		SnapshotName      string                        `json:"snapshot_name"`
		SnapshotResp      *festruct.TGetSnapshotResult_ `json:"snapshot_resp"`
		TableCommitSeqMap map[int64]int64               `json:"table_commit_seq_map"`
	}

	// TODO: snapshot machine, not need create snapshot each time
	// TODO(Drogon): check last snapshot commitSeq > first commitSeq, maybe we can reuse this snapshot
	switch j.progress.SubSyncState {
	case BeginCreateSnapshot:
		// Step 1: Create snapshot
		log.Debugf("create snapshot")

		backupTableList := make([]string, 0)
		switch j.SyncType {
		case DBSync:
			tables, err := j.srcMeta.GetTables()
			if err != nil {
				return err
			}
			for _, table := range tables {
				backupTableList = append(backupTableList, table.Name)
			}
		case TableSync:
			backupTableList = append(backupTableList, j.Src.Table)
		default:
			return errors.Errorf("invalid sync type %s", j.SyncType)
		}
		snapshotName, err := j.Src.CreateSnapshotAndWaitForDone(backupTableList)
		if err != nil {
			return err
		}

		j.progress.NextSubWithPersist(GetSnapshotInfo, snapshotName)

	case GetSnapshotInfo:
		// Step 2: Get snapshot info
		log.Tracef("get snapshot info")

		snapshotName := j.progress.PersistData
		src := &j.Src
		srcRpc, err := rpc.NewThriftRpc(src)
		if err != nil {
			return err
		}

		log.Debugf("begin get snapshot %s", snapshotName)
		snapshotResp, err := srcRpc.GetSnapshot(src, snapshotName)
		if err != nil {
			return err
		}
		log.Debugf("job: %s", string(snapshotResp.GetJobInfo()))

		if !snapshotResp.IsSetJobInfo() {
			return errors.New("jobInfo is not set")
		}

		tableCommitSeqMap, err := ExtractTableCommitSeqMap(snapshotResp.GetJobInfo())
		if err != nil {
			return err
		}

		if j.SyncType == TableSync {
			if _, ok := tableCommitSeqMap[j.Src.TableId]; !ok {
				return errors.Errorf("tableid %d, commit seq not found", j.Src.TableId)
			}
		}

		inMemoryData := &inMemoryData{
			SnapshotName:      snapshotName,
			SnapshotResp:      snapshotResp,
			TableCommitSeqMap: tableCommitSeqMap,
		}
		j.progress.NextSub(AddExtraInfo, inMemoryData)

	case AddExtraInfo:
		// Step 3: Add extra info
		log.Tracef("add extra info")

		inMemoryData := j.progress.InMemoryData.(*inMemoryData)
		snapshotResp := inMemoryData.SnapshotResp
		jobInfo := snapshotResp.GetJobInfo()
		tableCommitSeqMap := inMemoryData.TableCommitSeqMap

		var jobInfoMap map[string]interface{}
		err := json.Unmarshal(jobInfo, &jobInfoMap)
		if err != nil {
			return errors.Wrapf(err, "unmarshal jobInfo failed, jobInfo: %s", string(jobInfo))
		}
		log.Debugf("jobInfo: %v", jobInfoMap)

		extraInfo, err := j.genExtraInfo()
		if err != nil {
			return err
		}
		log.Infof("extraInfo: %v", extraInfo)

		jobInfoMap["extra_info"] = extraInfo
		jobInfoBytes, err := json.Marshal(jobInfoMap)
		if err != nil {
			return errors.Errorf("marshal jobInfo failed, jobInfo: %v", jobInfoMap)
		}
		log.Infof("jobInfoBytes: %s", string(jobInfoBytes))
		snapshotResp.SetJobInfo(jobInfoBytes)

		var commitSeq int64 = math.MaxInt64
		switch j.SyncType {
		case DBSync:
			for _, seq := range tableCommitSeqMap {
				commitSeq = utils.Min(commitSeq, seq)
			}
			j.progress.TableCommitSeqMap = tableCommitSeqMap // persist in CommitNext
		case TableSync:
			commitSeq = tableCommitSeqMap[j.Src.TableId]
		}
		j.progress.CommitNextSubWithPersist(commitSeq, RestoreSnapshot, inMemoryData)

	case RestoreSnapshot:
		// Step : Restore snapshot
		log.Tracef("restore snapshot")

		if j.progress.InMemoryData == nil {
			persistData := j.progress.PersistData
			inMemoryData := &inMemoryData{}
			if err := json.Unmarshal([]byte(persistData), inMemoryData); err != nil {
				// TODO: return to snapshot
				return errors.Errorf("unmarshal persistData failed, persistData: %s", persistData)
			}
			j.progress.InMemoryData = inMemoryData
		}

		// Step 4: start a new fullsync && persist
		inMemoryData := j.progress.InMemoryData.(*inMemoryData)
		snapshotName := inMemoryData.SnapshotName
		snapshotResp := inMemoryData.SnapshotResp

		// Step 5: restore snapshot
		// Restore snapshot to dest
		dest := &j.Dest
		destRpc, err := rpc.NewThriftRpc(dest)
		if err != nil {
			return err
		}
		log.Debugf("begin restore snapshot %s", snapshotName)
		restoreResp, err := destRpc.RestoreSnapshot(dest, snapshotName, snapshotResp)
		if err != nil {
			return err
		}
		log.Infof("resp: %v", restoreResp)
		// TODO: impl wait for done, use show restore
		restoreFinished, err := j.Dest.CheckRestoreFinished(snapshotName)
		if err != nil {
			return err
		}
		if !restoreFinished {
			err = errors.Errorf("check restore state timeout, max try times: %d", base.MAX_CHECK_RETRY_TIMES)
			return err
		}
		j.progress.NextSubWithPersist(PersistRestoreInfo, snapshotName)

	case PersistRestoreInfo:
		// Step 6: Update job progress && dest table id
		// update job info, only for dest table id
		log.Tracef("persist restore info")

		// TODO: retry && mark it for not start a new full sync
		switch j.SyncType {
		case DBSync:
			j.progress.NextWithPersist(j.progress.CommitSeq, DBTablesIncrementalSync, DB_1, "")
		case TableSync:
			if destTableId, err := j.destMeta.GetTableId(j.Dest.Table); err != nil {
				return err
			} else {
				j.Dest.TableId = destTableId
			}

			// TODO: reload check job table id
			if err := j.persistJob(); err != nil {
				return err
			}

			j.progress.TableCommitSeqMap = nil
			j.progress.NextWithPersist(j.progress.CommitSeq, TableIncrementalSync, DB_1, "")
		default:
			return errors.Errorf("invalid sync type %d", j.SyncType)
		}

		return nil
	default:
		return errors.Errorf("invalid job sub sync state %d", j.progress.SubSyncState)
	}

	return j.fullSync()
}

func (j *Job) persistJob() error {
	data, err := json.Marshal(j)
	if err != nil {
		return errors.Errorf("marshal job failed, job: %v", j)
	}

	if err := j.db.UpdateJob(j.Name, string(data)); err != nil {
		return err
	}

	return nil
}

func (j *Job) newLabel(commitSeq int64) string {
	src := &j.Src
	dest := &j.Dest
	if j.SyncType == DBSync {
		// label "ccrj:${sync_type}:${src_db_id}:${dest_db_id}:${commit_seq}"
		return fmt.Sprintf("ccrj:%s:%d:%d:%d", j.SyncType, src.DbId, dest.DbId, commitSeq)
	} else {
		// TableSync
		// label "ccrj:${sync_type}:${src_db_id}_${src_table_id}:${dest_db_id}_${dest_table_id}:${commit_seq}"
		return fmt.Sprintf("ccrj:%s:%d_%d:%d_%d:%d", j.SyncType, src.DbId, src.TableId, dest.DbId, dest.TableId, commitSeq)
	}
}

// only called by DBSync, TableSync tableId is in Src/Dest Spec
// TODO: [Performance] improve by cache
func (j *Job) getDestTableIdBySrc(srcTableId int64) (int64, error) {
	if destTableId, ok := j.destSrcTableIdMap[srcTableId]; ok {
		return destTableId, nil
	}

	srcTableName, err := j.srcMeta.GetTableNameById(srcTableId)
	if err != nil {
		return 0, err
	}

	if destTableId, err := j.destMeta.GetTableId(srcTableName); err != nil {
		return 0, err
	} else {
		j.destSrcTableIdMap[srcTableId] = destTableId
		return destTableId, nil
	}
}

func (j *Job) getDbSyncTableRecords(upsert *record.Upsert) ([]*record.TableRecord, error) {
	commitSeq := upsert.CommitSeq
	tableCommitSeqMap := j.progress.TableCommitSeqMap
	tableRecords := make([]*record.TableRecord, 0, len(upsert.TableRecords))

	for tableId, tableRecord := range upsert.TableRecords {
		// DBIncrementalSync
		if tableCommitSeqMap == nil {
			tableRecords = append(tableRecords, tableRecord)
			continue
		}

		if tableCommitSeq, ok := tableCommitSeqMap[tableId]; ok {
			if commitSeq > tableCommitSeq {
				tableRecords = append(tableRecords, tableRecord)
			}
		} else {
			// TODO: check
		}
	}

	return tableRecords, nil
}

func (j *Job) getReleatedTableRecords(upsert *record.Upsert) ([]*record.TableRecord, error) {
	var tableRecords []*record.TableRecord //, 0, len(upsert.TableRecords))

	switch j.SyncType {
	case DBSync:
		records, err := j.getDbSyncTableRecords(upsert)
		if err != nil {
			return nil, err
		}

		if len(records) == 0 {
			return nil, nil
		}
		tableRecords = records
	case TableSync:
		tableRecord, ok := upsert.TableRecords[j.Src.TableId]
		if !ok {
			return nil, errors.Errorf("table record not found, table: %s", j.Src.Table)
		}
		tableRecords = make([]*record.TableRecord, 0, 1)
		tableRecords = append(tableRecords, tableRecord)
	default:
		return nil, errors.Errorf("invalid sync type: %s", j.SyncType)
	}

	return tableRecords, nil
}

// Table ingestBinlog
// TODO: add check success, check ingestBinlog commitInfo
// TODO: rewrite by use tableId
func (j *Job) ingestBinlog(txnId int64, tableRecords []*record.TableRecord) ([]*ttypes.TTabletCommitInfo, error) {
	log.Tracef("ingestBinlog, txnId: %d", txnId)

	var wg sync.WaitGroup
	var srcBackendMap map[int64]*base.Backend
	var err error
	srcBackendMap, err = j.srcMeta.GetBackendMap()
	if err != nil {
		return nil, err
	}
	var destBackendMap map[int64]*base.Backend
	destBackendMap, err = j.destMeta.GetBackendMap()
	if err != nil {
		return nil, err
	}

	var lastErr error
	var lastErrLock sync.RWMutex
	updateLastError := func(err error) {
		lastErrLock.Lock()
		lastErr = err
		lastErrLock.Unlock()
	}
	getLastError := func() error {
		lastErrLock.RLock()
		defer lastErrLock.RUnlock()
		return lastErr
	}
	commitInfos := make([]*ttypes.TTabletCommitInfo, 0)
	var commitInfosLock sync.Mutex
	updateCommitInfos := func(commitInfo *ttypes.TTabletCommitInfo) {
		commitInfosLock.Lock()
		commitInfos = append(commitInfos, commitInfo)
		commitInfosLock.Unlock()
	}

	for _, tableRecord := range tableRecords {
		if getLastError() != nil {
			break
		}

		log.Tracef("tableRecord: %v", tableRecord)
		// TODO: check it before ingestBinlog
		var srcTableId int64
		var destTableId int64

		var err error
		switch j.SyncType {
		case TableSync:
			srcTableId = j.Src.TableId
			destTableId = j.Dest.TableId
		case DBSync:
			srcTableId = tableRecord.Id
			destTableId, err = j.getDestTableIdBySrc(tableRecord.Id)
			if err != nil {
				break
			}
		default:
			err = errors.Errorf("invalid sync type: %s", j.SyncType)
		}
		if err != nil {
			updateLastError(err)
			break
		}

		for _, partitionRecord := range tableRecord.PartitionRecords {
			log.Tracef("partitionRecord: %v", partitionRecord)
			binlogVersion := partitionRecord.Version

			srcPartitionId := partitionRecord.PartitionID
			var srcPartitionName string
			srcPartitionName, err = j.srcMeta.GetPartitionName(srcTableId, srcPartitionId)
			if err != nil {
				updateLastError(err)
				break
			}
			var destPartitionId int64
			destPartitionId, err = j.destMeta.GetPartitionIdByName(destTableId, srcPartitionName)
			if err != nil {
				updateLastError(err)
				break
			}

			var srcTablets []*TabletMeta
			srcTablets, err = j.srcMeta.GetTabletList(srcTableId, srcPartitionId)
			if err != nil {
				updateLastError(err)
				break
			}
			var destTablets []*TabletMeta
			destTablets, err = j.destMeta.GetTabletList(destTableId, destPartitionId)
			if err != nil {
				updateLastError(err)
				break
			}
			if len(srcTablets) != len(destTablets) {
				updateLastError(errors.Errorf("tablet count not match, src: %d, dest: %d", len(srcTablets), len(destTablets)))
				break
			}

			for tabletIndex, destTablet := range destTablets {
				srcTablet := srcTablets[tabletIndex]
				log.Debugf("handle tablet index: %v, src tablet: %v, dest tablet: %v, dest replicas length: %d", tabletIndex, srcTablet, destTablet, destTablet.ReplicaMetas.Len()) // TODO: remove it

				// iterate dest replicas
				destTablet.ReplicaMetas.Scan(func(destReplicaId int64, destReplica *ReplicaMeta) bool {
					log.Debugf("handle dest replica id: %v", destReplicaId)
					destBackend, ok := destBackendMap[destReplica.BackendId]
					if !ok {
						lastErr = errors.Errorf("backend not found, backend id: %d", destReplica.BackendId)
						return false
					}
					destTabletId := destReplica.TabletId

					destRpc, err := rpc.NewBeThriftRpc(destBackend)
					if err != nil {
						updateLastError(err)
						return false
					}
					loadId := ttypes.NewTUniqueId()
					loadId.SetHi(-1)
					loadId.SetLo(-1)

					srcReplicas := srcTablet.ReplicaMetas
					iter := srcReplicas.Iter()
					if ok := iter.First(); !ok {
						updateLastError(errors.Errorf("src replicas is empty"))
						return false
					}
					srcBackendId := iter.Value().BackendId
					var srcBackend *base.Backend
					srcBackend, ok = srcBackendMap[srcBackendId]
					if !ok {
						updateLastError(errors.Errorf("backend not found, backend id: %d", srcBackendId))
						return false
					}
					req := &bestruct.TIngestBinlogRequest{
						TxnId:          u.ThriftValueWrapper(txnId),
						RemoteTabletId: u.ThriftValueWrapper[int64](srcTablet.Id),
						BinlogVersion:  u.ThriftValueWrapper(binlogVersion),
						RemoteHost:     u.ThriftValueWrapper(srcBackend.Host),
						RemotePort:     u.ThriftValueWrapper(srcBackend.GetHttpPortStr()),
						PartitionId:    u.ThriftValueWrapper[int64](destPartitionId),
						LocalTabletId:  u.ThriftValueWrapper[int64](destTabletId),
						LoadId:         loadId,
					}
					commitInfo := &ttypes.TTabletCommitInfo{
						TabletId:  destTabletId,
						BackendId: destBackend.Id,
					}

					wg.Add(1)
					go func() {
						defer wg.Done()

						resp, err := destRpc.IngestBinlog(req)
						if err != nil {
							updateLastError(err)
							return
						}

						log.Debugf("ingest resp: %v", resp)
						if !resp.IsSetStatus() {
							err = errors.Errorf("ingest resp status not set")
							updateLastError(err)
							return
						} else if resp.Status.StatusCode != tstatus.TStatusCode_OK {
							err = errors.Errorf("ingest resp status code: %v, msg: %v", resp.Status.StatusCode, resp.Status.ErrorMsgs)
							updateLastError(err)
							return
						} else {
							updateCommitInfos(commitInfo)
						}
					}()

					return true
				})
				if getLastError() != nil {
					break
				}
			}
		}
	}

	wg.Wait()

	if lastErr != nil {
		return nil, lastErr
	}
	return commitInfos, nil
}

// TODO: handle error by abort txn
func (j *Job) handleUpsert(binlog *festruct.TBinlog) error {
	log.Tracef("handle upsert binlog")

	data := binlog.GetData()
	upsert, err := record.NewUpsertFromJson(data)
	if err != nil {
		return err
	}
	log.Tracef("upsert: %v", upsert)

	dest := &j.Dest
	commitSeq := upsert.CommitSeq

	// Step 1: get related tableRecords
	tableRecords, err := j.getReleatedTableRecords(upsert)
	if err != nil {
		log.Errorf("get releated table records failed, err: %v", err)
	}
	if len(tableRecords) == 0 {
		return nil
	}
	log.Tracef("tableRecords: %v", tableRecords)
	destTableIds := make([]int64, 0, len(tableRecords))
	if j.SyncType == DBSync {
		for _, tableRecord := range tableRecords {
			if destTableId, err := j.getDestTableIdBySrc(tableRecord.Id); err != nil {
				return err
			} else {
				destTableIds = append(destTableIds, destTableId)
			}
		}
	} else {
		destTableIds = append(destTableIds, j.Dest.TableId)
	}

	// Step 2: begin txn
	log.Tracef("begin txn, dest: %v, commitSeq: %d", dest, commitSeq)
	destRpc, err := rpc.NewThriftRpc(dest)
	if err != nil {
		return err
	}

	label := j.newLabel(commitSeq)

	beginTxnResp, err := destRpc.BeginTransaction(dest, label, destTableIds)
	if err != nil {
		return err
	}
	log.Infof("resp: %v", beginTxnResp)
	if beginTxnResp.GetStatus().GetStatusCode() != tstatus.TStatusCode_OK {
		return errors.Errorf("begin txn failed, status: %v", beginTxnResp.GetStatus())
	}
	txnId := beginTxnResp.GetTxnId()
	log.Infof("TxnId: %d, DbId: %d", txnId, beginTxnResp.GetDbId())

	j.progress.BeginTransaction(txnId)

	// Step 3: ingest binlog
	var commitInfos []*ttypes.TTabletCommitInfo
	commitInfos, err = j.ingestBinlog(txnId, tableRecords)
	if err != nil {
		return err
	}
	log.Tracef("commitInfos: %v", commitInfos)

	// Step 4: commit txn
	resp, err := destRpc.CommitTransaction(dest, txnId, commitInfos)
	if err != nil {
		return err
	}
	log.Debugf("commit resp: %v", resp)

	if j.SyncType == DBSync && len(j.progress.TableCommitSeqMap) > 0 {
		for tableId := range upsert.TableRecords {
			tableCommitSeq, ok := j.progress.TableCommitSeqMap[tableId]
			if !ok {
				continue
			}

			if tableCommitSeq < commitSeq {
				j.progress.TableCommitSeqMap[tableId] = commitSeq
			}
			// TODO: [PERFORMANCE] remove old commit seq
		}

		j.progress.Persist()
	}

	return nil
}

// handleAddPartition
func (j *Job) handleAddPartition(binlog *festruct.TBinlog) error {
	log.Tracef("handle add partition binlog")

	data := binlog.GetData()
	addPartition, err := record.NewAddPartitionFromJson(data)
	if err != nil {
		return err
	}

	destDbName := j.Dest.Database
	var destTableName string
	if j.SyncType == TableSync {
		destTableName = j.Dest.Table
	} else if j.SyncType == DBSync {
		destTableName, err = j.destMeta.GetTableNameById(addPartition.TableId)
		if err != nil {
			return err
		}
	}

	// addPartitionSql = "ALTER TABLE " + sql
	addPartitionSql := fmt.Sprintf("ALTER TABLE %s.%s %s", destDbName, destTableName, addPartition.Sql)
	log.Tracef("addPartitionSql: %s", addPartitionSql)
	return j.Dest.Exec(addPartitionSql)
}

// handleDropPartition
func (j *Job) handleDropPartition(binlog *festruct.TBinlog) error {
	log.Tracef("handle drop partition binlog")

	data := binlog.GetData()
	dropPartition, err := record.NewDropPartitionFromJson(data)
	if err != nil {
		return err
	}

	destDbName := j.Dest.Database
	var destTableName string
	if j.SyncType == TableSync {
		destTableName = j.Dest.Table
	} else if j.SyncType == DBSync {
		destTableName, err = j.destMeta.GetTableNameById(dropPartition.TableId)
		if err != nil {
			return err
		}
	}

	// dropPartitionSql = "ALTER TABLE " + sql
	dropPartitionSql := fmt.Sprintf("ALTER TABLE %s.%s %s", destDbName, destTableName, dropPartition.Sql)
	log.Tracef("dropPartitionSql: %s", dropPartitionSql)
	return j.Dest.Exec(dropPartitionSql)
}

// handleCreateTable
func (j *Job) handleCreateTable(binlog *festruct.TBinlog) error {
	log.Tracef("handle create table binlog")

	if j.SyncType != DBSync {
		return errors.Errorf("invalid sync type: %v", j.SyncType)
	}

	data := binlog.GetData()
	createTable, err := record.NewCreateTableFromJson(data)
	if err != nil {
		return err
	}

	sql := createTable.Sql
	log.Tracef("createTableSql: %s", sql)
	// HACK: for drop table
	err = j.Dest.DbExec(sql)
	j.srcMeta.GetTables()
	j.destMeta.GetTables()
	return err
}

// handleDropTable
func (j *Job) handleDropTable(binlog *festruct.TBinlog) error {
	log.Tracef("handle drop table binlog")

	if j.SyncType != DBSync {
		return errors.Errorf("invalid sync type: %v", j.SyncType)
	}

	data := binlog.GetData()
	dropTable, err := record.NewDropTableFromJson(data)
	if err != nil {
		return err
	}

	tableName := dropTable.TableName
	if tableName == "" {
		dirtySrcTables := j.srcMeta.DirtyGetTables()
		srcTable, ok := dirtySrcTables[dropTable.TableId]
		if !ok {
			return errors.Errorf("table not found, tableId: %d", dropTable.TableId)
		}

		tableName = srcTable.Name
	}

	sql := fmt.Sprintf("DROP TABLE %s FORCE", tableName)
	log.Tracef("dropTableSql: %s", sql)
	err = j.Dest.DbExec(sql)
	j.srcMeta.GetTables()
	j.destMeta.GetTables()
	return err
}

// handleAlterJob
func (j *Job) handleAlterJob(binlog *festruct.TBinlog) error {
	log.Tracef("handle alter job binlog")

	data := binlog.GetData()
	alterJob, err := record.NewAlterJobV2FromJson(data)
	if err != nil {
		return err
	}
	if alterJob.TableName == "" {
		return errors.Errorf("invalid alter job, tableName: %s", alterJob.TableName)
	}
	if !alterJob.IsFinished() {
		return nil
	}

	// HACK: busy loop for success
	// TODO: Add to state machine
	for {
		// drop table dropTableSql
		// TODO: [IMPROVEMENT] use rename table instead of drop table
		var dropTableSql string
		if j.SyncType == TableSync {
			dropTableSql = fmt.Sprintf("DROP TABLE %s FORCE", j.Dest.Table)
		} else {
			dropTableSql = fmt.Sprintf("DROP TABLE %s FORCE", alterJob.TableName)
		}
		log.Tracef("dropTableSql: %s", dropTableSql)

		if err := j.destMeta.DbExec(dropTableSql); err == nil {
			break
		}
	}

	switch j.SyncType {
	case TableSync:
		j.progress.NextWithPersist(j.progress.CommitSeq, TableFullSync, BeginCreateSnapshot, "")
	case DBSync:
		j.progress.NextWithPersist(j.progress.CommitSeq, DBFullSync, BeginCreateSnapshot, "")
	default:
		return errors.Errorf("unknown table sync type: %v", j.SyncType)
	}

	return nil
}

// handleLightningSchemaChange
func (j *Job) handleLightningSchemaChange(binlog *festruct.TBinlog) error {
	log.Tracef("handle lightning schema change binlog")

	data := binlog.GetData()
	lightningSchemaChange, err := record.NewModifyTableAddOrDropColumnsFromJson(data)
	if err != nil {
		return err
	}

	rawSql := lightningSchemaChange.RawSql
	//   "rawSql": "ALTER TABLE `default_cluster:ccr`.`test_ddl` ADD COLUMN `nid1` int(11) NULL COMMENT \"\""
	// replace `default_cluster:${Src.Database}`.`test_ddl` to `test_ddl`
	sql := strings.Replace(rawSql, fmt.Sprintf("`default_cluster:%s`.", j.Src.Database), "", 1)
	log.Tracef("lightningSchemaChangeSql, rawSql: %s, sql: %s", rawSql, sql)
	return j.Dest.DbExec(sql)
}

func (j *Job) handleBinlog(binlog *festruct.TBinlog) error {
	if binlog == nil || !binlog.IsSetCommitSeq() {
		return errors.Errorf("invalid binlog: %v", binlog)
	}

	// Step 2: update job progress
	j.progress.StartHandle(binlog.GetCommitSeq())

	// TODO: use table driven
	switch binlog.GetType() {
	case festruct.TBinlogType_UPSERT:
		if err := j.handleUpsert(binlog); err != nil {
			return err
		}
	case festruct.TBinlogType_ADD_PARTITION:
		if err := j.handleAddPartition(binlog); err != nil {
			return err
		}
	case festruct.TBinlogType_DROP_PARTITION:
		if err := j.handleDropPartition(binlog); err != nil {
			return err
		}
	case festruct.TBinlogType_CREATE_TABLE:
		if err := j.handleCreateTable(binlog); err != nil {
			return err
		}
	case festruct.TBinlogType_DROP_TABLE:
		if err := j.handleDropTable(binlog); err != nil {
			return err
		}
	case festruct.TBinlogType_ALTER_JOB:
		if err := j.handleAlterJob(binlog); err != nil {
			return err
		}
	case festruct.TBinlogType_MODIFY_TABLE_ADD_OR_DROP_COLUMNS:
		if err := j.handleLightningSchemaChange(binlog); err != nil {
			return err
		}
	default:
		return errors.Errorf("unknown binlog type: %v", binlog.GetType())
	}

	return nil
}

func (j *Job) incrementalSync() error {
	src := &j.Src

	// Step 1: get binlog
	srcRpc, err := rpc.NewThriftRpc(src)
	if err != nil {
		return nil
	}

	for {
		commitSeq := j.progress.CommitSeq
		log.Tracef("src: %s, CommitSeq: %v", src, commitSeq)

		getBinlogResp, err := srcRpc.GetBinlog(src, commitSeq)
		if err != nil {
			return nil
		}
		log.Tracef("resp: %v", getBinlogResp)

		// Step 2: check binlog status
		status := getBinlogResp.GetStatus()
		switch status.StatusCode {
		case tstatus.TStatusCode_OK:
		case tstatus.TStatusCode_BINLOG_TOO_OLD_COMMIT_SEQ:
		case tstatus.TStatusCode_BINLOG_TOO_NEW_COMMIT_SEQ:
			return nil
		case tstatus.TStatusCode_BINLOG_DISABLE:
			return errors.Errorf("binlog is disabled")
		case tstatus.TStatusCode_BINLOG_NOT_FOUND_DB:
			return errors.Errorf("can't found db")
		case tstatus.TStatusCode_BINLOG_NOT_FOUND_TABLE:
			return errors.Errorf("can't found table")
		default:
			return errors.Errorf("invalid binlog status type: %v", status.StatusCode)
		}

		// Step 3: handle binlog records if has job
		binlogs := getBinlogResp.GetBinlogs()
		if len(binlogs) == 0 {
			return errors.Errorf("no binlog, but status code is: %v", status.StatusCode)
		}

		for _, binlog := range binlogs {
			// Step 3.1: handle binlog
			if err := j.handleBinlog(binlog); err != nil {
				return err
			}

			commitSeq := binlog.GetCommitSeq()
			if j.SyncType == DBSync && j.progress.TableCommitSeqMap != nil {
				// TODO: [PERFORMANCE] use largest tableCommitSeq in memorydata to acc it
				// when all table commit seq > commitSeq, it's true
				reachSwitchToDBIncrementalSync := true
				for _, tableCommitSeq := range j.progress.TableCommitSeqMap {
					if tableCommitSeq > commitSeq {
						reachSwitchToDBIncrementalSync = false
						break
					}
				}

				if reachSwitchToDBIncrementalSync {
					j.progress.TableCommitSeqMap = nil
					j.progress.NextWithPersist(j.progress.CommitSeq, DBIncrementalSync, DB_1, "")
				}
			}
			// Step 3.2: update progress to db
			j.progress.Done()
		}
	}
}

func (j *Job) recoverJobProgress() error {
	// parse progress
	if progress, err := NewJobProgressFromJson(j.Name, j.db); err != nil {
		log.Errorf("parse job progress failed, job: %s, err: %+v", j.Name, err)
		return err
	} else {
		j.progress = progress
		return nil
	}
}

// tableSync is a function that synchronizes a table between the source and destination databases.
// If it is the first synchronization, it performs a full sync of the table.
// If it is not the first synchronization, it recovers the job progress and performs an incremental sync.
func (j *Job) tableSync() error {
	switch j.progress.SyncState {
	case TableFullSync:
		log.Tracef("table full sync")
		return j.fullSync()
	case TableIncrementalSync:
		log.Tracef("table incremental sync")
		return j.incrementalSync()
	default:
		return errors.Errorf("unknown sync state: %v", j.progress.SyncState)
	}
}

// TODO(Drogon): impl
func (j *Job) dbTablesIncrementalSync() error {
	log.Tracef("db tables incremental sync")

	return j.incrementalSync()
}

// TODO(Drogon): impl DBSpecificTableFullSync
func (j *Job) dbSpecificTableFullSync() error {
	log.Tracef("db specific table full sync")

	return nil
}

func (j *Job) dbSync() error {
	switch j.progress.SyncState {
	case DBFullSync:
		log.Tracef("db full sync")
		return j.fullSync()
	case DBTablesIncrementalSync:
		return j.dbTablesIncrementalSync()
	case DBSpecificTableFullSync:
		return j.dbSpecificTableFullSync()
	case DBIncrementalSync:
		log.Tracef("db incremental sync")
		return j.incrementalSync()
	default:
		return errors.Errorf("unknown db sync state: %v", j.progress.SyncState)
	}
}

func (j *Job) sync() error {
	j.lock.Lock()
	defer j.lock.Unlock()

	switch j.SyncType {
	case TableSync:
		return j.tableSync()
	case DBSync:
		return j.dbSync()
	default:
		return errors.Errorf("unknown table sync type: %v", j.SyncType)
	}
}

func (j *Job) run() error {
	ticker := time.NewTicker(SYNC_DURATION)
	defer ticker.Stop()

	for {
		select {
		case <-j.stop:
			return nil
		case <-ticker.C:
			if err := j.sync(); err != nil {
				log.Errorf("job sync failed, job: %s, err: %+v", j.Name, err)
			}
		}
	}
}

// run job
func (j *Job) Run() error {
	// retry 3 times to check IsProgressExist
	var isProgressExist bool
	var err error
	for i := 0; i < 3; i++ {
		isProgressExist, err = j.db.IsProgressExist(j.Name)
		if err != nil {
			log.Errorf("check progress exist failed, error: %+v", err)
			continue
		}
		break
	}
	if err != nil {
		return err
	}

	if isProgressExist {
		if err := j.recoverJobProgress(); err != nil {
			log.Fatalf("recover job %s progress failed: %+v", j.Name, err)
			return err
		}
	} else {
		j.progress = NewJobProgress(j.Name, j.SyncType, j.db)
		switch j.SyncType {
		case TableSync:
			j.progress.NextWithPersist(0, TableFullSync, BeginCreateSnapshot, "")
		case DBSync:
			j.progress.NextWithPersist(0, DBFullSync, BeginCreateSnapshot, "")
		default:
			return errors.Errorf("unknown table sync type: %v", j.SyncType)
		}
	}

	// Hack: for drop table
	if j.SyncType == DBSync {
		j.srcMeta.GetTables()
		j.destMeta.GetTables()
	}

	return j.run()
}

// stop job
func (j *Job) Stop() error {
	close(j.stop)
	return nil
}

func (j *Job) FirstRun() error {
	log.Info("first run check job", zap.String("src", j.Src.String()), zap.String("dest", j.Dest.String()))

	// Step 1: check fe and be binlog feature is enabled
	if err := j.srcMeta.CheckBinlogFeature(); err != nil {
		return err
	}
	if err := j.destMeta.CheckBinlogFeature(); err != nil {
		return err
	}

	// Step 2: check src database
	if src_db_exists, err := j.Src.CheckDatabaseExists(); err != nil {
		return err
	} else if !src_db_exists {
		return errors.Errorf("src database %s not exists", j.Src.Database)
	}
	if j.SyncType == DBSync {
		if enable, err := j.Src.IsDatabaseEnableBinlog(); err != nil {
			return err
		} else if !enable {
			return errors.Errorf("src database %s not enable binlog", j.Src.Database)
		}
	}
	if srcDbId, err := j.srcMeta.GetDbId(); err != nil {
		return err
	} else {
		j.Src.DbId = srcDbId
	}

	// Step 3: check src table exists, if not exists, return err
	if j.SyncType == TableSync {
		if src_table_exists, err := j.Src.CheckTableExists(); err != nil {
			return err
		} else if !src_table_exists {
			return errors.Errorf("src table %s.%s not exists", j.Src.Database, j.Src.Table)
		}

		if enable, err := j.Src.IsTableEnableBinlog(); err != nil {
			return err
		} else if !enable {
			return errors.Errorf("src table %s.%s not enable binlog", j.Src.Database, j.Src.Table)
		}

		if srcTableId, err := j.srcMeta.GetTableId(j.Src.Table); err != nil {
			return err
		} else {
			j.Src.TableId = srcTableId
		}
	}

	// Step 4: check dest database && table exists
	// if dest database && table exists, return err
	dest_db_exists, err := j.Dest.CheckDatabaseExists()
	if err != nil {
		return err
	}
	if !dest_db_exists {
		if err := j.Dest.CreateDatabase(); err != nil {
			return err
		}
	}
	if destDbId, err := j.destMeta.GetDbId(); err != nil {
		return err
	} else {
		j.Dest.DbId = destDbId
	}
	if j.SyncType == TableSync {
		dest_table_exists, err := j.Dest.CheckTableExists()
		if err != nil {
			return err
		}
		if dest_table_exists {
			return errors.Errorf("dest table %s.%s already exists", j.Dest.Database, j.Dest.Table)
		}
	}

	return nil
}

// HACK: temp impl
func (j *Job) GetLag() (int64, error) {
	j.lock.Lock()
	defer j.lock.Unlock()

	srcSpec := &j.Src
	rpc, err := rpc.NewThriftRpc(srcSpec)
	if err != nil {
		return 0, err
	}

	commitSeq := j.progress.CommitSeq
	resp, err := rpc.GetBinlogLag(srcSpec, commitSeq)
	if err != nil {
		return 0, err
	}

	log.Tracef("resp: %v, lag: %d", resp, resp.GetLag())
	return resp.GetLag(), nil
}
