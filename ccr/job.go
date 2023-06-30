package ccr

// TODO: rewrite by state machine, such as first sync, full/incremental sync

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/selectdb/ccr_syncer/ccr/base"
	"github.com/selectdb/ccr_syncer/ccr/record"
	"github.com/selectdb/ccr_syncer/rpc"
	"github.com/selectdb/ccr_syncer/storage"
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
	SyncType    SyncType      `json:"sync_type"`
	Name        string        `json:"name"`
	Src         base.Spec     `json:"src"`
	srcMeta     *Meta         `json:"-"`
	Dest        base.Spec     `json:"dest"`
	destMeta    *Meta         `json:"-"`
	isFirstSync bool          `json:"-"`
	progress    *JobProgress  `json:"-"`
	db          storage.DB    `json:"-"`
	stop        chan struct{} `json:"-"`
}

// new job
func NewJobFromService(name string, src, dest base.Spec, db storage.DB) (*Job, error) {
	job := &Job{
		Name:        name,
		Src:         src,
		srcMeta:     NewMeta(&src),
		Dest:        dest,
		destMeta:    NewMeta(&dest),
		isFirstSync: true,
		progress:    NewJobProgress(name, db),
		db:          db,
		stop:        make(chan struct{}),
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
		return nil, err
	}
	job.srcMeta = NewMeta(&job.Src)
	job.destMeta = NewMeta(&job.Dest)

	// retry 3 times to check IsProgressExist
	var isProgressExist bool
	for i := 0; i < 3; i++ {
		isProgressExist, err = db.IsProgressExist(job.Name)
		if err != nil {
			log.Error("check progress exist failed", zap.Error(err))
			continue
		}
		break
	}

	if err != nil {
		return nil, err
	}

	if isProgressExist {
		job.isFirstSync = false
	} else {
		job.isFirstSync = true
		job.progress = NewJobProgress(job.Name, db)
	}
	job.db = db
	job.stop = make(chan struct{})
	return &job, nil
}

func (j *Job) valid() error {
	var err error
	if exist, err := j.db.IsJobExist(j.Name); err != nil {
		return errors.Wrap(err, "check job exist failed")
	} else if exist {
		return fmt.Errorf("job %s already exist", j.Name)
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
		log.Errorf("get master token failed: %v", err)
		return nil, err
	}

	backends, err := meta.GetBackends()
	if err != nil {
		log.Errorf("get backends failed: %v", err)
		return nil, err
	} else {
		log.Infof("found backends: %v", backends)
	}

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
	// TODO: snapshot machine, not need create snapshot each time
	// Step 1: Create snapshot
	// TODO(Drogon): check last snapshot commitSeq > first commitSeq, maybe we can reuse this snapshot
	log.Debugf("begin create snapshot")
	j.progress.BeginCreateSnapshot()
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
	j.progress.DoneCreateSnapshot(snapshotName)

	// Step 2: Get snapshot info
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

	jobInfo := snapshotResp.GetJobInfo()
	tableCommitSeqMap, err := ExtractTableCommitSeqMap(jobInfo)
	if err != nil {
		log.Errorf("extract table commit seq map failed, err: %v", err)
		return err
	}
	if j.SyncType == TableSync {
		if _, ok := tableCommitSeqMap[j.Src.TableId]; !ok {
			return errors.New("table commit seq not found")
		}
	}

	// Step 3: Add extra info
	var jobInfoMap map[string]interface{}
	err = json.Unmarshal(jobInfo, &jobInfoMap)
	if err != nil {
		log.Errorf("unmarshal jobInfo failed, err: %v", err)
		return err
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
		log.Errorf("marshal jobInfo failed, err: %v", err)
		return err
	}
	log.Infof("jobInfoBytes: %s", string(jobInfoBytes))
	snapshotResp.SetJobInfo(jobInfoBytes)

	// Step 4: start a new fullsync && persist
	switch j.SyncType {
	case DBSync:
		j.progress.BeginDbRestore(tableCommitSeqMap)
	case TableSync:
		commitSeq := tableCommitSeqMap[j.Src.TableId]
		j.progress.BeginTableRestore(commitSeq)
	}

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
		log.Errorf("restore snapshot failed, err: %v", err)
		return err
	}
	log.Infof("resp: %v", restoreResp)
	// TODO: impl wait for done, use show restore
	restoreFinished, err := j.Dest.CheckRestoreFinished(snapshotName)
	if err != nil {
		log.Errorf("check restore state failed, err: %v", err)
		return err
	}
	if !restoreFinished {
		err = fmt.Errorf("check restore state timeout, max try times: %d", base.MAX_CHECK_RETRY_TIMES)
		return err
	}

	// Step 6: Update job progress && dest table id
	// update job info, only for dest table id
	// TODO: retry && mark it for not start a new full sync
	if j.SyncType == DBSync {
		if destTableId, err := j.destMeta.GetTableId(j.Dest.Table); err != nil {
			return err
		} else {
			j.Dest.TableId = destTableId
		}
	}
	j.progress.Done()
	// TODO: reload check job table id
	data, err := json.Marshal(j)
	if err != nil {
		return err
	}
	if err := j.db.UpdateJob(j.Name, string(data)); err != nil {
		return err
	}

	j.isFirstSync = false
	return nil
}

// TODO: check snapshot state
func (j *Job) tableFullSync() error {
	log.Tracef("begin table full sync")

	return j.fullSync()
}

func (j *Job) newLabel(commitSeq int64) string {
	src := &j.Src
	dest := &j.Dest
	if j.SyncType == DBSync {
		// label "ccr_sync_job:${sync_type}:${src_db_id}:${dest_db_id}:${commit_seq}"
		return fmt.Sprintf("ccr_sync_job:%s:%d:%d:%d", j.SyncType, src.DbId, dest.DbId, commitSeq)
	} else {
		// TableSync
		// label "ccr_sync_job:${sync_type}:${src_db_id}_${src_table_id}:${dest_db_id}_${dest_table_id}:${commit_seq}"
		return fmt.Sprintf("ccr_sync_job:%s:%d_%d:%d_%d:%d", j.SyncType, src.DbId, src.TableId, dest.DbId, dest.TableId, commitSeq)
	}
}

// Table ingestBinlog
// TODO: add check success, check ingestBinlog commitInfo
func (j *Job) ingestBinlog(txnId int64, upsert *record.Upsert) ([]*ttypes.TTabletCommitInfo, error) {
	log.Tracef("ingestBinlog, txnId: %d, upsert: %v", txnId, upsert)

	tableRecords := make([]*record.TableRecord, 0, len(upsert.TableRecords))
	switch j.SyncType {
	case DBSync:
		for _, tableRecord := range upsert.TableRecords {
			tableRecords = append(tableRecords, &tableRecord)
		}
	case TableSync:
		tableRecord, ok := upsert.TableRecords[j.Src.TableId]
		if !ok {
			return nil, fmt.Errorf("table record not found, table: %s", j.Src.Table)
		}
		tableRecords = append(tableRecords, &tableRecord)
	default:
		return nil, fmt.Errorf("invalid sync type: %s", j.SyncType)
	}

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
		log.Infof("tableRecord: %v", tableRecord) // TODO: remove it
		for _, partitionRecord := range tableRecord.PartitionRecords {
			log.Infof("partitionRecord: %v", partitionRecord) // TODO: remove it
			binlogVersion := partitionRecord.Version

			srcPartitionId := partitionRecord.PartitionID
			var srcPartitionName string
			srcPartitionName, err = j.srcMeta.GetPartitionName(j.Src.Table, srcPartitionId)
			if err != nil {
				updateLastError(err)
				break
			}
			var destPartitionId int64
			destPartitionId, err = j.destMeta.GetPartitionIdByName(j.Dest.Table, srcPartitionName)
			if err != nil {
				updateLastError(err)
				break
			}

			var srcTablets []*TabletMeta
			srcTablets, err = j.srcMeta.GetTabletList(j.Src.Table, srcPartitionId)
			if err != nil {
				updateLastError(err)
				break
			}
			var destTablets []*TabletMeta
			destTablets, err = j.destMeta.GetTabletList(j.Dest.Table, destPartitionId)
			if err != nil {
				updateLastError(err)
				break
			}
			if len(srcTablets) != len(destTablets) {
				updateLastError(fmt.Errorf("tablet count not match, src: %d, dest: %d", len(srcTablets), len(destTablets)))
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
						lastErr = fmt.Errorf("backend not found, backend id: %d", destReplica.BackendId)
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
						updateLastError(fmt.Errorf("src replicas is empty"))
						return false
					}
					srcBackendId := iter.Value().BackendId
					var srcBackend *base.Backend
					srcBackend, ok = srcBackendMap[srcBackendId]
					if !ok {
						updateLastError(fmt.Errorf("backend not found, backend id: %d", srcBackendId))
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
						}
						log.Debugf("ingest resp: %v", resp)
						if !resp.IsSetStatus() {
							err = fmt.Errorf("ingest resp status not set")
							updateLastError(err)
						} else if resp.Status.StatusCode != tstatus.TStatusCode_OK {
							err = fmt.Errorf("ingest resp status code: %v, msg: %v", resp.Status.StatusCode, resp.Status.ErrorMsgs)
							updateLastError(err)
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

// TODO: deal error by abort txn
func (j *Job) dealUpsertBinlog(binlog *festruct.TBinlog) error {
	log.Tracef("deal upsert binlog")

	data := binlog.GetData()
	upsert, err := record.NewUpsertFromJson(data)
	if err != nil {
		return err
	}

	dest := &j.Dest
	commitSeq := upsert.CommitSeq

	// Step 1: begin txn
	log.Tracef("begin txn, dest: %v, commitSeq: %d", dest, commitSeq)
	destRpc, err := rpc.NewThriftRpc(dest)
	if err != nil {
		panic(err)
	}

	label := j.newLabel(commitSeq)

	beginTxnResp, err := destRpc.BeginTransaction(dest, label)
	if err != nil {
		panic(err)
	}
	log.Infof("resp: %v", beginTxnResp)
	txnId := beginTxnResp.GetTxnId()
	log.Infof("TxnId: %d, DbId: %d", txnId, beginTxnResp.GetDbId())

	// Step 2: update job progress
	j.progress.BeginTransaction(txnId)

	// Step 3: ingest binlog
	var commitInfos []*ttypes.TTabletCommitInfo
	commitInfos, err = j.ingestBinlog(txnId, upsert)
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
	return nil
}

// dealAddPartitionBinlog
func (j *Job) dealAddPartitionBinlog(binlog *festruct.TBinlog) error {
	log.Tracef("deal add partition binlog")

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
	return j.destMeta.Exec(addPartitionSql)
}

// dealDropPartitionBinlog
func (j *Job) dealDropPartitionBinlog(binlog *festruct.TBinlog) error {
	log.Tracef("deal drop partition binlog")

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
	return j.destMeta.Exec(dropPartitionSql)
}

// dealCreateTableBinlog
func (j *Job) dealCreateTableBinlog(binlog *festruct.TBinlog) error {
	log.Tracef("deal create table binlog")

	data := binlog.GetData()
	createTable, err := record.NewCreateTableFromJson(data)
	if err != nil {
		return err
	}

	if j.SyncType != DBSync {
		return fmt.Errorf("invalid sync type: %v", j.SyncType)
	}

	sql := createTable.Sql
	log.Tracef("createTableSql: %s", sql)
	return j.destMeta.Exec(sql)
}

func (j *Job) dealBinlog(binlog *festruct.TBinlog) error {
	if binlog == nil || !binlog.IsSetCommitSeq() {
		return fmt.Errorf("invalid binlog: %v", binlog)
	}

	// Step 2: update job progress
	j.progress.StartDeal(binlog.GetCommitSeq())

	// TODO: use table driven
	switch binlog.GetType() {
	case festruct.TBinlogType_UPSERT:
		if err := j.dealUpsertBinlog(binlog); err != nil {
			return err
		}
	case festruct.TBinlogType_ADD_PARTITION:
		if err := j.dealAddPartitionBinlog(binlog); err != nil {
			return err
		}
	case festruct.TBinlogType_DROP_PARTITION:
		if err := j.dealDropPartitionBinlog(binlog); err != nil {
			return err
		}
	case festruct.TBinlogType_CREATE_TABLE:
		if err := j.dealCreateTableBinlog(binlog); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown binlog type: %v", binlog.GetType())
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
		log.Tracef("src: %v, CommitSeq: %v", src, commitSeq)

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
			return fmt.Errorf("binlog is disabled")
		case tstatus.TStatusCode_BINLOG_NOT_FOUND_DB:
			return fmt.Errorf("can't found db")
		case tstatus.TStatusCode_BINLOG_NOT_FOUND_TABLE:
			return fmt.Errorf("can't found table")
		default:
			return fmt.Errorf("invalid binlog status type: %v", status.StatusCode)
		}

		// Step 3: deal binlog records if has job
		binlogs := getBinlogResp.GetBinlogs()
		if len(binlogs) == 0 {
			return fmt.Errorf("no binlog, but status code is: %v", status.StatusCode)
		}

		for _, binlog := range binlogs {
			if err := j.dealBinlog(binlog); err != nil {
				return err
			}

			// Step 4: update progress to db
			j.progress.Done()
		}
	}
}

func (j *Job) recoverJobProgress() error {
	// parse progress
	if progress, err := NewJobProgressFromJson(j.Name, j.db); err != nil {
		log.Error("parse job progress failed", zap.String("job", j.Name), zap.Error(err))
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
	if j.isFirstSync {
		return j.tableFullSync()
	}

	/// Continue sync
	if j.progress == nil {
		err := j.recoverJobProgress()
		if err != nil {
			return err
		}
	}
	return j.incrementalSync()
}

func (j *Job) dbFullSync() error {
	log.Tracef("begin db full sync")

	return j.fullSync()
}

func (j *Job) dbSync() error {
	if j.isFirstSync {
		return j.dbFullSync()
	}

	/// Continue sync
	if j.progress == nil {
		err := j.recoverJobProgress()
		if err != nil {
			return err
		}
	}
	return j.incrementalSync()
}

func (j *Job) Sync() error {
	switch j.SyncType {
	case TableSync:
		return j.tableSync()
	case DBSync:
		return j.dbSync()
	default:
		return fmt.Errorf("unknown sync type: %v", j.SyncType)
	}
}

// run job
func (j *Job) Run() error {
	// 5s check once
	ticker := time.NewTicker(SYNC_DURATION)
	defer ticker.Stop()

	for {
		select {
		case <-j.stop:
			return nil
		case <-ticker.C:
			if err := j.Sync(); err != nil {
				log.Error("job sync failed", zap.String("job", j.Name), zap.Error(err))
			}
		}
	}
}

// stop job
func (j *Job) Stop() error {
	close(j.stop)
	return nil
}

func (j *Job) FirstRun() error {
	log.Info("first run check job", zap.String("src", j.Src.String()), zap.String("dest", j.Dest.String()))

	// Step 1: check src database
	if src_db_exists, err := j.Src.CheckDatabaseExists(); err != nil {
		return err
	} else if !src_db_exists {
		return fmt.Errorf("src database %s not exists", j.Src.Database)
	}
	if j.SyncType == DBSync {
		if enable, err := j.Src.IsDatabaseEnableBinlog(); err != nil {
			return err
		} else if !enable {
			return fmt.Errorf("src database %s not enable binlog", j.Src.Database)
		}
	}
	if srcDbId, err := j.srcMeta.GetDbId(); err != nil {
		return err
	} else {
		j.Src.DbId = srcDbId
	}

	// Step 2: check src table exists, if not exists, return err
	if j.SyncType == TableSync {
		if src_table_exists, err := j.Src.CheckTableExists(); err != nil {
			return err
		} else if !src_table_exists {
			return fmt.Errorf("src table %s.%s not exists", j.Src.Database, j.Src.Table)
		}

		if enable, err := j.Src.IsTableEnableBinlog(); err != nil {
			return err
		} else if !enable {
			return fmt.Errorf("src table %s.%s not enable binlog", j.Src.Database, j.Src.Table)
		}

		if srcTableId, err := j.srcMeta.GetTableId(j.Src.Table); err != nil {
			return err
		} else {
			j.Src.TableId = srcTableId
		}
	}

	// Step 3: check dest database && table exists
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
			return fmt.Errorf("dest table %s.%s already exists", j.Dest.Database, j.Dest.Table)
		}
	}

	return nil
}
