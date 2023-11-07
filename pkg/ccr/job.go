package ccr

// TODO: rewrite by state machine, such as first sync, full/incremental sync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/selectdb/ccr_syncer/pkg/ccr/base"
	"github.com/selectdb/ccr_syncer/pkg/ccr/record"
	"github.com/selectdb/ccr_syncer/pkg/rpc"
	"github.com/selectdb/ccr_syncer/pkg/storage"
	utils "github.com/selectdb/ccr_syncer/pkg/utils"
	"github.com/selectdb/ccr_syncer/pkg/xerror"

	festruct "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/frontendservice"
	tstatus "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/status"
	ttypes "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/types"

	_ "github.com/go-sql-driver/mysql"
	"github.com/modern-go/gls"
	log "github.com/sirupsen/logrus"
	"go.uber.org/zap"
)

const (
	SYNC_DURATION = time.Second * 3
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

type JobState int

const (
	JobRunning JobState = 0
	JobPaused  JobState = 1
)

// JobState Stringer
func (j JobState) String() string {
	switch j {
	case JobRunning:
		return "running"
	case JobPaused:
		return "paused"
	default:
		return "unknown"
	}
}

// TODO: refactor merge Src && Isrc, Dest && IDest
type Job struct {
	SyncType          SyncType        `json:"sync_type"`
	Name              string          `json:"name"`
	Src               base.Spec       `json:"src"`
	ISrc              base.Specer     `json:"-"`
	srcMeta           Metaer          `json:"-"`
	Dest              base.Spec       `json:"dest"`
	IDest             base.Specer     `json:"-"`
	destMeta          Metaer          `json:"-"`
	State             JobState        `json:"state"`
	destSrcTableIdMap map[int64]int64 `json:"-"`
	progress          *JobProgress    `json:"-"`
	db                storage.DB      `json:"-"`
	jobFactory        *JobFactory     `json:"-"`
	rpcFactory        rpc.IRpcFactory `json:"-"`
	stop              chan struct{}   `json:"-"`
	lock              sync.Mutex      `json:"-"`
}

type JobContext struct {
	context.Context
	src     base.Spec
	dest    base.Spec
	db      storage.DB
	factory *Factory
}

func NewJobContext(src, dest base.Spec, db storage.DB, factory *Factory) *JobContext {
	return &JobContext{
		Context: context.Background(),
		src:     src,
		dest:    dest,
		db:      db,
		factory: factory,
	}
}

// new job
func NewJobFromService(name string, ctx context.Context) (*Job, error) {
	jobContext, ok := ctx.(*JobContext)
	if !ok {
		return nil, xerror.Errorf(xerror.Normal, "invalid context type: %T", ctx)
	}

	metaFactory := jobContext.factory.MetaFactory
	iSpecFactory := jobContext.factory.ISpecFactory
	src := jobContext.src
	dest := jobContext.dest
	job := &Job{
		Name:              name,
		Src:               src,
		ISrc:              iSpecFactory.NewSpecer(&src),
		srcMeta:           metaFactory.NewMeta(&jobContext.src),
		Dest:              dest,
		IDest:             iSpecFactory.NewSpecer(&dest),
		destMeta:          metaFactory.NewMeta(&jobContext.dest),
		State:             JobRunning,
		destSrcTableIdMap: make(map[int64]int64),
		progress:          nil,
		db:                jobContext.db,
		stop:              make(chan struct{}),
	}

	if err := job.valid(); err != nil {
		return nil, xerror.Wrap(err, xerror.Normal, "job is invalid")
	}

	if job.Src.Table == "" {
		job.SyncType = DBSync
	} else {
		job.SyncType = TableSync
	}

	job.jobFactory = NewJobFactory()
	job.rpcFactory = jobContext.factory.RpcFactory

	return job, nil
}

func NewJobFromJson(jsonData string, db storage.DB, factory *Factory) (*Job, error) {
	var job Job
	err := json.Unmarshal([]byte(jsonData), &job)
	if err != nil {
		return nil, xerror.Wrapf(err, xerror.Normal, "unmarshal json failed, json: %s", jsonData)
	}
	job.ISrc = factory.ISpecFactory.NewSpecer(&job.Src)
	job.IDest = factory.ISpecFactory.NewSpecer(&job.Dest)
	job.srcMeta = factory.MetaFactory.NewMeta(&job.Src)
	job.destMeta = factory.MetaFactory.NewMeta(&job.Dest)
	job.destSrcTableIdMap = make(map[int64]int64)
	job.progress = nil
	job.db = db
	job.stop = make(chan struct{})
	job.jobFactory = NewJobFactory()
	job.rpcFactory = factory.RpcFactory
	return &job, nil
}

func (j *Job) valid() error {
	var err error
	if exist, err := j.db.IsJobExist(j.Name); err != nil {
		return xerror.Wrap(err, xerror.Normal, "check job exist failed")
	} else if exist {
		return xerror.Errorf(xerror.Normal, "job %s already exist", j.Name)
	}

	if j.Name == "" {
		return xerror.New(xerror.Normal, "name is empty")
	}

	err = j.ISrc.Valid()
	if err != nil {
		return xerror.Wrap(err, xerror.Normal, "src spec is invalid")
	}

	err = j.IDest.Valid()
	if err != nil {
		return xerror.Wrap(err, xerror.Normal, "dest spec is invalid")
	}

	if (j.Src.Table == "" && j.Dest.Table != "") || (j.Src.Table != "" && j.Dest.Table == "") {
		return xerror.New(xerror.Normal, "src/dest are not both db or table sync")
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
	err := j.IDest.ClearDB()
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
	masterToken, err := meta.GetMasterToken(j.rpcFactory)
	if err != nil {
		return nil, err
	}

	backends, err := meta.GetBackends()
	if err != nil {
		return nil, err
	}

	log.Debugf("found backends: %v", backends)

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

func (j *Job) isIncrementalSync() bool {
	return j.progress.SyncState == DBIncrementalSync || j.progress.SyncState == TableIncrementalSync
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
	case Done:
		if err := j.newSnapshot(j.progress.CommitSeq); err != nil {
			return err
		}

	case BeginCreateSnapshot:
		// Step 1: Create snapshot
		log.Infof("fullsync status: create snapshot")

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
			return xerror.Errorf(xerror.Normal, "invalid sync type %s", j.SyncType)
		}
		snapshotName, err := j.ISrc.CreateSnapshotAndWaitForDone(backupTableList)
		if err != nil {
			return err
		}

		j.progress.NextSubCheckpoint(GetSnapshotInfo, snapshotName)

	case GetSnapshotInfo:
		// Step 2: Get snapshot info
		log.Infof("fullsync status: get snapshot info")

		snapshotName := j.progress.PersistData
		src := &j.Src
		srcRpc, err := j.rpcFactory.NewFeRpc(src)
		if err != nil {
			return err
		}

		log.Debugf("begin get snapshot %s", snapshotName)
		snapshotResp, err := srcRpc.GetSnapshot(src, snapshotName)
		if err != nil {
			return err
		}

		if snapshotResp.Status.GetStatusCode() != tstatus.TStatusCode_OK {
			log.Errorf("get snapshot failed, status: %v", snapshotResp.Status)
		}

		log.Debugf("job: %s", string(snapshotResp.GetJobInfo()))
		if !snapshotResp.IsSetJobInfo() {
			return xerror.New(xerror.Normal, "jobInfo is not set")
		}

		tableCommitSeqMap, err := ExtractTableCommitSeqMap(snapshotResp.GetJobInfo())
		if err != nil {
			return err
		}

		if j.SyncType == TableSync {
			if _, ok := tableCommitSeqMap[j.Src.TableId]; !ok {
				return xerror.Errorf(xerror.Normal, "tableid %d, commit seq not found", j.Src.TableId)
			}
		}

		inMemoryData := &inMemoryData{
			SnapshotName:      snapshotName,
			SnapshotResp:      snapshotResp,
			TableCommitSeqMap: tableCommitSeqMap,
		}
		j.progress.NextSubVolatile(AddExtraInfo, inMemoryData)

	case AddExtraInfo:
		// Step 3: Add extra info
		log.Infof("fullsync status: add extra info")

		inMemoryData := j.progress.InMemoryData.(*inMemoryData)
		snapshotResp := inMemoryData.SnapshotResp
		jobInfo := snapshotResp.GetJobInfo()
		tableCommitSeqMap := inMemoryData.TableCommitSeqMap

		var jobInfoMap map[string]interface{}
		err := json.Unmarshal(jobInfo, &jobInfoMap)
		if err != nil {
			return xerror.Wrapf(err, xerror.Normal, "unmarshal jobInfo failed, jobInfo: %s", string(jobInfo))
		}
		log.Debugf("jobInfo: %v", jobInfoMap)

		extraInfo, err := j.genExtraInfo()
		if err != nil {
			return err
		}
		log.Debugf("extraInfo: %v", extraInfo)

		jobInfoMap["extra_info"] = extraInfo
		jobInfoBytes, err := json.Marshal(jobInfoMap)
		if err != nil {
			return xerror.Errorf(xerror.Normal, "marshal jobInfo failed, jobInfo: %v", jobInfoMap)
		}
		log.Debugf("jobInfoBytes: %s", string(jobInfoBytes))
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
		// Step 4: Restore snapshot
		log.Infof("fullsync status: restore snapshot")

		if j.progress.InMemoryData == nil {
			persistData := j.progress.PersistData
			inMemoryData := &inMemoryData{}
			if err := json.Unmarshal([]byte(persistData), inMemoryData); err != nil {
				// TODO: return to snapshot
				return xerror.Errorf(xerror.Normal, "unmarshal persistData failed, persistData: %s", persistData)
			}
			j.progress.InMemoryData = inMemoryData
		}

		// Step 4.1: start a new fullsync && persist
		inMemoryData := j.progress.InMemoryData.(*inMemoryData)
		snapshotName := inMemoryData.SnapshotName
		snapshotResp := inMemoryData.SnapshotResp

		// Step 4.2: restore snapshot to dest
		dest := &j.Dest
		destRpc, err := j.rpcFactory.NewFeRpc(dest)
		if err != nil {
			return err
		}
		log.Debugf("begin restore snapshot %s", snapshotName)

		var tableRefs []*festruct.TTableRef
		if j.SyncType == TableSync && j.Src.Table != j.Dest.Table {
			log.Debugf("table sync snapshot not same name, table: %s, dest table: %s", j.Src.Table, j.Dest.Table)
			tableRefs = make([]*festruct.TTableRef, 0)
			tableRef := &festruct.TTableRef{
				Table:     &j.Src.Table,
				AliasName: &j.Dest.Table,
			}
			tableRefs = append(tableRefs, tableRef)
		}
		restoreResp, err := destRpc.RestoreSnapshot(dest, tableRefs, snapshotName, snapshotResp)
		if err != nil {
			return err
		}
		if restoreResp.Status.GetStatusCode() != tstatus.TStatusCode_OK {
			return xerror.Errorf(xerror.Normal, "restore snapshot failed, status: %v", restoreResp.Status)
		}
		log.Infof("resp: %v", restoreResp)

		// TODO: impl wait for done, use show restore
		restoreFinished, err := j.IDest.CheckRestoreFinished(snapshotName)
		if err != nil {
			return err
		}
		if !restoreFinished {
			err = xerror.Errorf(xerror.Normal, "check restore state timeout, max try times: %d", base.MAX_CHECK_RETRY_TIMES)
			return err
		}
		j.progress.NextSubCheckpoint(PersistRestoreInfo, snapshotName)

	case PersistRestoreInfo:
		// Step 5: Update job progress && dest table id
		// update job info, only for dest table id
		log.Infof("fullsync status: persist restore info")

		// TODO: retry && mark it for not start a new full sync
		switch j.SyncType {
		case DBSync:
			tableMapping := make(map[int64]int64)
			for srcTableId, _ := range j.progress.TableCommitSeqMap {
				srcTableName, err := j.srcMeta.GetTableNameById(srcTableId)
				if err != nil {
					return err
				}

				destTableId, err := j.destMeta.GetTableId(srcTableName)
				if err != nil {
					return err
				}

				tableMapping[srcTableId] = destTableId
			}

			j.progress.TableMapping = tableMapping
			j.progress.NextWithPersist(j.progress.CommitSeq, DBTablesIncrementalSync, Done, "")
		case TableSync:
			if destTable, err := j.destMeta.UpdateTable(j.Dest.Table, 0); err != nil {
				return err
			} else {
				j.Dest.TableId = destTable.Id
			}

			// TODO: reload check job table id
			if err := j.persistJob(); err != nil {
				return err
			}

			j.progress.TableCommitSeqMap = nil
			j.progress.TableMapping = nil
			j.progress.NextWithPersist(j.progress.CommitSeq, TableIncrementalSync, Done, "")
		default:
			return xerror.Errorf(xerror.Normal, "invalid sync type %d", j.SyncType)
		}

		return nil
	default:
		return xerror.Errorf(xerror.Normal, "invalid job sub sync state %d", j.progress.SubSyncState)
	}

	return j.fullSync()
}

func (j *Job) persistJob() error {
	data, err := json.Marshal(j)
	if err != nil {
		return xerror.Errorf(xerror.Normal, "marshal job failed, job: %v", j)
	}

	if err := j.db.UpdateJob(j.Name, string(data)); err != nil {
		return err
	}

	return nil
}

// FIXME: label will conflict when commitSeq equal
func (j *Job) newLabel(commitSeq int64) string {
	src := &j.Src
	dest := &j.Dest
	randNum := rand.Intn(65536) // hex 4 chars
	if j.SyncType == DBSync {
		// label "ccrj-rand:${sync_type}:${src_db_id}:${dest_db_id}:${commit_seq}"
		return fmt.Sprintf("ccrj-%x:%s:%d:%d:%d", randNum, j.SyncType, src.DbId, dest.DbId, commitSeq)
	} else {
		// TableSync
		// label "ccrj-rand:${sync_type}:${src_db_id}_${src_table_id}:${dest_db_id}_${dest_table_id}:${commit_seq}"
		return fmt.Sprintf("ccrj-%x:%s:%d_%d:%d_%d:%d", randNum, j.SyncType, src.DbId, src.TableId, dest.DbId, dest.TableId, commitSeq)
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
			return nil, xerror.Errorf(xerror.Normal, "table record not found, table: %s", j.Src.Table)
		}
		tableRecords = make([]*record.TableRecord, 0, 1)
		tableRecords = append(tableRecords, tableRecord)
	default:
		return nil, xerror.Errorf(xerror.Normal, "invalid sync type: %s", j.SyncType)
	}

	return tableRecords, nil
}

// Table ingestBinlog
// TODO: add check success, check ingestBinlog commitInfo
// TODO: rewrite by use tableId
func (j *Job) ingestBinlog(txnId int64, tableRecords []*record.TableRecord) ([]*ttypes.TTabletCommitInfo, error) {
	log.Infof("ingestBinlog, txnId: %d", txnId)

	job, err := j.jobFactory.CreateJob(NewIngestContext(txnId, tableRecords, j.progress.TableMapping), j, "IngestBinlog")
	if err != nil {
		return nil, err
	}

	ingestBinlogJob, ok := job.(*IngestBinlogJob)
	if !ok {
		return nil, xerror.Errorf(xerror.Normal, "invalid job type, job: %+v", job)
	}

	job.Run()
	if err := job.Error(); err != nil {
		return nil, err
	}
	return ingestBinlogJob.CommitInfos(), nil
}

// TODO: handle error by abort txn
func (j *Job) handleUpsert(binlog *festruct.TBinlog) error {
	log.Infof("handle upsert binlog, sub sync state: %s", j.progress.SubSyncState)

	// inMemory will be update in state machine, but progress keep any, so progress.inMemory is also latest, well call NextSubCheckpoint don't need to upate inMemory in progress
	// TODO(IMPROVE): some steps not need all data, so we can reset some data in progress, such as RollbackTransaction only need txnId
	type inMemoryData struct {
		CommitSeq    int64                       `json:"commit_seq"`
		TxnId        int64                       `json:"txn_id"`
		DestTableIds []int64                     `json:"dest_table_ids"`
		TableRecords []*record.TableRecord       `json:"table_records"`
		CommitInfos  []*ttypes.TTabletCommitInfo `json:"commit_infos"`
	}

	upateInMemory := func() error {
		if j.progress.InMemoryData == nil {
			persistData := j.progress.PersistData
			inMemoryData := &inMemoryData{}
			if err := json.Unmarshal([]byte(persistData), inMemoryData); err != nil {
				return xerror.Errorf(xerror.Normal, "unmarshal persistData failed, persistData: %s", persistData)
			}
			j.progress.InMemoryData = inMemoryData
		}
		return nil
	}

	rollback := func(err error, inMemoryData *inMemoryData) {
		log.Errorf("need rollback, err: %+v", err)
		j.progress.NextSubCheckpoint(RollbackTransaction, inMemoryData)
	}

	dest := &j.Dest
	switch j.progress.SubSyncState {
	case Done:
		if binlog == nil {
			log.Errorf("binlog is nil, %+v", xerror.Errorf(xerror.Normal, "handle nil upsert binlog"))
			return nil
		}

		data := binlog.GetData()
		upsert, err := record.NewUpsertFromJson(data)
		if err != nil {
			return err
		}
		log.Debugf("upsert: %v", upsert)

		// TODO(Fix)
		// commitSeq := upsert.CommitSeq

		// Step 1: get related tableRecords
		tableRecords, err := j.getReleatedTableRecords(upsert)
		if err != nil {
			log.Errorf("get releated table records failed, err: %+v", err)
		}
		if len(tableRecords) == 0 {
			log.Debug("no releated table records")
			return nil
		}

		log.Debugf("tableRecords: %v", tableRecords)
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
		inMemoryData := &inMemoryData{
			CommitSeq:    upsert.CommitSeq,
			DestTableIds: destTableIds,
			TableRecords: tableRecords,
		}
		j.progress.NextSubVolatile(BeginTransaction, inMemoryData)

	case BeginTransaction:
		// Step 2: begin txn
		inMemoryData := j.progress.InMemoryData.(*inMemoryData)
		commitSeq := inMemoryData.CommitSeq
		log.Debugf("begin txn, dest: %v, commitSeq: %d", dest, commitSeq)

		destRpc, err := j.rpcFactory.NewFeRpc(dest)
		if err != nil {
			return err
		}

		label := j.newLabel(commitSeq)

		beginTxnResp, err := destRpc.BeginTransaction(dest, label, inMemoryData.DestTableIds)
		if err != nil {
			return err
		}
		log.Debugf("resp: %v", beginTxnResp)
		if beginTxnResp.GetStatus().GetStatusCode() != tstatus.TStatusCode_OK {
			return xerror.Errorf(xerror.Normal, "begin txn failed, status: %v", beginTxnResp.GetStatus())
		}
		txnId := beginTxnResp.GetTxnId()
		log.Debugf("TxnId: %d, DbId: %d", txnId, beginTxnResp.GetDbId())

		inMemoryData.TxnId = txnId
		j.progress.NextSubCheckpoint(IngestBinlog, inMemoryData)

	case IngestBinlog:
		log.Debug("ingest binlog")
		if err := upateInMemory(); err != nil {
			return err
		}
		inMemoryData := j.progress.InMemoryData.(*inMemoryData)
		tableRecords := inMemoryData.TableRecords
		txnId := inMemoryData.TxnId

		// TODO: 反查现在的状况
		// Step 3: ingest binlog
		var commitInfos []*ttypes.TTabletCommitInfo
		commitInfos, err := j.ingestBinlog(txnId, tableRecords)
		if err != nil {
			rollback(err, inMemoryData)
		} else {
			log.Debugf("commitInfos: %v", commitInfos)
			inMemoryData.CommitInfos = commitInfos
			j.progress.NextSubCheckpoint(CommitTransaction, inMemoryData)
		}

	case CommitTransaction:
		// Step 4: commit txn
		log.Debug("commit txn")
		if err := upateInMemory(); err != nil {
			return err
		}
		inMemoryData := j.progress.InMemoryData.(*inMemoryData)
		txnId := inMemoryData.TxnId
		commitInfos := inMemoryData.CommitInfos

		destRpc, err := j.rpcFactory.NewFeRpc(dest)
		if err != nil {
			rollback(err, inMemoryData)
			break
		}

		resp, err := destRpc.CommitTransaction(dest, txnId, commitInfos)
		if err != nil {
			rollback(err, inMemoryData)
			break
		}

		if statusCode := resp.Status.GetStatusCode(); statusCode == tstatus.TStatusCode_PUBLISH_TIMEOUT {
			dest.WaitTransactionDone(txnId)
		} else if statusCode != tstatus.TStatusCode_OK {
			err := xerror.Errorf(xerror.Normal, "commit txn failed, status: %v", resp.Status)
			rollback(err, inMemoryData)
			break
		}

		log.Infof("commit TxnId: %d resp: %v", txnId, resp)
		commitSeq := j.progress.CommitSeq
		destTableIds := inMemoryData.DestTableIds
		if j.SyncType == DBSync && len(j.progress.TableCommitSeqMap) > 0 {
			for _, tableId := range destTableIds {
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
		j.progress.Done()
		return nil

	case RollbackTransaction:
		log.Debugf("Rollback txn")
		// Not Step 5: just rollback txn
		if err := upateInMemory(); err != nil {
			return err
		}

		inMemoryData := j.progress.InMemoryData.(*inMemoryData)
		txnId := inMemoryData.TxnId
		destRpc, err := j.rpcFactory.NewFeRpc(dest)
		if err != nil {
			return err
		}

		resp, err := destRpc.RollbackTransaction(dest, txnId)
		if err != nil {
			return err
		}
		if resp.Status.GetStatusCode() != tstatus.TStatusCode_OK {
			return xerror.Errorf(xerror.Normal, "rollback txn failed, status: %v", resp.Status)
		}
		log.Infof("rollback TxnId: %d resp: %v", txnId, resp)
		j.progress.Done()
		return nil

	default:
		return xerror.Errorf(xerror.Normal, "invalid job sub sync state %d", j.progress.SubSyncState)
	}

	return j.handleUpsert(binlog)
}

// handleAddPartition
func (j *Job) handleAddPartition(binlog *festruct.TBinlog) error {
	log.Infof("handle add partition binlog")

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
	log.Infof("addPartitionSql: %s", addPartitionSql)
	return j.IDest.Exec(addPartitionSql)
}

// handleDropPartition
func (j *Job) handleDropPartition(binlog *festruct.TBinlog) error {
	log.Infof("handle drop partition binlog")

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
	log.Infof("dropPartitionSql: %s", dropPartitionSql)
	return j.IDest.Exec(dropPartitionSql)
}

// handleCreateTable
func (j *Job) handleCreateTable(binlog *festruct.TBinlog) error {
	log.Infof("handle create table binlog")

	if j.SyncType != DBSync {
		return xerror.Errorf(xerror.Normal, "invalid sync type: %v", j.SyncType)
	}

	data := binlog.GetData()
	createTable, err := record.NewCreateTableFromJson(data)
	if err != nil {
		return err
	}

	sql := createTable.Sql
	log.Infof("createTableSql: %s", sql)
	// HACK: for drop table
	err = j.IDest.DbExec(sql)
	j.srcMeta.GetTables()
	j.destMeta.GetTables()
	return err
}

// handleDropTable
func (j *Job) handleDropTable(binlog *festruct.TBinlog) error {
	log.Infof("handle drop table binlog")

	if j.SyncType != DBSync {
		return xerror.Errorf(xerror.Normal, "invalid sync type: %v", j.SyncType)
	}

	data := binlog.GetData()
	dropTable, err := record.NewDropTableFromJson(data)
	if err != nil {
		return err
	}

	tableName := dropTable.TableName
	// depreated
	if tableName == "" {
		dirtySrcTables := j.srcMeta.DirtyGetTables()
		srcTable, ok := dirtySrcTables[dropTable.TableId]
		if !ok {
			return xerror.Errorf(xerror.Normal, "table not found, tableId: %d", dropTable.TableId)
		}

		tableName = srcTable.Name
	}

	sql := fmt.Sprintf("DROP TABLE %s FORCE", tableName)
	log.Infof("dropTableSql: %s", sql)
	err = j.IDest.DbExec(sql)
	j.srcMeta.GetTables()
	j.destMeta.GetTables()
	return err
}

func (j *Job) handleDummy(binlog *festruct.TBinlog) error {
	dummyCommitSeq := binlog.GetCommitSeq()

	log.Infof("handle dummy binlog, need full sync. SyncType: %v, seq: %v", j.SyncType, dummyCommitSeq)

	return j.newSnapshot(dummyCommitSeq)
}

// handleAlterJob
func (j *Job) handleAlterJob(binlog *festruct.TBinlog) error {
	log.Infof("handle alter job binlog")

	data := binlog.GetData()
	alterJob, err := record.NewAlterJobV2FromJson(data)
	if err != nil {
		return err
	}
	if alterJob.TableName == "" {
		return xerror.Errorf(xerror.Normal, "invalid alter job, tableName: %s", alterJob.TableName)
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
		log.Infof("dropTableSql: %s", dropTableSql)

		if err := j.destMeta.DbExec(dropTableSql); err == nil {
			break
		}
	}

	return j.newSnapshot(j.progress.CommitSeq)
}

// handleLightningSchemaChange
func (j *Job) handleLightningSchemaChange(binlog *festruct.TBinlog) error {
	log.Infof("handle lightning schema change binlog")

	data := binlog.GetData()
	lightningSchemaChange, err := record.NewModifyTableAddOrDropColumnsFromJson(data)
	if err != nil {
		return err
	}

	log.Debugf("lightningSchemaChange %v", lightningSchemaChange)

	rawSql := lightningSchemaChange.RawSql
	//   "rawSql": "ALTER TABLE `default_cluster:ccr`.`test_ddl` ADD COLUMN `nid1` int(11) NULL COMMENT \"\""
	// replace `default_cluster:${Src.Database}`.`test_ddl` to `test_ddl`
	sql := strings.Replace(rawSql, fmt.Sprintf("`default_cluster:%s`.", j.Src.Database), "", 1)
	log.Infof("lightningSchemaChangeSql, rawSql: %s, sql: %s", rawSql, sql)
	return j.IDest.DbExec(sql)
}

func (j *Job) handleTruncateTable(binlog *festruct.TBinlog) error {
	log.Infof("handle truncate table binlog")

	data := binlog.GetData()
	truncateTable, err := record.NewTruncateTableFromJson(data)
	if err != nil {
		return err
	}

	var destTableName string
	switch j.SyncType {
	case DBSync:
		destTableName = truncateTable.TableName
	case TableSync:
		destTableName = j.Dest.Table
	default:
		return xerror.Panicf(xerror.Normal, "invalid sync type: %v", j.SyncType)
	}

	var sql string
	if truncateTable.RawSql == "" {
		sql = fmt.Sprintf("TRUNCATE TABLE %s", destTableName)
	} else {
		sql = fmt.Sprintf("TRUNCATE TABLE %s %s", destTableName, truncateTable.RawSql)
	}

	log.Infof("truncateTableSql: %s", sql)

	err = j.IDest.DbExec(sql)
	if err == nil {
		if srcTableName, err := j.srcMeta.GetTableNameById(truncateTable.TableId); err == nil {
			// if err != nil, maybe truncate table had been dropped
			j.srcMeta.ClearTable(j.Src.Database, srcTableName)
		}
		j.destMeta.ClearTable(j.Dest.Database, destTableName)
	}

	return err
}

// return: error && bool backToRunLoop
func (j *Job) handleBinlogs(binlogs []*festruct.TBinlog) (error, bool) {
	for _, binlog := range binlogs {
		// Step 1: dispatch handle binlog
		if err := j.handleBinlog(binlog); err != nil {
			return err, false
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
				j.progress.NextWithPersist(j.progress.CommitSeq, DBIncrementalSync, Done, "")
			}
		}

		// Step 2: update progress to db
		if !j.progress.IsDone() {
			j.progress.Done()
		}

		// Step 3: check job state, if not incrementalSync, break
		if !j.isIncrementalSync() {
			return nil, true
		}
	}
	return nil, false
}

func (j *Job) handleBinlog(binlog *festruct.TBinlog) error {
	if binlog == nil || !binlog.IsSetCommitSeq() {
		return xerror.Errorf(xerror.Normal, "invalid binlog: %v", binlog)
	}

	log.Debugf("binlog data: %s", binlog.GetData())

	// Step 2: update job progress
	j.progress.StartHandle(binlog.GetCommitSeq())

	// TODO: use table driven, keep this and driven, conert BinlogType to TBinlogType
	switch binlog.GetType() {
	case festruct.TBinlogType_UPSERT:
		return j.handleUpsert(binlog)
	case festruct.TBinlogType_ADD_PARTITION:
		return j.handleAddPartition(binlog)
	case festruct.TBinlogType_CREATE_TABLE:
		return j.handleCreateTable(binlog)
	case festruct.TBinlogType_DROP_PARTITION:
		return j.handleDropPartition(binlog)
	case festruct.TBinlogType_DROP_TABLE:
		return j.handleDropTable(binlog)
	case festruct.TBinlogType_ALTER_JOB:
		return j.handleAlterJob(binlog)
	case festruct.TBinlogType_MODIFY_TABLE_ADD_OR_DROP_COLUMNS:
		return j.handleLightningSchemaChange(binlog)
	case festruct.TBinlogType_DUMMY:
		return j.handleDummy(binlog)
	case festruct.TBinlogType_ALTER_DATABASE_PROPERTY:
		// TODO(Drogon)
	case festruct.TBinlogType_MODIFY_TABLE_PROPERTY:
		// TODO(Drogon)
	case festruct.TBinlogType_BARRIER:
		log.Info("handle barrier binlog")
	case festruct.TBinlogType_TRUNCATE_TABLE:
		return j.handleTruncateTable(binlog)
	default:
		return xerror.Errorf(xerror.Normal, "unknown binlog type: %v", binlog.GetType())
	}

	return nil
}

func (j *Job) recoverIncrementalSync() error {
	switch j.progress.SubSyncState.BinlogType {
	case BinlogUpsert:
		return j.handleUpsert(nil)
	default:
		j.progress.Rollback()
	}

	return nil
}

func (j *Job) incrementalSync() error {
	if !j.progress.IsDone() {
		log.Infof("job progress is not done, state is (%s), need recover", j.progress.SubSyncState)

		return j.recoverIncrementalSync()
	}

	// Step 1: get binlog
	log.Debug("start incremental sync")
	src := &j.Src
	srcRpc, err := j.rpcFactory.NewFeRpc(src)
	if err != nil {
		log.Errorf("new fe rpc failed, src: %v, err: %+v", src, err)
		return err
	}

	// Step 2: handle all binlog
	for {
		commitSeq := j.progress.CommitSeq
		log.Debugf("src: %s, commitSeq: %v", src, commitSeq)

		getBinlogResp, err := srcRpc.GetBinlog(src, commitSeq)
		if err != nil {
			return nil
		}
		log.Debugf("resp: %v", getBinlogResp)

		// Step 2.1: check binlog status
		status := getBinlogResp.GetStatus()
		switch status.StatusCode {
		case tstatus.TStatusCode_OK:
		case tstatus.TStatusCode_BINLOG_TOO_OLD_COMMIT_SEQ:
		case tstatus.TStatusCode_BINLOG_TOO_NEW_COMMIT_SEQ:
			return nil
		case tstatus.TStatusCode_BINLOG_DISABLE:
			return xerror.Errorf(xerror.Normal, "binlog is disabled")
		case tstatus.TStatusCode_BINLOG_NOT_FOUND_DB:
			return xerror.Errorf(xerror.Normal, "can't found db")
		case tstatus.TStatusCode_BINLOG_NOT_FOUND_TABLE:
			return xerror.Errorf(xerror.Normal, "can't found table")
		default:
			return xerror.Errorf(xerror.Normal, "invalid binlog status type: %v", status.StatusCode)
		}

		// Step 2.2: handle binlogs records if has job
		binlogs := getBinlogResp.GetBinlogs()
		if len(binlogs) == 0 {
			return xerror.Errorf(xerror.Normal, "no binlog, but status code is: %v", status.StatusCode)
		}

		// Step 2.3: dispatch handle binlogs
		if err, backToRunLoop := j.handleBinlogs(binlogs); err != nil {
			return err
		} else if backToRunLoop {
			return nil
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
		log.Debug("table full sync")
		return j.fullSync()
	case TableIncrementalSync:
		log.Debug("table incremental sync")
		return j.incrementalSync()
	default:
		return xerror.Errorf(xerror.Normal, "unknown sync state: %v", j.progress.SyncState)
	}
}

func (j *Job) dbTablesIncrementalSync() error {
	log.Debug("db tables incremental sync")

	return j.incrementalSync()
}

// TODO(Drogon): impl DBSpecificTableFullSync
func (j *Job) dbSpecificTableFullSync() error {
	log.Debug("db specific table full sync")

	return nil
}

func (j *Job) dbSync() error {
	switch j.progress.SyncState {
	case DBFullSync:
		log.Debug("db full sync")
		return j.fullSync()
	case DBTablesIncrementalSync:
		return j.dbTablesIncrementalSync()
	case DBSpecificTableFullSync:
		return j.dbSpecificTableFullSync()
	case DBIncrementalSync:
		log.Debug("db incremental sync")
		return j.incrementalSync()
	default:
		return xerror.Errorf(xerror.Normal, "unknown db sync state: %v", j.progress.SyncState)
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
		return xerror.Errorf(xerror.Normal, "unknown table sync type: %v", j.SyncType)
	}
}

// if err is Panic, return it
func (j *Job) handleError(err error) error {
	var xerr *xerror.XError
	if !errors.As(err, &xerr) {
		return nil
	}

	if xerr.IsPanic() {
		return err
	}

	// TODO(Drogon): do more things, not only snapshot
	if xerr.Category() == xerror.Meta {
		// TODO(Drogon): handle error
		j.newSnapshot(j.progress.CommitSeq)
	}
	return nil
}

func (j *Job) run() {
	ticker := time.NewTicker(SYNC_DURATION)
	defer ticker.Stop()

	var panicError error

	for {
		select {
		case <-j.stop:
			gls.DeleteGls(gls.GoID())
			log.Infof("job stopped, job: %s", j.Name)
			return
		case <-ticker.C:
			if j.getJobState() != JobRunning {
				break
			}

			// loop to print error, not panic, waiting for user to pause/stop/remove Job
			// TODO(Drogon): Add user resume the job, so reset panicError for retry
			if panicError != nil {
				log.Errorf("job panic, job: %s, err: %+v", j.Name, panicError)
				break
			}

			err := j.sync()
			if err == nil {
				break
			}

			log.Errorf("job sync failed, job: %s, err: %+v", j.Name, err)
			panicError = j.handleError(err)
		}
	}
}

func (j *Job) newSnapshot(commitSeq int64) error {
	log.Infof("new snapshot, commitSeq: %d", commitSeq)

	switch j.SyncType {
	case TableSync:
		j.progress.NextWithPersist(commitSeq, TableFullSync, BeginCreateSnapshot, "")
		return nil
	case DBSync:
		j.progress.NextWithPersist(commitSeq, DBFullSync, BeginCreateSnapshot, "")
		return nil
	default:
		err := xerror.Panicf(xerror.Normal, "unknown table sync type: %v", j.SyncType)
		log.Fatalf("run %+v", err)
		return err
	}
}

// run job
func (j *Job) Run() error {
	gls.ResetGls(gls.GoID(), map[interface{}]interface{}{})
	gls.Set("job", j.Name)

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
			log.Errorf("recover job %s progress failed: %+v", j.Name, err)
			return err
		}
	} else {
		j.progress = NewJobProgress(j.Name, j.SyncType, j.db)
		if err := j.newSnapshot(0); err != nil {
			return err
		}
	}

	// Hack: for drop table
	if j.SyncType == DBSync {
		j.srcMeta.GetTables()
		j.destMeta.GetTables()
	}

	j.run()
	return nil
}

func (j *Job) desyncTable() error {
	log.Debugf("desync table")

	tableName, err := j.destMeta.GetTableNameById(j.Dest.TableId)
	if err != nil {
		return err
	}

	desyncSql := fmt.Sprintf("ALTER TABLE %s SET (\"is_being_synced\"=\"false\")", tableName)
	log.Debugf("db exec: %s", desyncSql)
	if err := j.IDest.DbExec(desyncSql); err != nil {
		return xerror.Wrapf(err, xerror.FE, "failed tables: %s", tableName)
	}
	return nil
}

func (j *Job) desyncDB() error {
	log.Debugf("desync db")

	var failedTable string = ""
	tables, err := j.destMeta.GetTables()
	if err != nil {
		return err
	}

	for _, tableMeta := range tables {
		desyncSql := fmt.Sprintf("ALTER TABLE %s SET (\"is_being_synced\"=\"false\")", tableMeta.Name)
		log.Debugf("db exec: %s", desyncSql)
		if err := j.IDest.DbExec(desyncSql); err != nil {
			failedTable += tableMeta.Name + " "
		}
	}

	if failedTable != "" {
		return xerror.Errorf(xerror.FE, "failed tables: %s", failedTable)
	}

	return nil
}

func (j *Job) Desync() error {
	if j.SyncType == DBSync {
		return j.desyncDB()
	} else {
		return j.desyncTable()
	}
}

// stop job
func (j *Job) Stop() {
	close(j.stop)
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
	if src_db_exists, err := j.ISrc.CheckDatabaseExists(); err != nil {
		return err
	} else if !src_db_exists {
		return xerror.Errorf(xerror.Normal, "src database %s not exists", j.Src.Database)
	}
	if j.SyncType == DBSync {
		if enable, err := j.ISrc.IsDatabaseEnableBinlog(); err != nil {
			return err
		} else if !enable {
			return xerror.Errorf(xerror.Normal, "src database %s not enable binlog", j.Src.Database)
		}
	}
	if srcDbId, err := j.srcMeta.GetDbId(); err != nil {
		return err
	} else {
		j.Src.DbId = srcDbId
	}

	// Step 3: check src table exists, if not exists, return err
	if j.SyncType == TableSync {
		if src_table_exists, err := j.ISrc.CheckTableExists(); err != nil {
			return err
		} else if !src_table_exists {
			return xerror.Errorf(xerror.Normal, "src table %s.%s not exists", j.Src.Database, j.Src.Table)
		}

		if enable, err := j.ISrc.IsTableEnableBinlog(); err != nil {
			return err
		} else if !enable {
			return xerror.Errorf(xerror.Normal, "src table %s.%s not enable binlog", j.Src.Database, j.Src.Table)
		}

		if srcTableId, err := j.srcMeta.GetTableId(j.Src.Table); err != nil {
			return err
		} else {
			j.Src.TableId = srcTableId
		}
	}

	// Step 4: check dest database && table exists
	// if dest database && table exists, return err
	dest_db_exists, err := j.IDest.CheckDatabaseExists()
	if err != nil {
		return err
	}
	if !dest_db_exists {
		if err := j.IDest.CreateDatabase(); err != nil {
			return err
		}
	}
	if destDbId, err := j.destMeta.GetDbId(); err != nil {
		return err
	} else {
		j.Dest.DbId = destDbId
	}
	if j.SyncType == TableSync {
		dest_table_exists, err := j.IDest.CheckTableExists()
		if err != nil {
			return err
		}
		if dest_table_exists {
			return xerror.Errorf(xerror.Normal, "dest table %s.%s already exists", j.Dest.Database, j.Dest.Table)
		}
	}

	return nil
}

// HACK: temp impl
func (j *Job) GetLag() (int64, error) {
	j.lock.Lock()
	defer j.lock.Unlock()

	srcSpec := &j.Src
	rpc, err := j.rpcFactory.NewFeRpc(srcSpec)
	if err != nil {
		return 0, err
	}

	commitSeq := j.progress.CommitSeq
	resp, err := rpc.GetBinlogLag(srcSpec, commitSeq)
	if err != nil {
		return 0, err
	}

	log.Debugf("resp: %v, lag: %d", resp, resp.GetLag())
	return resp.GetLag(), nil
}

func (j *Job) getJobState() JobState {
	j.lock.Lock()
	defer j.lock.Unlock()

	return j.State
}

func (j *Job) changeJobState(state JobState) error {
	j.lock.Lock()
	defer j.lock.Unlock()

	if j.State == state {
		log.Debugf("job %s state is already %s", j.Name, state)
		return nil
	}

	originState := j.State
	j.State = state
	if err := j.persistJob(); err != nil {
		j.State = originState
		return err
	}
	log.Debugf("change job %s state from %s to %s", j.Name, originState, state)
	return nil
}

func (j *Job) Pause() error {
	log.Infof("pause job %s", j.Name)

	return j.changeJobState(JobPaused)
}

func (j *Job) Resume() error {
	log.Infof("resume job %s", j.Name)

	return j.changeJobState(JobRunning)
}

type JobStatus struct {
	Name          string `json:"name"`
	State         string `json:"state"`
	ProgressState string `json:"progress_state"`
}

func (j *Job) Status() *JobStatus {
	j.lock.Lock()
	defer j.lock.Unlock()

	state := j.State.String()
	progress_state := j.progress.SyncState.String()

	return &JobStatus{
		Name:          j.Name,
		State:         state,
		ProgressState: progress_state,
	}
}
