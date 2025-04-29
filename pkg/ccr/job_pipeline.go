package ccr

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"time"

	"github.com/selectdb/ccr_syncer/pkg/ccr/record"
	festruct "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/frontendservice"
	tstatus "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/status"
	ttypes "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/types"
	utils "github.com/selectdb/ccr_syncer/pkg/utils"
	"github.com/selectdb/ccr_syncer/pkg/xerror"
	"github.com/selectdb/ccr_syncer/pkg/xmetrics"
	log "github.com/sirupsen/logrus"
)

type TxnLink struct {
	// The previous txn link
	Prev <-chan any
	// The next txn link
	Next chan<- any
}

func (l *TxnLink) Done(fn func()) {
	if l.Prev != nil {
		<-l.Prev // Wait the previous txn
	}
	fn()
	if l.Next != nil {
		l.Next <- struct{}{} // Notify the next txn link
	}
}

type TxnContext struct {
	// The commit seq of the upsert binlog
	CommitSeq int64 `json:"commit_seq"`
	// The txn id
	TxnId int64 `json:"txn_id"`
	// The txn label
	Label string `json:"label"`
	// The txn commit info
	CommitInfos []*ttypes.TTabletCommitInfo `json:"commit_infos"`
	// Is txn insert? (which contains sub txn info)
	IsTxnInsert bool `json:"is_txn_insert"`
	// The sub txn info of the txn insert
	SubTxnInfos []*festruct.TSubTxnInfo `json:"sub_txn_infos"`
	// The source sub table ids involved in this txn
	SourceStids []int64 `json:"source_stid"`
	// The dest sub table ids involved in this txn
	DestStids []int64 `json:"desc_stid"`
	// The dest table ids involved in this txn
	DestTableIds []int64 `json:"dest_table_ids"`
	// The table records involved in this txn
	TableRecords []*record.TableRecord `json:"table_records"`
	// The txn link, to preserve the txn commit sequence
	Link TxnLink `json:"-"`
	// The defer metrics handle
	DeferMetricsHandle func() `json:"-"`
}

type TxnIngestResult struct {
	// The ingesting txn context
	Context *TxnContext
	// The ingesting error
	Err error
}

// A in-memory context for the pipeline ingesting.
type JobPipelineContext struct {
	// The next commit seq to get binlogs
	NextCommitSeq int64
	// The binlogs cached in the job
	Binlogs []*festruct.TBinlog
	// The commitable upsert txns ingesting result
	CommitCh chan TxnIngestResult
	// The txn link
	NextTxnLink chan any
	// Is pipeline closed (no more txn will be launched)
	Close bool
	// The cancel context for the ingesting job
	Context context.Context
	// The cancel function for the ingesting job
	Cancel context.CancelFunc
}

type PipelineInMemoryData struct {
	RunningTxnList []*TxnContext `json:"running_txn_list,omitempty"`
	CommitableTxn  *TxnContext   `json:"commitable_txn,omitempty"`
}

func (data *PipelineInMemoryData) hasAvailableSlot() bool {
	return len(data.RunningTxnList) < MaxPipelineIngestingNum
}

func (data *PipelineInMemoryData) consumeCommitableTxn() {
	data.CommitableTxn = nil
	data.RunningTxnList = data.RunningTxnList[1:]
}

func (data *PipelineInMemoryData) setCommitableTxn(ctx *TxnContext) error {
	if len(data.RunningTxnList) == 0 {
		return xerror.Errorf(xerror.Normal, "found a commitable txn, but running txns is empty")
	}

	if ctx.TxnId != data.RunningTxnList[0].TxnId {
		return xerror.Errorf(xerror.Normal, "commitable txn is not match the issued order, commitable txn id: %d, issued txn id: %d",
			ctx.TxnId, data.RunningTxnList[0].TxnId)
	}

	data.CommitableTxn = ctx
	return nil
}

var MaxPipelineIngestingNum int

func init() {
	flag.IntVar(&MaxPipelineIngestingNum, "max_pipeline_ingesting_num", 16,
		"The max num of pipeline ingesting txns, default is 16")
}

func (ctx *JobPipelineContext) hasBinlogs() bool {
	return len(ctx.Binlogs) > 0
}

func (ctx *JobPipelineContext) hasNonUpsertBinlog() bool {
	return len(ctx.Binlogs) > 0 && ctx.Binlogs[0].GetType() != festruct.TBinlogType_UPSERT
}

func (ctx *JobPipelineContext) getNextBinlog() *festruct.TBinlog {
	return ctx.Binlogs[0]
}

func (ctx *JobPipelineContext) consumeNextBinlog() {
	ctx.Binlogs = ctx.Binlogs[1:]
}

func (ctx *JobPipelineContext) getNextUpsertBinlog() *festruct.TBinlog {
	if len(ctx.Binlogs) == 0 {
		return nil
	}
	binlog := ctx.Binlogs[0]
	if binlog.GetType() != festruct.TBinlogType_UPSERT {
		return nil
	}
	return binlog
}

func (ctx *JobPipelineContext) takeNextCommitableTxn(wait bool) (*TxnContext, error) {
	if !wait {
		select {
		case result := <-ctx.CommitCh:
			return result.Context, result.Err
		default:
			return nil, nil
		}
	}

	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case result := <-ctx.CommitCh:
		return result.Context, result.Err
	case <-timer.C:
		return nil, nil
	}
}

// The state machine of pipeline sync
//
// |        +-----------------------------------+
// |        v                                   |
// |    DONE -> LAUNCH_TRANSACTION <-> COMMIT_PIPELINE <-> COMMIT_PIPELINE_TRANSACTION
// |       ^                  |                 |               |
// |       |                  |                 |               |
// |    ROLLBACK_PIPELINE <----+----------------+---------------+
func (j *Job) pipelineSync() error {
	log.Trace("start pipeline sync")

	// May rollback the pipeline
	if j.isNeedRollbackPipeline() {
		if err := j.rollbackPipeline(); err != nil {
			return err
		}
	}

	j.drainStaledBinlogs()

	hasMoreBinlogs := true
	for !j.hasInterruptSignal() {
		switch j.progress.SubSyncState {
		case Done:
			log.Tracef("pipeline sync: done")
			j.initializePipelineContext()

			// Trigger a partial snapshot
			if j.Extra.PartialSnapshotParams != nil {
				j.resetPipeline()
				params := j.Extra.PartialSnapshotParams
				return j.NewPartialSnapshot(params.TableId, params.TableName, params.Partitions, params.Replace, params.IsView)
			}

			// For compatible with the old version, we need to check the upsert binlog progress.
			if !j.progress.IsDone() {
				log.Infof("job progress is not done, need recover. state: %s, prevCommitSeq: %d, commitSeq: %d",
					j.progress.SubSyncState, j.progress.PrevCommitSeq, j.progress.CommitSeq)
				j.resetPipeline()
				return j.recoverIncrementalSync()
			}

			if exit, err := j.maySkipBinlog(); err != nil {
				return err
			} else if exit {
				j.resetPipeline()
				return nil
			}

			// Step launch transaction if no binlogs or the next binlog is an upsert binlog.
			if !j.pipelineCtx.hasNonUpsertBinlog() {
				j.progress.NextSubVolatile(LaunchTransaction, &PipelineInMemoryData{})
				continue
			}

			binlog := j.pipelineCtx.getNextBinlog()
			if err, back := j.handleBinlog(binlog); err != nil {
				return err
			} else if back {
				j.resetPipeline()
				return nil
			} else {
				j.pipelineCtx.consumeNextBinlog()
			}

		case LaunchTransaction:
			log.Tracef("pipeline sync: launch transaction")

			hasMoreBinlogs = true

			// fetch the binlogs, if the binlogs is empty.
			if !j.pipelineCtx.hasBinlogs() {
				if err := j.getNextBinlogs(); err != nil {
					return err
				}
				hasMoreBinlogs = len(j.pipelineCtx.Binlogs) > 0
			}

			binlog := j.pipelineCtx.getNextUpsertBinlog()
			if binlog != nil {
				if err := j.launchTxn(binlog); err != nil {
					j.resetPipeline() // reset the pipeline context, to force the pipeline to rollback.
					return err
				}
				j.pipelineCtx.consumeNextBinlog()
			}

			j.progress.NextSubVolatile(CommitPipeline, j.progress.InMemoryData)

		case CommitPipeline:
			log.Tracef("pipeline sync: commit pipeline")
			data := j.progress.InMemoryData.(*PipelineInMemoryData)
			if len(data.RunningTxnList) > 0 {
				// The situations to wait new txn
				// 1. The pipeline is closed:
				// 		when the left txns are committed, the pipeline will be closed.
				// 2. The pipeline no more binlogs to launch:
				// 		yield the pipeline when the left txns are committed.
				// 3. The pipeline has no available slot to launch new txns:
				// 		wait for the txns to be committed.
				// 4. The pipeline has non-upsert binlogs:
				// 		when the left txns are committed, the pipeline will be closed.
				waitTxn := j.pipelineCtx.Close || !hasMoreBinlogs || !data.hasAvailableSlot() || j.pipelineCtx.hasNonUpsertBinlog()
				log.Tracef("pipeline sync: take next commitable txn, wait: %t", waitTxn)
				if ctx, err := j.pipelineCtx.takeNextCommitableTxn(waitTxn); err == errTriggerPartialSnapshot {
					j.progress.NextSubCheckpoint(RollbackPipeline, data)
					continue
				} else if err != nil {
					j.resetPipeline() // reset the pipeline context, to force the pipeline to rollback.
					return err
				} else if ctx != nil {
					log.Tracef("pipeline sync: get commitable txn, txn id: %d", ctx.TxnId)
					if err := data.setCommitableTxn(ctx); err != nil {
						j.resetPipeline()
						return err
					}
					j.progress.NextSubCheckpoint(CommitPipelineTransaction, data)
					continue
				}
			}

			// Determine the next state
			hasRunningTxn := len(data.RunningTxnList) > 0
			if (j.Extra.SkipBinlog || j.pipelineCtx.hasNonUpsertBinlog() || j.pipelineCtx.Close) && !hasRunningTxn {
				// All the txns are committed, and the pipeline is finished.
				log.Tracef("pipeline sync step done, skip binlog: %t, pipeline close: %t, has running txn: %t, has non upsert binlog: %t",
					j.Extra.SkipBinlog, j.pipelineCtx.Close, hasRunningTxn, j.pipelineCtx.hasNonUpsertBinlog())
				j.progress.NextSubCheckpoint(Done, nil)
			} else if !j.pipelineCtx.Close && hasMoreBinlogs && data.hasAvailableSlot() {
				// There is a chance that to launch more upsert binlogs
				j.progress.NextSubVolatile(LaunchTransaction, data)
			} else if !hasMoreBinlogs && !hasRunningTxn {
				// No more binlogs, no running txns, yield the pipeline.
				return nil
			} else {
				// Wait for the txns to be committed.
			}

		case CommitPipelineTransaction:
			log.Tracef("pipeline sync: commit pipeline transaction")
			data := j.progress.InMemoryData.(*PipelineInMemoryData)
			if data.CommitableTxn == nil {
				return xerror.Errorf(xerror.Normal, "commitable txn is nil but state is commit pipeline transaction")
			}

			if err := j.commitTxn(data.CommitableTxn); err != nil {
				j.resetPipeline() // reset the pipeline context, to force the pipeline to rollback.
				return err
			}

			data.consumeCommitableTxn()
			j.progress.DoneSubCheckpoint(CommitPipeline, data)

			// release the binlogs before PrevCommitSeq.
			if err := j.lockBinlog(j.progress.PrevCommitSeq); err != nil {
				return err
			}

			j.updateJobStatus()

		case RollbackPipeline:
			// NOTE: pipelineCtx may be nil, if the pipeline is not started.
			log.Tracef("pipeline sync: rollback pipeline")
			if err := j.mayLoadPipelineInMemoryData(); err != nil {
				return err
			}

			// Rollback all the inflighting txns in the pipeline
			data := j.progress.InMemoryData.(*PipelineInMemoryData)
			for _, ctx := range data.RunningTxnList {
				if err := j.rollbackTxn(ctx); err != nil {
					return err
				}
			}
			j.progress.NextSubCheckpoint(Done, nil)

		default:
			return xerror.Errorf(xerror.Normal, "unknown pipeline sync state: %v", j.progress.SubSyncState)
		}
	}

	return nil
}

func (j *Job) drainStaledBinlogs() {
	if j.pipelineCtx == nil {
		return
	}

	for len(j.pipelineCtx.Binlogs) > 0 {
		binlog := j.pipelineCtx.Binlogs[0]
		if binlog.GetCommitSeq() > j.progress.PrevCommitSeq {
			break
		}
		j.pipelineCtx.Binlogs = j.pipelineCtx.Binlogs[1:]
	}
}

func (j *Job) mayLoadPipelineInMemoryData() error {
	if j.progress.InMemoryData == nil {
		var data PipelineInMemoryData
		// may restart the job, and persist data is not set
		if j.progress.PersistData == "" {
			log.Warnf("PersistData is empty, initializing with empty PipelineInMemoryData")
			j.progress.InMemoryData = &data
			return nil
		}
		if err := json.Unmarshal([]byte(j.progress.PersistData), &data); err != nil {
			return xerror.Errorf(xerror.Normal, "unmarshal pipeline memory data failed, err: %v", err)
		}
		j.progress.InMemoryData = &data
	}
	return nil
}

func (j *Job) initializePipelineContext() {
	if j.pipelineCtx != nil {
		j.pipelineCtx.Close = false
		return
	}

	log.Debugf("initial a pipeline, commit seq: %d", j.progress.CommitSeq)
	txnLink := make(chan any, 1)
	txnLink <- struct{}{} // Send a signal to the channel, to indicate that the previous txn is ready.
	ctx, cancel := context.WithCancel(context.Background())
	j.pipelineCtx = &JobPipelineContext{
		NextCommitSeq: j.progress.CommitSeq,
		CommitCh:      make(chan TxnIngestResult, 100),
		NextTxnLink:   txnLink,
		Close:         false,
		Context:       ctx,
		Cancel:        cancel,
	}
}

func (j *Job) resetPipeline() {
	if j.pipelineCtx == nil {
		panic("should not be here")
	}

	j.pipelineCtx.Cancel()
	j.pipelineCtx = nil
}

func (j *Job) isNeedRollbackPipeline() bool {
	// It seems that we are rebootted, and the pipeline is not finished.
	return j.pipelineCtx == nil && j.progress.SubSyncState != Done
}

func (j *Job) rollbackPipeline() error {
	if !j.isNeedRollbackPipeline() {
		panic("should not be here")
	}

	if j.progress.SubSyncState.BinlogType == BinlogUpsert {
		// compatible with the old version, convert it to Done state to rollback it.
		j.progress.NextSubVolatile(Done, nil)
		return nil
	}

	if err := j.mayLoadPipelineInMemoryData(); err != nil {
		return err
	}

	data, ok := j.progress.InMemoryData.(*PipelineInMemoryData)
	if !ok {
		return xerror.Errorf(xerror.Normal, "invalid pipeline in memory data: %v", j.progress.InMemoryData)
	}

	switch j.progress.SubSyncState {
	case LaunchTransaction, CommitPipeline, CommitPipelineTransaction:
		j.progress.NextSubCheckpoint(RollbackPipeline, data)
		return nil
	case RollbackPipeline:
		return nil
	default:
		return xerror.Errorf(xerror.Normal, "invalid pipeline state: %v", j.progress.SubSyncState)
	}
}

func (j *Job) getNextBinlogs() error {
	if len(j.pipelineCtx.Binlogs) > 0 {
		panic("should not be here")
	}

	commitSeq := j.pipelineCtx.NextCommitSeq
	if commitSeq < j.progress.PrevCommitSeq {
		panic(fmt.Sprintf("the prev commit seq in pipeline ctx %d less than the progress prev commit seq: %d", commitSeq, j.progress.PrevCommitSeq))
	}

	src := &j.Src
	srcRpc, err := j.factory.NewFeRpc(src)
	if err != nil {
		log.Errorf("new fe rpc failed, src: %v, err: %+v", src, err)
		return err
	}
	log.Tracef("src: %s, commitSeq: %d", src, commitSeq)

	getBinlogResp, err := srcRpc.GetBinlog(src, commitSeq, flagBinlogBatchSize)
	if err != nil {
		return err
	}
	log.Tracef("get binlog resp: %v", getBinlogResp)

	status := getBinlogResp.GetStatus()
	switch status.StatusCode {
	case tstatus.TStatusCode_OK:
	case tstatus.TStatusCode_BINLOG_TOO_OLD_COMMIT_SEQ:
	case tstatus.TStatusCode_BINLOG_TOO_NEW_COMMIT_SEQ:
		// consume prev txn id for not to check and wait prev transaction finished
		if j.progress.PrevTxnId != -1 {
			log.Infof("consume prev txn id: %d", j.progress.PrevTxnId)
			j.Dest.WaitTransactionDone(j.progress.PrevTxnId)
			j.progress.PrevTxnId = -1
			j.progress.Persist()
		}
		return nil
	case tstatus.TStatusCode_BINLOG_DISABLE:
		return xerror.Errorf(xerror.Normal, "binlog is disabled")
	case tstatus.TStatusCode_BINLOG_NOT_FOUND_DB:
		return xerror.Errorf(xerror.Normal, "can't found db")
	case tstatus.TStatusCode_BINLOG_NOT_FOUND_TABLE:
		return xerror.Errorf(xerror.Normal, "can't found table")
	default:
		return xerror.Errorf(xerror.Normal, "invalid binlog status type: %v, msg: %s",
			status.StatusCode, utils.FirstOr(status.GetErrorMsgs(), ""))
	}

	binlogs := getBinlogResp.GetBinlogs()
	if len(binlogs) == 0 {
		return xerror.Errorf(xerror.Normal, "no binlog, but status code is: %v", status.StatusCode)
	}

	lastBinlog := binlogs[len(binlogs)-1]

	j.pipelineCtx.Binlogs = binlogs
	j.pipelineCtx.NextCommitSeq = lastBinlog.GetCommitSeq()

	log.Tracef("get %d binlogs, next commit seq: %d", len(binlogs), j.pipelineCtx.NextCommitSeq)
	return nil
}

func (j *Job) launchTxn(binlog *festruct.TBinlog) error {
	if binlog.GetType() != festruct.TBinlogType_UPSERT {
		return xerror.Errorf(xerror.Normal, "launch txn but binlog type: %v is not a UPSERT", binlog.GetType())
	}

	if utils.HasJobFailpoint(j.Name, "handle_binlog_failed") {
		log.Warnf("fail to handle binlog by failpoint, binlog type: %s, binlog data: %s", festruct.TBinlogType_UPSERT, binlog.GetData())
		return xerror.Errorf(xerror.Normal, "fail to handle binlog by failpoint")
	}

	commitSeq := binlog.GetCommitSeq()
	binlogType := binlog.GetType()
	log.Debugf("binlog type: %s, commit seq: %d, binlog data: %s", binlogType, commitSeq, binlog.GetData())

	ctx, err := j.buildTxnContextWithRetry(binlog)
	if err != nil {
		return err
	} else if ctx == nil {
		// Skip the binlog since txn context is nil
		return nil
	} else if err = j.beginTxn(ctx); err != nil {
		return err
	} else {
		return j.launchIngestJob(ctx)
	}
}

// Like buildTxnContext, but retry the meta is staled.
func (j *Job) buildTxnContextWithRetry(binlog *festruct.TBinlog) (*TxnContext, error) {
	ctx, err := j.buildTxnContext(binlog)
	if err == nil {
		return ctx, nil
	} else if !xerror.IsCategory(err, xerror.Meta) {
		return nil, err
	} else {
		log.Warnf("a meta error occurred, retry to handle upsert binlog again, commitSeq: %d", binlog.GetCommitSeq())
		return j.buildTxnContext(binlog)
	}
}

func (j *Job) buildTxnContext(binlog *festruct.TBinlog) (*TxnContext, error) {
	data := binlog.GetData()
	upsert, err := record.NewUpsertFromJson(data)
	if err != nil {
		return nil, err
	}
	log.Tracef("upsert: %v", upsert)

	isTxnInsert := upsert.IsTxnInsert()
	if isTxnInsert && !featureTxnInsert {
		log.Warnf("The txn insert is not supported yet")
		return nil, xerror.Errorf(xerror.Normal, "The txn insert is not supported yet")
	}

	commitSeq := upsert.CommitSeq
	metricsHandle := xmetrics.RecordHandlingBinlog(j.Name, commitSeq)

	// Step 1: get related tableRecords
	tableRecords, err := j.getRelatedTableRecords(upsert)
	if err != nil {
		log.Errorf("get related table records failed, err: %+v", err)
		return nil, err
	}
	if len(tableRecords) == 0 {
		log.Debug("no related table records")
		return nil, nil
	}

	destTableIds := make([]int64, 0, len(tableRecords))
	if j.SyncType == DBSync {
		savedRecords := make([]*record.TableRecord, 0, len(tableRecords))
		for _, tableRecord := range tableRecords {
			if isAsyncMv, err := j.IsMaterializedViewTable(tableRecord.Id); err != nil {
				return nil, err
			} else if isAsyncMv {
				// ignore the upsert of materialized view table.
				continue
			} else if destTableId, err := j.GetDestTableIdBySrc(tableRecord.Id); err != nil {
				return nil, err
			} else {
				savedRecords = append(savedRecords, tableRecord)
				destTableIds = append(destTableIds, destTableId)
			}
		}
		tableRecords = savedRecords
	} else {
		destTableIds = append(destTableIds, j.Dest.TableId)
	}
	if len(tableRecords) == 0 {
		log.Debug("no related table records")
		return nil, nil
	}

	log.Debugf("handle upsert, table records: %v", tableRecords)

	linkCh := make(chan any, 1)
	prevCh := j.pipelineCtx.NextTxnLink
	j.pipelineCtx.NextTxnLink = linkCh
	link := TxnLink{Prev: prevCh, Next: linkCh}
	ctx := &TxnContext{
		CommitSeq:          upsert.CommitSeq,
		Label:              upsert.Label,
		IsTxnInsert:        isTxnInsert,
		SourceStids:        upsert.Stids,
		DestTableIds:       destTableIds,
		TableRecords:       tableRecords,
		Link:               link,
		DeferMetricsHandle: metricsHandle,
	}
	return ctx, nil
}

func (j *Job) beginTxn(ctx *TxnContext) error {
	commitSeq := ctx.CommitSeq
	sourceStids := ctx.SourceStids
	isTxnInsert := ctx.IsTxnInsert
	destTableIds := ctx.DestTableIds

	dest := &j.Dest
	destRpc, err := j.factory.NewFeRpc(dest)
	if err != nil {
		return err
	}

	var label string
	if j.Extra.ReuseBinlogLabel {
		label = ctx.Label
	} else {
		label = j.newLabel(commitSeq)
	}
	log.Tracef("begin txn, label: %s, dest: %v, commitSeq: %d", label, dest, commitSeq)

	var beginTxnResp *festruct.TBeginTxnResult_
	if isTxnInsert {
		// when txn insert, give an array length in BeginTransaction, it will return a list of stid
		beginTxnResp, err = destRpc.BeginTransactionForTxnInsert(dest, label, destTableIds, int64(len(sourceStids)))
	} else {
		beginTxnResp, err = destRpc.BeginTransaction(dest, label, destTableIds)
	}
	if err != nil {
		return err
	}
	log.Tracef("begin txn resp: %v", beginTxnResp)

	if beginTxnResp.GetStatus().GetStatusCode() != tstatus.TStatusCode_OK {
		if isTableNotFound(beginTxnResp.GetStatus()) && j.SyncType == DBSync {
			// It might caused by the staled TableMapping entries.
			// In order to rebuild the dest table ids, this progress should be rollback.
			j.progress.Rollback()
			for _, tableRecord := range ctx.TableRecords {
				delete(j.progress.TableMapping, tableRecord.Id)
			}
		}
		return xerror.Errorf(xerror.Normal, "begin txn failed, status: %v", beginTxnResp.GetStatus())
	}
	txnId := beginTxnResp.GetTxnId()
	if isTxnInsert {
		destStids := beginTxnResp.GetSubTxnIds()
		ctx.DestStids = destStids
		log.Infof("begin txn %d, label: %s, db: %d, destStids: %v",
			txnId, label, beginTxnResp.GetDbId(), destStids)
	} else {
		log.Infof("begin txn %d, label: %s, db: %d", txnId, label, beginTxnResp.GetDbId())
	}

	ctx.TxnId = txnId

	data := j.progress.InMemoryData.(*PipelineInMemoryData)
	data.RunningTxnList = append(data.RunningTxnList, ctx)
	j.progress.PersistInMemoryData()

	return nil
}

func (j *Job) launchIngestJob(ctx *TxnContext) error {
	ingestJob, err := j.prepareIngestJobWithRetry(ctx)
	if err == errTriggerPartialSnapshot {
		if j.Extra.PartialSnapshotParams == nil {
			panic("partial snapshot params is nil when trigger partial snapshot")
		}
		log.Debugf("txn %d launch ingest job, close the pipeline to trigger partial snapshot, commit seq: %d", ctx.TxnId, ctx.CommitSeq)
		j.pipelineCtx.Close = true
		commitCh := j.pipelineCtx.CommitCh
		// Send a trigger signal to the commit channel, force pipeline to commit previous txns and rollback the current txn.
		go func() {
			ctx.Link.Done(func() {
				commitCh <- TxnIngestResult{Context: ctx, Err: err}
			})
		}()
		return nil
	} else if err != nil {
		return err
	}

	// Try ingesting the binlogs in async
	commitCh := j.pipelineCtx.CommitCh
	cancelCtx := j.pipelineCtx.Context
	go func() {
		// ATTN: this function is called in a goroutine, so we need to be careful about the context.
		ingestJob.Ingest(cancelCtx)
		err := ingestJob.Error()
		if err == nil {
			ctx.CommitInfos = ingestJob.GetTabletCommitInfos()
			if ctx.IsTxnInsert {
				ctx.SubTxnInfos = ingestJob.GetSubTxnInfos()
			}
		}

		ctx.Link.Done(func() {
			commitCh <- TxnIngestResult{Context: ctx, Err: err}
		})
	}()
	return nil
}

func (j *Job) prepareIngestJobWithRetry(ctx *TxnContext) (*IngestBinlogJob, error) {
	job, err := j.prepareIngestJob(ctx)
	if err == nil {
		return job, nil
	} else if !xerror.IsCategory(err, xerror.Meta) {
		return nil, err
	} else {
		log.Warnf("a meta error occurred, retry to handle upsert binlog again, commitSeq: %d", ctx.CommitSeq)
		return j.prepareIngestJob(ctx)
	}
}

func (j *Job) prepareIngestJob(ctx *TxnContext) (*IngestBinlogJob, error) {
	commitSeq := ctx.CommitSeq
	txnId := ctx.TxnId
	isTxnInsert := ctx.IsTxnInsert
	tableRecords := ctx.TableRecords

	log.Tracef("prepare ingest job, commitSeq: %d, txnId: %d, is txn insert: %t", commitSeq, txnId, isTxnInsert)

	stidMap := make(map[int64]int64)
	if isTxnInsert {
		sourceStids := ctx.SourceStids
		destStids := ctx.DestStids
		if len(sourceStids) == len(destStids) {
			for i := 0; i < len(sourceStids); i++ {
				stidMap[sourceStids[i]] = destStids[i]
			}
		}
	}

	job, err := NewIngestBinlogJob(NewIngestContext(commitSeq, txnId, tableRecords, j.progress.TableMapping, stidMap), j)
	if err != nil {
		return nil, err
	}

	job.Prepare()
	if err := job.Error(); err != nil {
		return nil, err
	}

	return job, nil
}

func (j *Job) commitTxn(ctx *TxnContext) error {
	dest := &j.Dest
	txnId := ctx.TxnId
	commitInfos := ctx.CommitInfos

	destRpc, err := j.factory.NewFeRpc(dest)
	if err != nil {
		return err
	}

	isTxnInsert := ctx.IsTxnInsert
	subTxnInfos := ctx.SubTxnInfos
	var resp *festruct.TCommitTxnResult_
	if isTxnInsert {
		resp, err = destRpc.CommitTransactionForTxnInsert(dest, txnId, true, subTxnInfos)
	} else {
		onlyCommitTxn := featureSkipWaitingTxnPublish
		resp, err = destRpc.CommitTransaction(dest, txnId, commitInfos, onlyCommitTxn)
	}
	if err != nil {
		return err
	}
	log.Tracef("commit txn %d resp: %v", txnId, resp)

	if statusCode := resp.Status.GetStatusCode(); statusCode == tstatus.TStatusCode_PUBLISH_TIMEOUT {
		dest.WaitTransactionDone(txnId)
	} else if statusCode != tstatus.TStatusCode_OK {
		err := xerror.Errorf(xerror.Normal, "commit txn failed, status: %v", resp.Status)
		return err
	}

	log.Infof("commit txn %d success, commit seq: %d", txnId, ctx.CommitSeq)

	j.applyTxn(ctx)
	return nil
}

func (j *Job) applyTxn(ctx *TxnContext) {
	commitSeq := ctx.CommitSeq
	destTableIds := ctx.DestTableIds

	j.progress.PrevTxnId = ctx.TxnId
	j.progress.CommitSeq = commitSeq
	if j.SyncType == DBSync && len(j.progress.TableCommitSeqMap) > 0 {
		for _, tableId := range destTableIds {
			tableCommitSeq, ok := j.progress.TableCommitSeqMap[tableId]
			if !ok {
				continue
			}

			if tableCommitSeq < commitSeq {
				j.progress.TableCommitSeqMap[tableId] = commitSeq
			}
		}
	}

	if ctx.DeferMetricsHandle != nil {
		ctx.DeferMetricsHandle()
	}

	j.afterHandleBinlog(commitSeq)
}

func (j *Job) rollbackTxn(ctx *TxnContext) error {
	dest := &j.Dest
	destRpc, err := j.factory.NewFeRpc(dest)
	if err != nil {
		return err
	}

	txnId := ctx.TxnId
	resp, err := destRpc.RollbackTransaction(dest, txnId)
	if err != nil {
		return err
	}
	if resp.Status.GetStatusCode() != tstatus.TStatusCode_OK {
		if isTxnNotFound(resp.Status) {
			log.Warnf("txn not found, txnId: %d", txnId)
		} else if isTxnAborted(resp.Status) {
			log.Infof("txn already aborted, txnId: %d", txnId)
		} else if isTxnCommitted(resp.Status) {
			log.Infof("txn already committed, txnId: %d", txnId)
			j.applyTxn(ctx)
			return nil
		} else {
			return xerror.Errorf(xerror.Normal, "rollback txn failed, status: %v", resp.Status)
		}
	}

	if ctx.DeferMetricsHandle != nil {
		ctx.DeferMetricsHandle()
	}

	log.Infof("rollback txn %d success", txnId)
	return nil
}
