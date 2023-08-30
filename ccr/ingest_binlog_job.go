package ccr

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/modern-go/gls"
	"github.com/pkg/errors"
	"github.com/selectdb/ccr_syncer/ccr/base"
	"github.com/selectdb/ccr_syncer/ccr/record"
	bestruct "github.com/selectdb/ccr_syncer/rpc/kitex_gen/backendservice"
	tstatus "github.com/selectdb/ccr_syncer/rpc/kitex_gen/status"
	ttypes "github.com/selectdb/ccr_syncer/rpc/kitex_gen/types"
	utils "github.com/selectdb/ccr_syncer/utils"
	log "github.com/sirupsen/logrus"
)

type tabletIngestBinlogHandler struct {
	ingestJob       *IngestBinlogJob
	binlogVersion   int64
	srcTablet       *TabletMeta
	destTablet      *TabletMeta
	destPartitionId int64

	err     error
	errLock sync.Mutex

	cancel atomic.Bool
	wg     sync.WaitGroup
}

func (h *tabletIngestBinlogHandler) setError(err error) {
	h.errLock.Lock()
	defer h.errLock.Unlock()

	h.err = err
}

func (h *tabletIngestBinlogHandler) error() error {
	h.errLock.Lock()
	defer h.errLock.Unlock()

	return h.err
}

// handle Replica
func (h *tabletIngestBinlogHandler) handleReplica(destReplica *ReplicaMeta) bool {
	if h.cancel.Load() {
		return true
	}

	j := h.ingestJob
	binlogVersion := h.binlogVersion
	srcTablet := h.srcTablet
	destReplicaId := destReplica.Id
	destPartitionId := h.destPartitionId

	log.Debugf("handle dest replica id: %v", destReplicaId)
	destBackend, ok := j.destBackendMap[destReplica.BackendId]
	if !ok {
		j.setError(errors.Errorf("backend not found, backend id: %d", destReplica.BackendId))
		return false
	}
	destTabletId := destReplica.TabletId

	destRpc, err := h.ingestJob.ccrJob.rpcFactory.NewBeRpc(destBackend)
	if err != nil {
		j.setError(err)
		return false
	}
	loadId := ttypes.NewTUniqueId()
	loadId.SetHi(-1)
	loadId.SetLo(-1)

	srcReplicas := srcTablet.ReplicaMetas
	iter := srcReplicas.Iter()
	if ok := iter.First(); !ok {
		j.setError(errors.Errorf("src replicas is empty"))
		return false
	}
	srcBackendId := iter.Value().BackendId
	var srcBackend *base.Backend
	srcBackend, ok = j.srcBackendMap[srcBackendId]
	if !ok {
		j.setError(errors.Errorf("backend not found, backend id: %d", srcBackendId))
		return false
	}
	req := &bestruct.TIngestBinlogRequest{
		TxnId:          utils.ThriftValueWrapper(j.txnId),
		RemoteTabletId: utils.ThriftValueWrapper[int64](srcTablet.Id),
		BinlogVersion:  utils.ThriftValueWrapper(binlogVersion),
		RemoteHost:     utils.ThriftValueWrapper(srcBackend.Host),
		RemotePort:     utils.ThriftValueWrapper(srcBackend.GetHttpPortStr()),
		PartitionId:    utils.ThriftValueWrapper[int64](destPartitionId),
		LocalTabletId:  utils.ThriftValueWrapper[int64](destTabletId),
		LoadId:         loadId,
	}
	commitInfo := &ttypes.TTabletCommitInfo{
		TabletId:  destTabletId,
		BackendId: destBackend.Id,
	}

	h.wg.Add(1)
	go func() {
		gls.ResetGls(gls.GoID(), map[interface{}]interface{}{})
		gls.Set("job", j.ccrJob.Name)

		defer h.wg.Done()

		resp, err := destRpc.IngestBinlog(req)
		if err != nil {
			j.setError(err)
			return
		}

		log.Infof("ingest resp: %v", resp)
		if !resp.IsSetStatus() {
			err = errors.Errorf("ingest resp status not set")
			j.setError(err)
			return
		} else if resp.Status.StatusCode != tstatus.TStatusCode_OK {
			err = errors.Errorf("ingest resp status code: %v, msg: %v", resp.Status.StatusCode, resp.Status.ErrorMsgs)
			j.setError(err)
			return
		} else {
			j.appendCommitInfos(commitInfo)
		}
	}()

	return true
}

func (h *tabletIngestBinlogHandler) handle() {
	h.destTablet.ReplicaMetas.Scan(func(destReplicaId int64, destReplica *ReplicaMeta) bool {
		return h.handleReplica(destReplica)
	})
	h.wg.Wait()
}

type IngestContext struct {
	context.Context
	txnId        int64
	tableRecords []*record.TableRecord
}

func NewIngestContext(txnId int64, tableRecords []*record.TableRecord) *IngestContext {
	return &IngestContext{
		Context:      context.Background(),
		txnId:        txnId,
		tableRecords: tableRecords,
	}
}

type IngestBinlogJob struct {
	ccrJob       *Job // ccr job
	txnId        int64
	tableRecords []*record.TableRecord

	srcBackendMap  map[int64]*base.Backend
	destBackendMap map[int64]*base.Backend

	tabletIngestJobs []*tabletIngestBinlogHandler

	commitInfos     []*ttypes.TTabletCommitInfo
	commitInfosLock sync.Mutex

	err     error
	errLock sync.RWMutex

	wg sync.WaitGroup
}

func NewIngestBinlogJob(ctx context.Context, ccrJob *Job) (*IngestBinlogJob, error) {
	// convert ctx to IngestContext
	ingestCtx, ok := ctx.(*IngestContext)
	if !ok {
		return nil, errors.Errorf("invalid context type: %T", ctx)
	}

	return &IngestBinlogJob{
		ccrJob:       ccrJob,
		txnId:        ingestCtx.txnId,
		tableRecords: ingestCtx.tableRecords,

		commitInfos: make([]*ttypes.TTabletCommitInfo, 0),
	}, nil
}

func (j *IngestBinlogJob) GetTabletCommitInfos() []*ttypes.TTabletCommitInfo {
	return j.commitInfos
}

func (j *IngestBinlogJob) setError(err error) {
	j.errLock.Lock()
	defer j.errLock.Unlock()

	j.err = err
}

func (j *IngestBinlogJob) Error() error {
	j.errLock.RLock()
	defer j.errLock.RUnlock()

	return j.err
}

func (j *IngestBinlogJob) appendCommitInfos(commitInfo *ttypes.TTabletCommitInfo) {
	j.commitInfosLock.Lock()
	defer j.commitInfosLock.Unlock()

	j.commitInfos = append(j.commitInfos, commitInfo)
}

func (j *IngestBinlogJob) CommitInfos() []*ttypes.TTabletCommitInfo {
	j.commitInfosLock.Lock()
	defer j.commitInfosLock.Unlock()

	return j.commitInfos
}

type prepareIndexArg struct {
	binlogVersion   int64
	srcTableId      int64
	srcPartitionId  int64
	destTableId     int64
	destPartitionId int64
	srcIndexMeta    *IndexMeta
	destIndexMeta   *IndexMeta
}

func (j *IngestBinlogJob) prepareIndex(arg *prepareIndexArg) {
	log.Debugf("prepareIndex: %v", arg)

	// Step 1: check tablets
	job := j.ccrJob
	srcTablets, err := job.srcMeta.GetTablets(arg.srcTableId, arg.srcPartitionId, arg.srcIndexMeta.Id)
	if err != nil {
		j.setError(err)
		return
	}
	destTablets, err := job.destMeta.GetTablets(arg.destTableId, arg.destPartitionId, arg.destIndexMeta.Id)
	if err != nil {
		j.setError(err)
		return
	}
	if srcTablets.Len() != destTablets.Len() {
		j.setError(errors.Errorf("src tablets length: %v not equal to dest tablets length: %v", srcTablets.Len(), destTablets.Len()))
		return
	}

	if srcTablets.Len() == 0 {
		log.Warn("src tablets length: 0, skip")
		return
	}

	srcIter := srcTablets.IterMut()
	if !srcIter.First() {
		j.setError(errors.Errorf("src tablets First() failed"))
		return
	}
	destIter := destTablets.IterMut()
	if !destIter.First() {
		j.setError(errors.Errorf("dest tablets First() failed"))
		return
	}

	// Step 2: add tablet ingest jobs
	for {
		srcTablet := srcIter.Value()
		destTablet := destIter.Value()
		tabletIngestBinlogHandler := &tabletIngestBinlogHandler{
			ingestJob:       j,
			binlogVersion:   arg.binlogVersion,
			srcTablet:       srcTablet,
			destTablet:      destTablet,
			destPartitionId: arg.destPartitionId,
		}
		j.tabletIngestJobs = append(j.tabletIngestJobs, tabletIngestBinlogHandler)

		if !srcIter.Next() {
			break
		} else {
			destIter.Next()
		}
	}
}

func (j *IngestBinlogJob) preparePartition(srcTableId, destTableId int64, partitionRecord record.PartitionRecord, indexIds []int64) {
	log.Debugf("partitionRecord: %v", partitionRecord)
	// 废弃 preparePartition， 上面index的那部分是这里的实现
	// 还是要求一下和下游对齐的index length，这个是不可以recover的
	// 思考那些是recover用的，主要就是tablet那块的
	job := j.ccrJob

	srcPartitionId := partitionRecord.Id
	srcPartitionRange := partitionRecord.Range
	destPartitionId, err := job.destMeta.GetPartitionIdByRange(destTableId, srcPartitionRange)
	if err != nil {
		j.setError(err)
		return
	}

	// Step 1: check index id
	srcIndexIdMap, err := j.ccrJob.srcMeta.GetIndexIdMap(srcTableId, srcPartitionId)
	if err != nil {
		j.setError(err)
		return
	}
	destIndexNameMap, err := j.ccrJob.destMeta.GetIndexNameMap(destTableId, destPartitionId)
	if err != nil {
		j.setError(err)
		return
	}
	for _, indexId := range indexIds {
		srcIndexMeta, ok := srcIndexIdMap[indexId]
		if !ok {
			j.setError(errors.Errorf("index id %v not found in src meta", indexId))
			return
		}
		srcIndexName := srcIndexMeta.Name

		if _, ok := destIndexNameMap[srcIndexName]; !ok {
			j.setError(errors.Errorf("index name %v not found in dest meta", srcIndexName))
			return
		}
	}

	// Step 2: prepare indexes
	prepareIndexArg := prepareIndexArg{
		binlogVersion:   partitionRecord.Version,
		srcTableId:      srcTableId,
		srcPartitionId:  srcPartitionId,
		destTableId:     destTableId,
		destPartitionId: destPartitionId,
	}
	for _, indexId := range indexIds {
		srcIndexMeta := srcIndexIdMap[indexId]
		destIndexMeta := destIndexNameMap[srcIndexMeta.Name]
		prepareIndexArg.srcIndexMeta = srcIndexMeta
		prepareIndexArg.destIndexMeta = destIndexMeta
		j.prepareIndex(&prepareIndexArg)
	}
}

func (j *IngestBinlogJob) prepareTable(tableRecord *record.TableRecord) {
	log.Debugf("tableRecord: %v", tableRecord)
	job := j.ccrJob
	// TODO: check it before ingestBinlog
	var srcTableId int64
	var destTableId int64

	var err error
	switch job.SyncType {
	case TableSync:
		srcTableId = job.Src.TableId
		destTableId = job.Dest.TableId
	case DBSync:
		srcTableId = tableRecord.Id
		destTableId, err = job.getDestTableIdBySrc(tableRecord.Id)
		if err != nil {
			break
		}
	default:
		err = errors.Errorf("invalid sync type: %s", job.SyncType)
	}
	if err != nil {
		j.setError(err)
		return
	}

	// Step 1: check all partitions in partition records are in src/dest cluster
	srcPartitionMap, err := job.srcMeta.GetPartitionRangeMap(srcTableId)
	if err != nil {
		j.setError(err)
		return
	}
	destPartitionMap, err := job.destMeta.GetPartitionRangeMap(destTableId)
	if err != nil {
		j.setError(err)
		return
	}
	for _, partitionRecord := range tableRecord.PartitionRecords {
		rangeKey := partitionRecord.Range
		// TODO(Improvment, Fix): this may happen after drop partition, can seek partition for more time, check from recycle bin
		if _, ok := srcPartitionMap[rangeKey]; !ok {
			err = errors.Errorf("partition range: %v not in src cluster", rangeKey)
			j.setError(err)
			return
		}
		if _, ok := destPartitionMap[rangeKey]; !ok {
			err = errors.Errorf("partition range: %v not in dest cluster", rangeKey)
			j.setError(err)
			return
		}
	}

	// Step 2: prepare partitions
	for _, partitionRecord := range tableRecord.PartitionRecords {
		j.preparePartition(srcTableId, destTableId, partitionRecord, tableRecord.IndexIds)
	}
}

func (j *IngestBinlogJob) prepareBackendMap() {
	log.Debug("prepareBackendMap")

	job := j.ccrJob

	var err error
	j.srcBackendMap, err = job.srcMeta.GetBackendMap()
	if err != nil {
		j.setError(err)
		return
	}

	j.destBackendMap, err = job.destMeta.GetBackendMap()
	if err != nil {
		j.setError(err)
		return
	}
}

func (j *IngestBinlogJob) prepareTabletIngestJobs() {
	j.tabletIngestJobs = make([]*tabletIngestBinlogHandler, 0)
	for _, tableRecord := range j.tableRecords {
		j.prepareTable(tableRecord)
		if j.Error() != nil {
			return
		}
	}
}

func (j *IngestBinlogJob) runTabletIngestJobs() {
	for _, tabletIngestJob := range j.tabletIngestJobs {
		j.wg.Add(1)
		go func(tabletIngestJob *tabletIngestBinlogHandler) {
			tabletIngestJob.handle()
			j.wg.Done()
		}(tabletIngestJob)
	}
	j.wg.Wait()
}

// TODO(Drogon): use monad error handle
func (j *IngestBinlogJob) Run() {
	j.prepareBackendMap()
	if err := j.Error(); err != nil {
		return
	}

	j.prepareTabletIngestJobs()
	if err := j.Error(); err != nil {
		return
	}

	j.runTabletIngestJobs()
	if err := j.Error(); err != nil {
		return
	}
}
