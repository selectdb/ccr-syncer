package ccr

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/modern-go/gls"
	"github.com/selectdb/ccr_syncer/pkg/ccr/base"
	"github.com/selectdb/ccr_syncer/pkg/ccr/record"
	utils "github.com/selectdb/ccr_syncer/pkg/utils"
	"github.com/selectdb/ccr_syncer/pkg/xerror"

	bestruct "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/backendservice"
	tstatus "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/status"
	ttypes "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/types"

	log "github.com/sirupsen/logrus"
)

var errNotFoundDestMappingTableId = xerror.NewWithoutStack(xerror.Meta, "not found dest mapping table id")

type commitInfosCollector struct {
	commitInfos     []*ttypes.TTabletCommitInfo
	commitInfosLock sync.Mutex
}

func newCommitInfosCollector() *commitInfosCollector {
	return &commitInfosCollector{
		commitInfos: make([]*ttypes.TTabletCommitInfo, 0),
	}
}

func (cic *commitInfosCollector) appendCommitInfos(commitInfo ...*ttypes.TTabletCommitInfo) {
	cic.commitInfosLock.Lock()
	defer cic.commitInfosLock.Unlock()

	cic.commitInfos = append(cic.commitInfos, commitInfo...)
}

func (cic *commitInfosCollector) CommitInfos() []*ttypes.TTabletCommitInfo {
	cic.commitInfosLock.Lock()
	defer cic.commitInfosLock.Unlock()

	return cic.commitInfos
}

type tabletIngestBinlogHandler struct {
	ingestJob       *IngestBinlogJob
	binlogVersion   int64
	srcTablet       *TabletMeta
	destTablet      *TabletMeta
	destPartitionId int64

	*commitInfosCollector

	cancel atomic.Bool
	wg     sync.WaitGroup
}

// handle Replica
func (h *tabletIngestBinlogHandler) handleReplica(srcReplica, destReplica *ReplicaMeta) bool {
	destReplicaId := destReplica.Id
	log.Debugf("handle dest replica id: %d", destReplicaId)

	if h.cancel.Load() {
		log.Infof("job canceled, replica id: %d", destReplicaId)
		return true
	}

	j := h.ingestJob
	binlogVersion := h.binlogVersion
	srcTablet := h.srcTablet
	destPartitionId := h.destPartitionId

	destBackend := j.GetDestBackend(destReplica.BackendId)
	if destBackend == nil {
		j.setError(xerror.XWrapf(errBackendNotFound, "backend id: %d", destReplica.BackendId))
		return false
	}
	destTabletId := destReplica.TabletId

	destRpc, err := h.ingestJob.ccrJob.factory.NewBeRpc(destBackend)
	if err != nil {
		j.setError(err)
		return false
	}
	srcBackendId := srcReplica.BackendId
	srcBackend := j.GetSrcBackend(srcBackendId)
	if srcBackend == nil {
		j.setError(xerror.XWrapf(errBackendNotFound, "backend id: %d", srcBackendId))
		return false
	}
	loadId := ttypes.NewTUniqueId()
	loadId.SetHi(-1)
	loadId.SetLo(-1)
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
	cwind := h.ingestJob.ccrJob.concurrencyManager.GetWindow(destBackend.Id)

	h.wg.Add(1)
	go func() {
		defer h.wg.Done()

		gls.ResetGls(gls.GoID(), map[interface{}]interface{}{})
		gls.Set("job", j.ccrJob.Name)
		defer gls.ResetGls(gls.GoID(), map[interface{}]interface{}{})

		cwind.Acquire()
		defer cwind.Release()

		resp, err := destRpc.IngestBinlog(req)
		if err != nil {
			j.setError(err)
			return
		}

		log.Debugf("ingest resp: %v", resp)
		if !resp.IsSetStatus() {
			err = xerror.Errorf(xerror.BE, "ingest resp status not set, req: %+v", req)
			j.setError(err)
			return
		} else if resp.Status.StatusCode != tstatus.TStatusCode_OK {
			err = xerror.Errorf(xerror.BE, "ingest error, req %v, resp status code: %v, msg: %v", req, resp.Status.StatusCode, resp.Status.ErrorMsgs)
			j.setError(err)
			return
		} else {
			h.appendCommitInfos(commitInfo)
		}
	}()

	return true
}

func (h *tabletIngestBinlogHandler) handle() {
	log.Debugf("handle tablet ingest binlog, src tablet id: %d, dest tablet id: %d", h.srcTablet.Id, h.destTablet.Id)

	// all src replicas version > binlogVersion
	srcReplicas := make([]*ReplicaMeta, 0, h.srcTablet.ReplicaMetas.Len())
	h.srcTablet.ReplicaMetas.Scan(func(srcReplicaId int64, srcReplica *ReplicaMeta) bool {
		if srcReplica.Version >= h.binlogVersion {
			srcReplicas = append(srcReplicas, srcReplica)
		}
		return true
	})

	if len(srcReplicas) == 0 {
		h.ingestJob.setError(xerror.Errorf(xerror.Meta, "no src replica version > %d", h.binlogVersion))
		return
	}

	srcReplicaIndex := 0
	h.destTablet.ReplicaMetas.Scan(func(destReplicaId int64, destReplica *ReplicaMeta) bool {
		// round robbin
		srcReplica := srcReplicas[srcReplicaIndex%len(srcReplicas)]
		srcReplicaIndex++
		return h.handleReplica(srcReplica, destReplica)
	})
	h.wg.Wait()

	h.ingestJob.appendCommitInfos(h.CommitInfos()...)
}

type IngestContext struct {
	context.Context
	txnId        int64
	tableRecords []*record.TableRecord
	tableMapping map[int64]int64
}

func NewIngestContext(txnId int64, tableRecords []*record.TableRecord, tableMapping map[int64]int64) *IngestContext {
	return &IngestContext{
		Context:      context.Background(),
		txnId:        txnId,
		tableRecords: tableRecords,
		tableMapping: tableMapping,
	}
}

type IngestBinlogJob struct {
	ccrJob  *Job // ccr job
	factory *Factory

	tableMapping map[int64]int64
	srcMeta      IngestBinlogMetaer
	destMeta     IngestBinlogMetaer

	txnId        int64
	tableRecords []*record.TableRecord

	srcBackendMap  map[int64]*base.Backend
	destBackendMap map[int64]*base.Backend

	tabletIngestJobs []*tabletIngestBinlogHandler

	*commitInfosCollector

	err     error
	errLock sync.RWMutex

	wg sync.WaitGroup
}

func NewIngestBinlogJob(ctx context.Context, ccrJob *Job) (*IngestBinlogJob, error) {
	// convert ctx to IngestContext
	ingestCtx, ok := ctx.(*IngestContext)
	if !ok {
		return nil, xerror.Errorf(xerror.Normal, "invalid context type: %T", ctx)
	}

	return &IngestBinlogJob{
		ccrJob:  ccrJob,
		factory: ccrJob.factory,

		tableMapping: ingestCtx.tableMapping,
		txnId:        ingestCtx.txnId,
		tableRecords: ingestCtx.tableRecords,

		commitInfosCollector: newCommitInfosCollector(),
	}, nil
}

func (j *IngestBinlogJob) GetSrcBackend(srcBackendId int64) *base.Backend {
	srcBackend, ok := j.srcBackendMap[srcBackendId]
	if !ok {
		return nil
	}
	return srcBackend
}

func (j *IngestBinlogJob) GetDestBackend(destBackendId int64) *base.Backend {
	destBackend, ok := j.destBackendMap[destBackendId]
	if !ok {
		return nil
	}
	return destBackend
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
	log.Debugf("arg %+v", arg)
	srcTablets, err := j.srcMeta.GetTablets(arg.srcTableId, arg.srcPartitionId, arg.srcIndexMeta.Id)
	if err != nil {
		j.setError(err)
		return
	}

	destTablets, err := j.destMeta.GetTablets(arg.destTableId, arg.destPartitionId, arg.destIndexMeta.Id)
	if err != nil {
		j.setError(err)
		return
	}

	if srcTablets.Len() != destTablets.Len() {
		j.setError(xerror.Errorf(xerror.Meta, "src tablets length: %v not equal to dest tablets length: %v", srcTablets.Len(), destTablets.Len()))
		return
	}

	if srcTablets.Len() == 0 {
		log.Warn("src tablets length: 0, skip")
		return
	}

	srcIter := srcTablets.IterMut()
	if !srcIter.First() {
		j.setError(xerror.Errorf(xerror.Meta, "src tablets First() failed"))
		return
	}

	destIter := destTablets.IterMut()
	if !destIter.First() {
		j.setError(xerror.Errorf(xerror.Meta, "dest tablets First() failed"))
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

			commitInfosCollector: newCommitInfosCollector(),
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

	if len(indexIds) == 0 {
		j.setError(xerror.Errorf(xerror.Meta, "index ids is empty"))
		return
	}

	job := j.ccrJob

	srcPartitionId := partitionRecord.Id
	srcPartitionRange := partitionRecord.Range
	destPartitionId, err := j.destMeta.GetPartitionIdByRange(destTableId, srcPartitionRange)
	if err != nil {
		j.setError(err)
		return
	}

	// Step 1: check index id
	srcIndexIdMap, err := j.srcMeta.GetIndexIdMap(srcTableId, srcPartitionId)
	if err != nil {
		j.setError(err)
		return
	}
	destIndexNameMap, destBaseIndex, err := j.destMeta.GetIndexNameMap(destTableId, destPartitionId)
	if err != nil {
		j.setError(err)
		return
	}
	log.Infof("walter dest index name map: %v, src table id %d, part id %d, dest table id %d, part id %d",
		destIndexNameMap, srcTableId, srcPartitionId, destTableId, destPartitionId)

	getSrcIndexName := func(ccrJob *Job, srcIndexMeta *IndexMeta) string {
		srcIndexName := srcIndexMeta.Name
		if ccrJob.SyncType == TableSync && srcIndexName == ccrJob.Src.Table {
			return ccrJob.Dest.Table
		} else if srcIndexMeta.IsBaseIndex {
			return destBaseIndex.Name
		} else {
			return srcIndexName
		}
	}

	for _, indexId := range indexIds {
		if j.srcMeta.IsIndexDropped(indexId) {
			continue
		}
		if featureFilterShadowIndexesUpsert {
			if _, ok := j.ccrJob.progress.ShadowIndexes[indexId]; ok {
				continue
			}
		}
		srcIndexMeta, ok := srcIndexIdMap[indexId]
		if !ok {
			j.setError(xerror.Errorf(xerror.Meta, "index id %v not found in src meta", indexId))
			return
		}

		srcIndexName := getSrcIndexName(job, srcIndexMeta)
		log.Debugf("src idx id %d, name %s", indexId, srcIndexName)
		if _, ok := destIndexNameMap[srcIndexName]; !ok {
			j.setError(xerror.Errorf(xerror.Meta,
				"index name %v not found in dest meta, is base index: %t, src index id: %d",
				srcIndexName, srcIndexMeta.IsBaseIndex, indexId))
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
		if j.srcMeta.IsIndexDropped(indexId) {
			log.Infof("skip the dropped index %d", indexId)
			continue
		}
		if featureFilterShadowIndexesUpsert {
			if _, ok := j.ccrJob.progress.ShadowIndexes[indexId]; ok {
				log.Infof("skip the shadow index %d", indexId)
				continue
			}
		}

		srcIndexMeta := srcIndexIdMap[indexId]
		destIndexMeta := destIndexNameMap[getSrcIndexName(job, srcIndexMeta)]
		prepareIndexArg.srcIndexMeta = srcIndexMeta
		prepareIndexArg.destIndexMeta = destIndexMeta
		j.prepareIndex(&prepareIndexArg)
	}
}

func (j *IngestBinlogJob) prepareTable(tableRecord *record.TableRecord) {
	log.Debugf("tableRecord: %v", tableRecord)
	if j.srcMeta.IsTableDropped(tableRecord.Id) {
		log.Infof("skip the dropped table %d", tableRecord.Id)
		return
	}

	if len(tableRecord.PartitionRecords) == 0 {
		j.setError(xerror.Errorf(xerror.Meta, "partition records is empty"))
		return
	}

	job := j.ccrJob
	// TODO: check it before ingestBinlog
	var srcTableId int64
	var destTableId int64

	// TODO: maybe use defer to setError
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
		err = xerror.Panicf(xerror.Normal, "invalid sync type: %s", job.SyncType)
	}
	if err != nil {
		j.setError(err)
		return
	}

	// Step 1: check all partitions in partition records are in src/dest cluster
	srcPartitionMap, err := j.srcMeta.GetPartitionRangeMap(srcTableId)
	if err != nil {
		j.setError(err)
		return
	}
	destPartitionMap, err := j.destMeta.GetPartitionRangeMap(destTableId)
	if err != nil {
		j.setError(err)
		return
	}
	for _, partitionRecord := range tableRecord.PartitionRecords {
		if partitionRecord.IsTemp || j.srcMeta.IsPartitionDropped(partitionRecord.Id) {
			continue
		}
		rangeKey := partitionRecord.Range
		if _, ok := srcPartitionMap[rangeKey]; !ok {
			err = xerror.Errorf(xerror.Meta, "partition range: %v not in src cluster", rangeKey)
			j.setError(err)
			return
		}
		if _, ok := destPartitionMap[rangeKey]; !ok {
			err = xerror.Errorf(xerror.Meta, "partition range: %v not in dest cluster", rangeKey)
			j.setError(err)
			return
		}
	}

	// Step 2: prepare partitions
	for _, partitionRecord := range tableRecord.PartitionRecords {
		if partitionRecord.IsTemp {
			log.Debugf("skip ingest binlog to an temp partition, id: %d range: %s, version: %d",
				partitionRecord.Id, partitionRecord.Range, partitionRecord.Version)
			continue
		}
		if j.srcMeta.IsPartitionDropped(partitionRecord.Id) {
			log.Infof("skip the dropped partition %d, range: %s, version: %d",
				partitionRecord.Id, partitionRecord.Range, partitionRecord.Version)
			continue
		}
		j.preparePartition(srcTableId, destTableId, partitionRecord, tableRecord.IndexIds)
	}
}

func (j *IngestBinlogJob) prepareBackendMap() {
	log.Debug("prepareBackendMap")

	var err error
	j.srcBackendMap, err = j.srcMeta.GetBackendMap()
	if err != nil {
		j.setError(err)
		return
	}

	j.destBackendMap, err = j.destMeta.GetBackendMap()
	if err != nil {
		j.setError(err)
		return
	}
}

func (j *IngestBinlogJob) prepareTabletIngestJobs() {
	log.Debugf("prepareTabletIngestJobs, table length: %d", len(j.tableRecords))

	j.tabletIngestJobs = make([]*tabletIngestBinlogHandler, 0)
	for _, tableRecord := range j.tableRecords {
		j.prepareTable(tableRecord)
		if j.Error() != nil {
			return
		}
	}
}

func (j *IngestBinlogJob) runTabletIngestJobs() {
	log.Debugf("runTabletIngestJobs, job length: %d", len(j.tabletIngestJobs))

	for _, tabletIngestJob := range j.tabletIngestJobs {
		j.wg.Add(1)
		go func(tabletIngestJob *tabletIngestBinlogHandler) {
			tabletIngestJob.handle()
			j.wg.Done()
		}(tabletIngestJob)
	}
	j.wg.Wait()
}

func (j *IngestBinlogJob) prepareMeta() {
	log.Debug("prepareMeta")
	srcTableIds := make([]int64, 0, len(j.tableRecords))
	job := j.ccrJob
	factory := j.factory

	switch job.SyncType {
	case DBSync:
		for _, tableRecord := range j.tableRecords {
			srcTableIds = append(srcTableIds, tableRecord.Id)
		}
	case TableSync:
		srcTableIds = append(srcTableIds, job.Src.TableId)
	default:
		err := xerror.Panicf(xerror.Normal, "invalid sync type: %s", job.SyncType)
		j.setError(err)
		return
	}

	srcMeta, err := factory.NewThriftMeta(&job.Src, j.ccrJob.factory, srcTableIds)
	if err != nil {
		j.setError(err)
		return
	}

	destTableIds := make([]int64, 0, len(j.tableRecords))
	switch job.SyncType {
	case DBSync:
		for _, srcTableId := range srcTableIds {
			if destTableId, ok := j.tableMapping[srcTableId]; ok {
				destTableIds = append(destTableIds, destTableId)
			} else {
				err := xerror.XWrapf(errNotFoundDestMappingTableId, "src table id: %d", srcTableId)
				j.setError(err)
				return
			}
		}
	case TableSync:
		destTableIds = append(destTableIds, job.Dest.TableId)
	default:
		err := xerror.Panicf(xerror.Normal, "invalid sync type: %s", job.SyncType)
		j.setError(err)
		return
	}

	destMeta, err := factory.NewThriftMeta(&job.Dest, j.ccrJob.factory, destTableIds)
	if err != nil {
		j.setError(err)
		return
	}

	j.srcMeta = srcMeta
	j.destMeta = destMeta
}

// TODO(Drogon): use monad error handle
func (j *IngestBinlogJob) Run() {
	j.prepareMeta()
	if err := j.Error(); err != nil {
		return
	}

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
