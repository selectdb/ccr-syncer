package ccr

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/selectdb/ccr_syncer/ccr/base"
	"github.com/selectdb/ccr_syncer/rpc"
	bestruct "github.com/selectdb/ccr_syncer/rpc/kitex_gen/backendservice"
	festruct "github.com/selectdb/ccr_syncer/rpc/kitex_gen/frontendservice"
	"github.com/selectdb/ccr_syncer/rpc/kitex_gen/status"
	"github.com/selectdb/ccr_syncer/test_util"
	"github.com/selectdb/ccr_syncer/xerror"
	"github.com/tidwall/btree"
	"go.uber.org/mock/gomock"
)

const (
	backendBaseId = int64(0xdeadbeef * 10)
	tableBaseId   = int64(23330)
	dbBaseId      = int64(114514)
)

var (
	dbSrcSpec   base.Spec
	dbDestSpec  base.Spec
	tblSrcSpec  base.Spec
	tblDestSpec base.Spec
)

func init() {
	dbSrcSpec = base.Spec{
		Frontend: base.Frontend{
			Host:       "localhost",
			Port:       "9030",
			ThriftPort: "9020",
		},
		User:     "root",
		Password: "",
		Database: "src_db_case",
		DbId:     dbBaseId,
		Table:    "",
	}
	dbDestSpec = base.Spec{
		Frontend: base.Frontend{
			Host:       "localhost",
			Port:       "9030",
			ThriftPort: "9020",
		},
		User:     "root",
		Password: "",
		Database: "dest_db_case",
		DbId:     dbBaseId,
		Table:    "",
	}
	tblSrcSpec = base.Spec{
		Frontend: base.Frontend{
			Host:       "localhost",
			Port:       "9030",
			ThriftPort: "9020",
		},
		User:     "root",
		Password: "",
		Database: "src_tbl_case",
		DbId:     dbBaseId,
		Table:    fmt.Sprint(tableBaseId),
		TableId:  tableBaseId,
	}
	tblDestSpec = base.Spec{
		Frontend: base.Frontend{
			Host:       "localhost",
			Port:       "9030",
			ThriftPort: "9020",
		},
		User:     "root",
		Password: "",
		Database: "dest_tbl_case",
		DbId:     dbBaseId,
		Table:    fmt.Sprint(tableBaseId),
		TableId:  tableBaseId,
	}
}

func getPartitionBaseId(tableId int64) int64 {
	return tableId * 10
}

func getIndexBaseId(partitionId int64) int64 {
	return partitionId * 10
}

func getTabletBaseId(indexId int64) int64 {
	return indexId * 10
}

func getReplicaBaseId(indexId int64) int64 {
	return indexId * 100
}

type BinlogImpl struct {
	CommitSeq int64
	Timestamp int64
	Type      festruct.TBinlogType
	DbId      int64
}

func newTestBinlog(binlogType festruct.TBinlogType, tableIds []int64) *festruct.TBinlog {
	binlogImpl := BinlogImpl{
		CommitSeq: 114,
		Timestamp: 514,
		Type:      binlogType,
		DbId:      114514,
	}
	binlog := &festruct.TBinlog{
		CommitSeq: &binlogImpl.CommitSeq,
		Timestamp: &binlogImpl.Timestamp,
		Type:      &binlogImpl.Type,
		DbId:      &binlogImpl.DbId,
		TableIds:  tableIds,
	}

	return binlog
}

func newMeta(spec *base.Spec, backends *map[int64]*base.Backend) *DatabaseMeta {
	var tableIds []int64
	if spec.Table == "" {
		tableIds = make([]int64, 0, 3)
		for i := 0; i < 3; i++ {
			tableIds = append(tableIds, tableBaseId+int64(i))
		}
	} else {
		tableIds = make([]int64, 0, 1)
		tableIds = append(tableIds, spec.TableId)
	}

	dbMeta := newDatabaseMeta(spec.DbId)
	for _, tableId := range tableIds {
		tblMeta := newTableMeta(tableId)
		tblMeta.DatabaseMeta = dbMeta

		partitionId := getPartitionBaseId(tableId)
		partitionMeta := newPartitionMeta(partitionId)
		partitionMeta.TableMeta = tblMeta
		tblMeta.PartitionIdMap[partitionId] = partitionMeta
		tblMeta.PartitionRangeMap[fmt.Sprint(partitionId)] = partitionMeta

		indexId := getIndexBaseId(partitionId)
		indexMeta := newIndexMeta(indexId)
		indexMeta.PartitionMeta = partitionMeta
		partitionMeta.IndexIdMap[indexId] = indexMeta
		partitionMeta.IndexNameMap[indexMeta.Name] = indexMeta

		tabletId := getTabletBaseId(indexId)
		tabletMeta := newTabletMeta(tabletId)
		tabletMeta.IndexMeta = indexMeta
		tabletMeta.ReplicaMetas = indexMeta.ReplicaMetas
		indexMeta.TabletMetas.Set(tabletId, tabletMeta)

		replicaBaseId := getReplicaBaseId(indexId)
		backendNum := len(*backends)
		backendIds := make([]int64, 0, backendNum)
		for backendId := range *backends {
			backendIds = append(backendIds, backendId)
		}
		for i := 0; i < backendNum; i++ {
			replicaId := replicaBaseId + int64(i)
			replicaMeta := newReplicaMeta(replicaId)
			replicaMeta.TabletMeta = tabletMeta
			replicaMeta.TabletId = tabletId
			replicaMeta.BackendId = backendIds[replicaId%int64(backendNum)]
			indexMeta.ReplicaMetas.Set(replicaId, replicaMeta)
		}
		dbMeta.Tables[tableId] = tblMeta
	}

	return dbMeta
}

func newDatabaseMeta(dbId int64) *DatabaseMeta {
	return &DatabaseMeta{
		Id:     dbId,
		Tables: make(map[int64]*TableMeta),
	}
}

func newTableMeta(tableId int64) *TableMeta {
	return &TableMeta{
		Id:                tableId,
		Name:              fmt.Sprint(tableId),
		PartitionIdMap:    make(map[int64]*PartitionMeta),
		PartitionRangeMap: make(map[string]*PartitionMeta),
	}
}

func newPartitionMeta(partitionId int64) *PartitionMeta {
	return &PartitionMeta{
		Id:           partitionId,
		Name:         fmt.Sprint(partitionId),
		Key:          fmt.Sprint(partitionId),
		Range:        fmt.Sprint(partitionId),
		IndexIdMap:   make(map[int64]*IndexMeta),
		IndexNameMap: make(map[string]*IndexMeta),
	}
}

func newIndexMeta(indexId int64) *IndexMeta {
	return &IndexMeta{
		Id:           indexId,
		Name:         fmt.Sprint(indexId),
		TabletMetas:  btree.NewMap[int64, *TabletMeta](degree),
		ReplicaMetas: btree.NewMap[int64, *ReplicaMeta](degree),
	}
}

func newTabletMeta(tabletId int64) *TabletMeta {
	return &TabletMeta{
		Id: tabletId,
	}
}

func newReplicaMeta(replicaId int64) *ReplicaMeta {
	return &ReplicaMeta{
		Id: replicaId,
	}
}

func newBackendMap(backendNum int) map[int64]*base.Backend {
	backendMap := make(map[int64]*base.Backend)
	for i := 0; i < backendNum; i++ {
		backendId := backendBaseId + int64(i)
		backendMap[backendId] = &base.Backend{
			Id:            backendId,
			Host:          "localhost",
			HeartbeatPort: 0xbeef,
			BePort:        0xbeef,
			HttpPort:      0xbeef,
			BrpcPort:      0xbeef,
		}
	}

	return backendMap
}

type UpsertContext struct {
	context.Context
	CommitSeq   int64
	DbId        int64
	TableId     int64
	TxnId       int64
	Version     int64
	PartitionId int64
	IndexId     int64
	TabletId    int64
}

func newUpsertData(ctx context.Context) (string, error) {
	upsertContext, ok := ctx.(*UpsertContext)
	if !ok {
		return "", xerror.Errorf(xerror.Normal, "invalid context type: %T", ctx)
	}

	dataMap := make(map[string]interface{})
	dataMap["commitSeq"] = upsertContext.CommitSeq
	dataMap["txnId"] = upsertContext.TxnId
	dataMap["timeStamp"] = 514
	dataMap["label"] = "insert_cca56f22e3624ab2_90b6b4ac06b44360"
	dataMap["dbId"] = upsertContext.DbId
	tableMap := make(map[string]interface{})
	dataMap["tableRecords"] = tableMap

	recordMap := make(map[string]interface{})

	partitionRecords := make([]map[string]interface{}, 0, 1)
	partitionRecord := make(map[string]interface{})
	partitionRecord["partitionId"] = upsertContext.PartitionId
	partitionRecord["range"] = fmt.Sprint(upsertContext.PartitionId)
	partitionRecord["version"] = upsertContext.Version
	partitionRecords = append(partitionRecords, partitionRecord)
	recordMap["partitionRecords"] = partitionRecords

	indexRecords := make([]int64, 0, 1)
	indexRecords = append(indexRecords, upsertContext.IndexId)
	recordMap["indexIds"] = indexRecords

	tableMap[fmt.Sprint(upsertContext.TableId)] = recordMap

	if data, err := json.Marshal(dataMap); err != nil {
		return "", err
	} else {
		return string(data), nil
	}
}

func TestHandleUpsertInTableSync(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// init test data
	txnId := int64(114514233)
	commitSeq := int64(114233514)
	version := int64(233114514)
	srcPartitionId := getPartitionBaseId(tblSrcSpec.TableId)
	srcIndexId := getIndexBaseId(srcPartitionId)
	srcTabletId := getTabletBaseId(srcIndexId)
	destPartitionId := getPartitionBaseId(tblSrcSpec.TableId)
	destIndexId := getIndexBaseId(destPartitionId)
	destTabletId := getTabletBaseId(destIndexId)

	backendMap := newBackendMap(3)
	srcMeta := newMeta(&tblSrcSpec, &backendMap)
	destMeta := newMeta(&tblDestSpec, &backendMap)

	// init db_mock
	db := test_util.NewMockDB(ctrl)
	db.EXPECT().IsJobExist("Test").Return(false, nil)
	db.EXPECT().UpdateProgress("Test", gomock.Any()).DoAndReturn(
		func(_ string, progressJson string) error {
			var jobProgress JobProgress
			if err := json.Unmarshal([]byte(progressJson), &jobProgress); err != nil {
				t.Error(err)
			}
			// lse if jobProgress.TransactionId != txnId {
			// 	t.Errorf("UnExpect TransactionId %v, need %v", jobProgress.TransactionId, txnId)
			// }
			return nil
		})

	// init factory
	rpcFactory := NewMockIRpcFactory(ctrl)
	metaFactory := NewMockIMetaFactory(ctrl)
	factory := NewFactory(rpcFactory, metaFactory, base.NewISpecFactory())

	// init rpcFactory
	rpcFactory.EXPECT().NewFeRpc(&tblDestSpec).DoAndReturn(func(_ *base.Spec) (rpc.IFeRpc, error) {
		mockFeRpc := NewMockIFeRpc(ctrl)
		tableIds := make([]int64, 0, 1)
		tableIds = append(tableIds, tblDestSpec.TableId)
		mockFeRpc.EXPECT().BeginTransaction(&tblDestSpec, gomock.Any(), tableIds).Return(
			&festruct.TBeginTxnResult_{
				Status: &status.TStatus{
					StatusCode: status.TStatusCode_OK,
					ErrorMsgs:  nil,
				},
				TxnId:     &txnId,
				JobStatus: nil,
				DbId:      &tblDestSpec.DbId,
			}, nil)
		mockFeRpc.EXPECT().CommitTransaction(&tblDestSpec, txnId, gomock.Any()).Return(
			&festruct.TCommitTxnResult_{
				Status: &status.TStatus{
					StatusCode: status.TStatusCode_OK,
					ErrorMsgs:  nil,
				},
			}, nil)
		return mockFeRpc, nil
	})
	rpcFactory.EXPECT().NewBeRpc(gomock.Any()).DoAndReturn(func(_ *base.Backend) (rpc.IBeRpc, error) {
		mockBeRpc := NewMockIBeRpc(ctrl)
		mockBeRpc.EXPECT().IngestBinlog(gomock.Any()).DoAndReturn(
			func(req *bestruct.TIngestBinlogRequest) (*bestruct.TIngestBinlogResult_, error) {
				if req.GetTxnId() != txnId {
					t.Errorf("txnId is mismatch: %d, need %d", req.GetTxnId(), txnId)
				} else if req.GetRemoteTabletId() != srcTabletId {
					t.Errorf("remote tabletId mismatch: %d, need %d", req.GetRemoteTabletId(), srcTabletId)
				} else if req.GetBinlogVersion() != version {
					t.Errorf("version mismatch: %d, need %d", req.GetBinlogVersion(), version)
				} else if req.GetRemoteHost() != "localhost" {
					t.Errorf("remote host mismatch: %s, need localhost", req.GetRemoteHost())
				} else if req.GetPartitionId() != destPartitionId {
					t.Errorf("partitionId mismatch: %d, need %d", req.GetPartitionId(), destPartitionId)
				} else if req.GetLocalTabletId() != destTabletId {
					t.Errorf("local tabletId mismatch: %d, need %d", req.GetLocalTabletId(), destTabletId)
				}

				return &bestruct.TIngestBinlogResult_{
					Status: &status.TStatus{
						StatusCode: status.TStatusCode_OK,
						ErrorMsgs:  nil,
					},
				}, nil
			})

		return mockBeRpc, nil
	}).Times(3)

	// init metaFactory
	metaFactory.EXPECT().NewMeta(&tblSrcSpec).DoAndReturn(func(_ *base.Spec) Metaer {
		mockMeta := NewMockIMeta(ctrl)

		mockMeta.EXPECT().GetBackendMap().Return(backendMap, nil)
		mockMeta.EXPECT().GetPartitionRangeMap(tblSrcSpec.TableId).DoAndReturn(
			func(tableId int64) (map[string]*PartitionMeta, error) {
				return srcMeta.Tables[tableId].PartitionRangeMap, nil
			})
		mockMeta.EXPECT().GetIndexIdMap(tblSrcSpec.TableId, srcPartitionId).DoAndReturn(
			func(tableId int64, partitionId int64) (map[int64]*IndexMeta, error) {
				return srcMeta.Tables[tableId].PartitionIdMap[partitionId].IndexIdMap, nil
			})
		mockMeta.EXPECT().GetTablets(tblSrcSpec.TableId, srcPartitionId, srcIndexId).DoAndReturn(
			func(tableId int64, partitionId int64, indexId int64) (*btree.Map[int64, *TabletMeta], error) {
				return srcMeta.Tables[tableId].PartitionIdMap[partitionId].IndexIdMap[indexId].TabletMetas, nil
			})

		return mockMeta
	})
	metaFactory.EXPECT().NewMeta(&tblDestSpec).DoAndReturn(func(_ *base.Spec) Metaer {
		mockMeta := NewMockIMeta(ctrl)
		mockMeta.EXPECT().GetBackendMap().Return(backendMap, nil)
		mockMeta.EXPECT().GetPartitionRangeMap(tblDestSpec.TableId).DoAndReturn(
			func(tableId int64) (map[string]*PartitionMeta, error) {
				return destMeta.Tables[tableId].PartitionRangeMap, nil
			})
		mockMeta.EXPECT().GetPartitionIdByRange(tblDestSpec.TableId, fmt.Sprint(destPartitionId)).DoAndReturn(
			func(tableId int64, partitionRange string) (int64, error) {
				return destMeta.Tables[tableId].PartitionRangeMap[partitionRange].Id, nil
			})
		mockMeta.EXPECT().GetIndexNameMap(tblDestSpec.TableId, destPartitionId).DoAndReturn(
			func(tableId int64, partitionId int64) (map[string]*IndexMeta, error) {
				return destMeta.Tables[tableId].PartitionIdMap[partitionId].IndexNameMap, nil
			})
		mockMeta.EXPECT().GetTablets(tblDestSpec.TableId, destPartitionId, destIndexId).DoAndReturn(
			func(tableId int64, partitionId int64, indexId int64) (*btree.Map[int64, *TabletMeta], error) {
				return destMeta.Tables[tableId].PartitionIdMap[partitionId].IndexIdMap[indexId].TabletMetas, nil
			})
		return mockMeta
	})

	// init job
	ctx := NewJobContext(tblSrcSpec, tblDestSpec, db, factory)
	job, err := NewJobFromService("Test", ctx)
	if err != nil {
		t.Error(err)
	}
	job.progress = NewJobProgress("Test", job.SyncType, db)

	// init binlog
	tableIds := make([]int64, 0, 1)
	tableIds = append(tableIds, tblSrcSpec.TableId)
	binlog := newTestBinlog(festruct.TBinlogType_UPSERT, tableIds)
	upsertContext := &UpsertContext{
		Context:     context.Background(),
		CommitSeq:   commitSeq,
		DbId:        tblSrcSpec.DbId,
		TableId:     tblSrcSpec.TableId,
		TxnId:       txnId,
		Version:     version,
		PartitionId: srcPartitionId,
		IndexId:     srcIndexId,
		TabletId:    srcTabletId,
	}
	if data, err := newUpsertData(upsertContext); err != nil {
		t.Error(err)
	} else {
		binlog.SetData(&data)
	}

	if err := job.handleUpsert(binlog); err != nil {
		t.Error(err)
	}
}

func TestHandleUpsertInDbSync(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// init test data
	txnId := int64(114514233)
	commitSeq := int64(114233514)
	version := int64(233114514)
	srcPartitionId := getPartitionBaseId(tableBaseId)
	srcIndexId := getIndexBaseId(srcPartitionId)
	srcTabletId := getTabletBaseId(srcIndexId)
	destPartitionId := getPartitionBaseId(tableBaseId)
	destIndexId := getIndexBaseId(destPartitionId)
	destTabletId := getTabletBaseId(destIndexId)

	backendMap := newBackendMap(3)
	srcMeta := newMeta(&dbSrcSpec, &backendMap)
	destMeta := newMeta(&dbDestSpec, &backendMap)

	// init db_mock
	db := test_util.NewMockDB(ctrl)
	db.EXPECT().IsJobExist("Test").Return(false, nil)
	db.EXPECT().UpdateProgress("Test", gomock.Any()).DoAndReturn(
		func(_ string, progressJson string) error {
			var jobProgress JobProgress
			if err := json.Unmarshal([]byte(progressJson), &jobProgress); err != nil {
				t.Error(err)
			} else if jobProgress.TransactionId != txnId {
				t.Errorf("UnExpect TransactionId %v, need %v", jobProgress.TransactionId, txnId)
			}
			return nil
		})

	// init factory
	rpcFactory := NewMockIRpcFactory(ctrl)
	metaFactory := NewMockIMetaFactory(ctrl)
	factory := NewFactory(rpcFactory, metaFactory, base.NewISpecFactory())

	// init rpcFactory
	rpcFactory.EXPECT().NewFeRpc(&dbDestSpec).DoAndReturn(func(_ *base.Spec) (rpc.IFeRpc, error) {
		mockFeRpc := NewMockIFeRpc(ctrl)
		tableIds := make([]int64, 0, 1)
		tableIds = append(tableIds, tableBaseId)
		mockFeRpc.EXPECT().BeginTransaction(&dbDestSpec, gomock.Any(), tableIds).Return(
			&festruct.TBeginTxnResult_{
				Status: &status.TStatus{
					StatusCode: status.TStatusCode_OK,
					ErrorMsgs:  nil,
				},
				TxnId:     &txnId,
				JobStatus: nil,
				DbId:      &dbDestSpec.DbId,
			}, nil)
		mockFeRpc.EXPECT().CommitTransaction(&dbDestSpec, txnId, gomock.Any()).Return(
			&festruct.TCommitTxnResult_{
				Status: &status.TStatus{
					StatusCode: status.TStatusCode_OK,
					ErrorMsgs:  nil,
				},
			}, nil)
		return mockFeRpc, nil
	})
	rpcFactory.EXPECT().NewBeRpc(gomock.Any()).DoAndReturn(func(_ *base.Backend) (rpc.IBeRpc, error) {
		mockBeRpc := NewMockIBeRpc(ctrl)
		mockBeRpc.EXPECT().IngestBinlog(gomock.Any()).DoAndReturn(
			func(req *bestruct.TIngestBinlogRequest) (*bestruct.TIngestBinlogResult_, error) {
				if req.GetTxnId() != txnId {
					t.Errorf("txnId is mismatch: %d, need %d", req.GetTxnId(), txnId)
				} else if req.GetRemoteTabletId() != srcTabletId {
					t.Errorf("remote tabletId mismatch: %d, need %d", req.GetRemoteTabletId(), srcTabletId)
				} else if req.GetBinlogVersion() != version {
					t.Errorf("version mismatch: %d, need %d", req.GetBinlogVersion(), version)
				} else if req.GetRemoteHost() != "localhost" {
					t.Errorf("remote host mismatch: %s, need localhost", req.GetRemoteHost())
				} else if req.GetPartitionId() != destPartitionId {
					t.Errorf("partitionId mismatch: %d, need %d", req.GetPartitionId(), destPartitionId)
				} else if req.GetLocalTabletId() != destTabletId {
					t.Errorf("local tabletId mismatch: %d, need %d", req.GetLocalTabletId(), destTabletId)
				}

				return &bestruct.TIngestBinlogResult_{
					Status: &status.TStatus{
						StatusCode: status.TStatusCode_OK,
						ErrorMsgs:  nil,
					},
				}, nil
			})

		return mockBeRpc, nil
	}).Times(3)

	// init metaFactory
	metaFactory.EXPECT().NewMeta(&dbSrcSpec).DoAndReturn(func(_ *base.Spec) Metaer {
		mockMeta := NewMockIMeta(ctrl)

		mockMeta.EXPECT().GetBackendMap().Return(backendMap, nil)
		mockMeta.EXPECT().GetTableNameById(tableBaseId).Return(fmt.Sprint(tableBaseId), nil)
		mockMeta.EXPECT().GetPartitionRangeMap(tableBaseId).DoAndReturn(
			func(tableId int64) (map[string]*PartitionMeta, error) {
				return srcMeta.Tables[tableId].PartitionRangeMap, nil
			})
		mockMeta.EXPECT().GetIndexIdMap(tableBaseId, srcPartitionId).DoAndReturn(
			func(tableId int64, partitionId int64) (map[int64]*IndexMeta, error) {
				return srcMeta.Tables[tableId].PartitionIdMap[partitionId].IndexIdMap, nil
			})
		mockMeta.EXPECT().GetTablets(tableBaseId, srcPartitionId, srcIndexId).DoAndReturn(
			func(tableId int64, partitionId int64, indexId int64) (*btree.Map[int64, *TabletMeta], error) {
				return srcMeta.Tables[tableId].PartitionIdMap[partitionId].IndexIdMap[indexId].TabletMetas, nil
			})

		return mockMeta
	})
	metaFactory.EXPECT().NewMeta(&dbDestSpec).DoAndReturn(func(_ *base.Spec) Metaer {
		mockMeta := NewMockIMeta(ctrl)
		mockMeta.EXPECT().GetBackendMap().Return(backendMap, nil)
		mockMeta.EXPECT().GetTableId(fmt.Sprint(tableBaseId)).Return(tableBaseId, nil)
		mockMeta.EXPECT().GetPartitionRangeMap(tblDestSpec.TableId).DoAndReturn(
			func(tableId int64) (map[string]*PartitionMeta, error) {
				return destMeta.Tables[tableId].PartitionRangeMap, nil
			})
		mockMeta.EXPECT().GetPartitionIdByRange(tblDestSpec.TableId, fmt.Sprint(destPartitionId)).DoAndReturn(
			func(tableId int64, partitionRange string) (int64, error) {
				return destMeta.Tables[tableId].PartitionRangeMap[partitionRange].Id, nil
			})
		mockMeta.EXPECT().GetIndexNameMap(tblDestSpec.TableId, destPartitionId).DoAndReturn(
			func(tableId int64, partitionId int64) (map[string]*IndexMeta, error) {
				return destMeta.Tables[tableId].PartitionIdMap[partitionId].IndexNameMap, nil
			})
		mockMeta.EXPECT().GetTablets(tblDestSpec.TableId, destPartitionId, destIndexId).DoAndReturn(
			func(tableId int64, partitionId int64, indexId int64) (*btree.Map[int64, *TabletMeta], error) {
				return destMeta.Tables[tableId].PartitionIdMap[partitionId].IndexIdMap[indexId].TabletMetas, nil
			})
		return mockMeta
	})

	// init job
	ctx := NewJobContext(dbSrcSpec, dbDestSpec, db, factory)
	job, err := NewJobFromService("Test", ctx)
	if err != nil {
		t.Error(err)
	}
	job.progress = NewJobProgress("Test", job.SyncType, db)

	// init binlog
	tableIds := make([]int64, 0, 1)
	tableIds = append(tableIds, tableBaseId)
	binlog := newTestBinlog(festruct.TBinlogType_UPSERT, tableIds)
	upsertContext := &UpsertContext{
		Context:     context.Background(),
		CommitSeq:   commitSeq,
		DbId:        dbSrcSpec.DbId,
		TableId:     tableBaseId,
		TxnId:       txnId,
		Version:     version,
		PartitionId: srcPartitionId,
		IndexId:     srcIndexId,
		TabletId:    srcTabletId,
	}
	if data, err := newUpsertData(upsertContext); err != nil {
		t.Error(err)
	} else {
		binlog.SetData(&data)
	}

	if err := job.handleUpsert(binlog); err != nil {
		t.Error(err)
	}
}

func TestHandleAddPartitionInTableSync(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// init data
	testSql := "ADD PARTITION `zero_to_five` VALUES [(\"0\"), (\"5\"))(\"version_info\" \u003d \"1\");"

	// init db_mock
	db := test_util.NewMockDB(ctrl)
	db.EXPECT().IsJobExist("Test").Return(false, nil)

	// init factory
	iSpecFactory := NewMockISpecFactory(ctrl)
	factory := NewFactory(rpc.NewRpcFactory(), NewMetaFactory(), iSpecFactory)

	// init iSpecFactory
	iSpecFactory.EXPECT().NewISpec(&tblSrcSpec).DoAndReturn(func(_ *base.Spec) base.Specer {
		mockISpec := NewMockISpec(ctrl)
		mockISpec.EXPECT().Valid().Return(nil)
		return mockISpec
	})
	iSpecFactory.EXPECT().NewISpec(&tblDestSpec).DoAndReturn(func(_ *base.Spec) base.Specer {
		mockISpec := NewMockISpec(ctrl)
		fullSql := fmt.Sprintf("ALTER TABLE %s.%s %s", tblDestSpec.Database, tblDestSpec.Table, testSql)
		mockISpec.EXPECT().Exec(fullSql).Return(nil)
		mockISpec.EXPECT().Valid().Return(nil)
		return mockISpec
	})

	// init job
	ctx := NewJobContext(tblSrcSpec, tblDestSpec, db, factory)
	job, err := NewJobFromService("Test", ctx)
	if err != nil {
		t.Error(err)
	}

	// init binlog
	tableIds := make([]int64, 0, 1)
	tableIds = append(tableIds, tblSrcSpec.TableId)
	binlog := newTestBinlog(festruct.TBinlogType_ADD_PARTITION, tableIds)
	dataMap := make(map[string]interface{})
	dataMap["tableId"] = tblSrcSpec.TableId
	dataMap["sql"] = testSql
	if data, err := json.Marshal(dataMap); err != nil {
		t.Error(err)
	} else {
		dataStr := string(data)
		binlog.SetData(&dataStr)
	}

	// test begin
	if err := job.handleAddPartition(binlog); err != nil {
		t.Error(err)
	}
}

func TestHandleAddPartitionInDbSync(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// init data
	testSql := "ADD PARTITION `zero_to_five` VALUES [(\"0\"), (\"5\"))(\"version_info\" \u003d \"1\");"

	// init db_mock
	db := test_util.NewMockDB(ctrl)
	db.EXPECT().IsJobExist("Test").Return(false, nil)

	// init factory
	metaFactory := NewMockIMetaFactory(ctrl)
	iSpecFactory := NewMockISpecFactory(ctrl)
	factory := NewFactory(rpc.NewRpcFactory(), metaFactory, iSpecFactory)

	// init metaFactory
	metaFactory.EXPECT().NewMeta(&dbSrcSpec).Return(NewMockIMeta(ctrl))
	metaFactory.EXPECT().NewMeta(&dbDestSpec).DoAndReturn(func(_ *base.Spec) Metaer {
		mockMeta := NewMockIMeta(ctrl)
		mockMeta.EXPECT().GetTableNameById(tableBaseId).Return(fmt.Sprint(tableBaseId), nil)
		return mockMeta
	})

	// init iSpecFactory
	iSpecFactory.EXPECT().NewISpec(&dbSrcSpec).DoAndReturn(func(_ *base.Spec) base.Specer {
		mockISpec := NewMockISpec(ctrl)
		mockISpec.EXPECT().Valid().Return(nil)
		return mockISpec
	})
	iSpecFactory.EXPECT().NewISpec(&dbDestSpec).DoAndReturn(func(_ *base.Spec) base.Specer {
		mockISpec := NewMockISpec(ctrl)
		fullSql := fmt.Sprintf("ALTER TABLE %s.%s %s", dbDestSpec.Database, fmt.Sprint(tableBaseId), testSql)
		mockISpec.EXPECT().Exec(fullSql).Return(nil)
		mockISpec.EXPECT().Valid().Return(nil)
		return mockISpec
	})

	// init job
	ctx := NewJobContext(dbSrcSpec, dbDestSpec, db, factory)
	job, err := NewJobFromService("Test", ctx)
	if err != nil {
		t.Error(err)
	}

	// init binlog
	tableIds := make([]int64, 0, 1)
	tableIds = append(tableIds, tableBaseId)
	binlog := newTestBinlog(festruct.TBinlogType_ADD_PARTITION, tableIds)
	dataMap := make(map[string]interface{})
	dataMap["tableId"] = tableBaseId
	dataMap["sql"] = testSql
	if data, err := json.Marshal(dataMap); err != nil {
		t.Error(err)
	} else {
		dataStr := string(data)
		binlog.SetData(&dataStr)
	}

	// test begin
	if err := job.handleAddPartition(binlog); err != nil {
		t.Error(err)
	}
}

func TestHandleDropPartitionInTableSync(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// init data
	testSql := "DROP PARTITION `zero_to_five`"

	// init db_mock
	db := test_util.NewMockDB(ctrl)
	db.EXPECT().IsJobExist("Test").Return(false, nil)

	// init factory
	iSpecFactory := NewMockISpecFactory(ctrl)
	factory := NewFactory(rpc.NewRpcFactory(), NewMetaFactory(), iSpecFactory)

	// init iSpecFactory
	iSpecFactory.EXPECT().NewISpec(&tblSrcSpec).DoAndReturn(func(_ *base.Spec) base.Specer {
		mockISpec := NewMockISpec(ctrl)
		mockISpec.EXPECT().Valid().Return(nil)
		return mockISpec
	})
	iSpecFactory.EXPECT().NewISpec(&tblDestSpec).DoAndReturn(func(_ *base.Spec) base.Specer {
		mockISpec := NewMockISpec(ctrl)
		fullSql := fmt.Sprintf("ALTER TABLE %s.%s %s", tblDestSpec.Database, tblDestSpec.Table, testSql)
		mockISpec.EXPECT().Exec(fullSql).Return(nil)
		mockISpec.EXPECT().Valid().Return(nil)
		return mockISpec
	})

	// init job
	ctx := NewJobContext(tblSrcSpec, tblDestSpec, db, factory)
	job, err := NewJobFromService("Test", ctx)
	if err != nil {
		t.Error(err)
	}

	// init binlog
	tableIds := make([]int64, 0, 1)
	tableIds = append(tableIds, tblSrcSpec.TableId)
	binlog := newTestBinlog(festruct.TBinlogType_ADD_PARTITION, tableIds)
	dataMap := make(map[string]interface{})
	dataMap["tableId"] = tblSrcSpec.TableId
	dataMap["sql"] = testSql
	if data, err := json.Marshal(dataMap); err != nil {
		t.Error(err)
	} else {
		dataStr := string(data)
		binlog.SetData(&dataStr)
	}

	// test begin
	if err := job.handleDropPartition(binlog); err != nil {
		t.Error(err)
	}
}

func TestHandleDropPartitionInDbSync(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// init data
	testSql := "DROP PARTITION `zero_to_five`"

	// init db_mock
	db := test_util.NewMockDB(ctrl)
	db.EXPECT().IsJobExist("Test").Return(false, nil)

	// init factory
	metaFactory := NewMockIMetaFactory(ctrl)
	iSpecFactory := NewMockISpecFactory(ctrl)
	factory := NewFactory(rpc.NewRpcFactory(), metaFactory, iSpecFactory)

	// init metaFactory
	metaFactory.EXPECT().NewMeta(&dbSrcSpec).Return(NewMockIMeta(ctrl))
	metaFactory.EXPECT().NewMeta(&dbDestSpec).DoAndReturn(func(_ *base.Spec) Metaer {
		mockMeta := NewMockIMeta(ctrl)
		mockMeta.EXPECT().GetTableNameById(tableBaseId).Return(fmt.Sprint(tableBaseId), nil)
		return mockMeta
	})

	// init iSpecFactory
	iSpecFactory.EXPECT().NewISpec(&dbSrcSpec).DoAndReturn(func(_ *base.Spec) base.Specer {
		mockISpec := NewMockISpec(ctrl)
		mockISpec.EXPECT().Valid().Return(nil)
		return mockISpec
	})
	iSpecFactory.EXPECT().NewISpec(&dbDestSpec).DoAndReturn(func(_ *base.Spec) base.Specer {
		mockISpec := NewMockISpec(ctrl)
		fullSql := fmt.Sprintf("ALTER TABLE %s.%s %s", dbDestSpec.Database, fmt.Sprint(tableBaseId), testSql)
		mockISpec.EXPECT().Exec(fullSql).Return(nil)
		mockISpec.EXPECT().Valid().Return(nil)
		return mockISpec
	})

	// init job
	ctx := NewJobContext(dbSrcSpec, dbDestSpec, db, factory)
	job, err := NewJobFromService("Test", ctx)
	if err != nil {
		t.Error(err)
	}

	// init binlog
	tableIds := make([]int64, 0, 1)
	tableIds = append(tableIds, tableBaseId)
	binlog := newTestBinlog(festruct.TBinlogType_ADD_PARTITION, tableIds)
	dataMap := make(map[string]interface{})
	dataMap["tableId"] = tableBaseId
	dataMap["sql"] = testSql
	if data, err := json.Marshal(dataMap); err != nil {
		t.Error(err)
	} else {
		dataStr := string(data)
		binlog.SetData(&dataStr)
	}

	// test begin
	if err := job.handleAddPartition(binlog); err != nil {
		t.Error(err)
	}
}

func TestHandleCreateTable(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// init data
	testSql := "A CREATE TABLE SQL"

	// init db_mock
	db := test_util.NewMockDB(ctrl)
	db.EXPECT().IsJobExist("Test").Return(false, nil)

	// init factory
	metaFactory := NewMockIMetaFactory(ctrl)
	iSpecFactory := NewMockISpecFactory(ctrl)
	factory := NewFactory(rpc.NewRpcFactory(), metaFactory, iSpecFactory)

	// init metaFactory
	metaFactory.EXPECT().NewMeta(&dbSrcSpec).DoAndReturn(func(_ *base.Spec) Metaer {
		mockMeta := NewMockIMeta(ctrl)
		mockMeta.EXPECT().GetTables().Return(make(map[int64]*TableMeta), nil)
		return mockMeta
	})
	metaFactory.EXPECT().NewMeta(&dbDestSpec).DoAndReturn(func(_ *base.Spec) Metaer {
		mockMeta := NewMockIMeta(ctrl)
		mockMeta.EXPECT().GetTables().Return(make(map[int64]*TableMeta), nil)
		return mockMeta
	})

	// init iSpecFactory
	iSpecFactory.EXPECT().NewISpec(&dbSrcSpec).DoAndReturn(func(_ *base.Spec) base.Specer {
		mockISpec := NewMockISpec(ctrl)
		mockISpec.EXPECT().Valid().Return(nil)
		return mockISpec
	})
	iSpecFactory.EXPECT().NewISpec(&dbDestSpec).DoAndReturn(func(_ *base.Spec) base.Specer {
		mockISpec := NewMockISpec(ctrl)
		mockISpec.EXPECT().DbExec(testSql).Return(nil)
		mockISpec.EXPECT().Valid().Return(nil)
		return mockISpec
	})

	// init job
	ctx := NewJobContext(dbSrcSpec, dbDestSpec, db, factory)
	job, err := NewJobFromService("Test", ctx)
	if err != nil {
		t.Error(err)
	}

	// init binlog
	tableIds := make([]int64, 0, 1)
	tableIds = append(tableIds, tableBaseId)
	binlog := newTestBinlog(festruct.TBinlogType_ADD_PARTITION, tableIds)
	dataMap := make(map[string]interface{})
	dataMap["dbId"] = dbSrcSpec.DbId
	dataMap["tableId"] = tableBaseId
	dataMap["sql"] = testSql
	if data, err := json.Marshal(dataMap); err != nil {
		t.Error(err)
	} else {
		dataStr := string(data)
		binlog.SetData(&dataStr)
	}

	// test begin
	if err := job.handleCreateTable(binlog); err != nil {
		t.Error(err)
	}
}

func TestHandleDropTable(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// init data
	testSql := "A DROP TABLE SQL"

	// init db_mock
	db := test_util.NewMockDB(ctrl)
	db.EXPECT().IsJobExist("Test").Return(false, nil)

	// init factory
	metaFactory := NewMockIMetaFactory(ctrl)
	iSpecFactory := NewMockISpecFactory(ctrl)
	factory := NewFactory(rpc.NewRpcFactory(), metaFactory, iSpecFactory)

	// init metaFactory
	metaFactory.EXPECT().NewMeta(&dbSrcSpec).DoAndReturn(func(_ *base.Spec) Metaer {
		mockMeta := NewMockIMeta(ctrl)
		mockMeta.EXPECT().GetTables().Return(make(map[int64]*TableMeta), nil)
		return mockMeta
	})
	metaFactory.EXPECT().NewMeta(&dbDestSpec).DoAndReturn(func(_ *base.Spec) Metaer {
		mockMeta := NewMockIMeta(ctrl)
		mockMeta.EXPECT().GetTables().Return(make(map[int64]*TableMeta), nil)
		return mockMeta
	})

	// init iSpecFactory
	iSpecFactory.EXPECT().NewISpec(&dbSrcSpec).DoAndReturn(func(_ *base.Spec) base.Specer {
		mockISpec := NewMockISpec(ctrl)
		mockISpec.EXPECT().Valid().Return(nil)
		return mockISpec
	})
	iSpecFactory.EXPECT().NewISpec(&dbDestSpec).DoAndReturn(func(_ *base.Spec) base.Specer {
		mockISpec := NewMockISpec(ctrl)
		dropSql := fmt.Sprintf("DROP TABLE %v FORCE", tableBaseId)
		mockISpec.EXPECT().DbExec(dropSql).Return(nil)
		mockISpec.EXPECT().Valid().Return(nil)
		return mockISpec
	})

	// init job
	ctx := NewJobContext(dbSrcSpec, dbDestSpec, db, factory)
	job, err := NewJobFromService("Test", ctx)
	if err != nil {
		t.Error(err)
	}

	// init binlog
	tableIds := make([]int64, 0, 1)
	tableIds = append(tableIds, tableBaseId)
	binlog := newTestBinlog(festruct.TBinlogType_ADD_PARTITION, tableIds)
	dataMap := make(map[string]interface{})
	dataMap["dbId"] = dbSrcSpec.DbId
	dataMap["tableId"] = tableBaseId
	dataMap["tableName"] = fmt.Sprint(tableBaseId)
	dataMap["rawSql"] = testSql
	if data, err := json.Marshal(dataMap); err != nil {
		t.Error(err)
	} else {
		dataStr := string(data)
		binlog.SetData(&dataStr)
	}

	// test begin
	if err := job.handleDropTable(binlog); err != nil {
		t.Error(err)
	}
}

func TestHandleDummyInTableSync(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// init data
	dummyCommitSeq := int64(114514)

	// init db_mock
	db := test_util.NewMockDB(ctrl)
	db.EXPECT().IsJobExist("Test").Return(false, nil)
	db.EXPECT().UpdateProgress("Test", gomock.Any()).DoAndReturn(
		func(_ string, progressJson string) error {
			var jobProgress JobProgress
			if err := json.Unmarshal([]byte(progressJson), &jobProgress); err != nil {
				t.Error(err)
			} else if jobProgress.CommitSeq != dummyCommitSeq {
				t.Errorf("UnExpect CommitSeq %v, need %v", jobProgress.CommitSeq, dummyCommitSeq)
			} else if jobProgress.SyncState != TableFullSync {
				t.Errorf("UnExpect SyncState %v, need %v", jobProgress.SyncState, TableFullSync)
			} else if jobProgress.SubSyncState != BeginCreateSnapshot {
				t.Errorf("UnExpect SubSyncState %v, need %v", jobProgress.SubSyncState, BeginCreateSnapshot)
			}
			return nil
		})

	// init factory
	factory := NewFactory(rpc.NewRpcFactory(), NewMetaFactory(), base.NewISpecFactory())

	// init job
	ctx := NewJobContext(tblSrcSpec, tblDestSpec, db, factory)
	job, err := NewJobFromService("Test", ctx)
	if err != nil {
		t.Error(err)
	}
	job.progress = NewJobProgress("Test", job.SyncType, db)

	// init binlog
	binlog := newTestBinlog(festruct.TBinlogType_DUMMY, nil)
	binlog.SetCommitSeq(&dummyCommitSeq)

	// test begin
	if err := job.handleDummy(binlog); err != nil {
		t.Error(err)
	}
}

func TestHandleDummyInDbSync(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// init data
	dummyCommitSeq := int64(114514)

	// init db_mock
	db := test_util.NewMockDB(ctrl)
	db.EXPECT().IsJobExist("Test").Return(false, nil)
	db.EXPECT().UpdateProgress("Test", gomock.Any()).DoAndReturn(
		func(_ string, progressJson string) error {
			var jobProgress JobProgress
			if err := json.Unmarshal([]byte(progressJson), &jobProgress); err != nil {
				t.Error(err)
			} else if jobProgress.CommitSeq != dummyCommitSeq {
				t.Errorf("UnExpect CommitSeq %v, need %v", jobProgress.CommitSeq, dummyCommitSeq)
			} else if jobProgress.SyncState != DBFullSync {
				t.Errorf("UnExpect SyncState %v, need %v", jobProgress.SyncState, DBFullSync)
			} else if jobProgress.SubSyncState != BeginCreateSnapshot {
				t.Errorf("UnExpect SubSyncState %v, need %v", jobProgress.SubSyncState, BeginCreateSnapshot)
			}
			return nil
		})

	// init factory
	factory := NewFactory(rpc.NewRpcFactory(), NewMetaFactory(), base.NewISpecFactory())

	// init job
	ctx := NewJobContext(dbSrcSpec, dbDestSpec, db, factory)
	job, err := NewJobFromService("Test", ctx)
	if err != nil {
		t.Error(err)
	}
	job.progress = NewJobProgress("Test", job.SyncType, db)

	// init binlog
	binlog := newTestBinlog(festruct.TBinlogType_DUMMY, nil)
	binlog.SetCommitSeq(&dummyCommitSeq)

	// test begin
	if err := job.handleDummy(binlog); err != nil {
		t.Error(err)
	}
}

func TestHandleAlterJobInTableSync(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// init data
	commitSeq := int64(233114514)
	alterType := "UNUSED_TYPE"
	jobId := int64(114514233)
	jobState := "FINISHED"
	rawSql := "A blank SQL"

	// init db_mock
	db := test_util.NewMockDB(ctrl)
	db.EXPECT().IsJobExist("Test").Return(false, nil)
	db.EXPECT().UpdateProgress("Test", gomock.Any()).DoAndReturn(
		func(_ string, progressJson string) error {
			var jobProgress JobProgress
			if err := json.Unmarshal([]byte(progressJson), &jobProgress); err != nil {
				t.Error(err)
			} else if jobProgress.CommitSeq != commitSeq {
				t.Errorf("UnExpect CommitSeq %v, need %v", jobProgress.CommitSeq, commitSeq)
			} else if jobProgress.SyncState != TableFullSync {
				t.Errorf("UnExpect SyncState %v, need %v", jobProgress.SyncState, TableFullSync)
			} else if jobProgress.SubSyncState != BeginCreateSnapshot {
				t.Errorf("UnExpect SubSyncState %v, need %v", jobProgress.SubSyncState, BeginCreateSnapshot)
			}
			return nil
		})

	// init factory
	metaFactory := NewMockIMetaFactory(ctrl)
	factory := NewFactory(rpc.NewRpcFactory(), metaFactory, base.NewISpecFactory())

	// init metaFactory
	metaFactory.EXPECT().NewMeta(&tblSrcSpec).Return(NewMockIMeta(ctrl))
	metaFactory.EXPECT().NewMeta(&tblDestSpec).DoAndReturn(func(_ *base.Spec) Metaer {
		mockMeta := NewMockIMeta(ctrl)
		dropSql := fmt.Sprintf("DROP TABLE %s FORCE", tblDestSpec.Table)
		mockMeta.EXPECT().DbExec(dropSql).Return(nil)
		return mockMeta
	})

	// init job
	ctx := NewJobContext(tblSrcSpec, tblDestSpec, db, factory)
	job, err := NewJobFromService("Test", ctx)
	if err != nil {
		t.Error(err)
	}
	job.progress = NewJobProgress("Test", job.SyncType, db)
	job.progress.CommitSeq = commitSeq

	// init binlog
	binlog := newTestBinlog(festruct.TBinlogType_ALTER_JOB, nil)
	dataMap := make(map[string]interface{})
	dataMap["type"] = alterType
	dataMap["dbId"] = tblSrcSpec.DbId
	dataMap["tableId"] = tblSrcSpec.TableId
	dataMap["tableName"] = fmt.Sprint(tblSrcSpec.Table)
	dataMap["jobId"] = jobId
	dataMap["jobState"] = jobState
	dataMap["rawSql"] = rawSql
	if data, err := json.Marshal(dataMap); err != nil {
		t.Error(err)
	} else {
		dataStr := string(data)
		binlog.SetData(&dataStr)
	}

	// test begin
	if err := job.handleAlterJob(binlog); err != nil {
		t.Error(err)
	}
}

func TestHandleAlterJobInDbSync(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// init data
	commitSeq := int64(233114514)
	alterType := "UNUSED_TYPE"
	jobId := int64(114514233)
	jobState := "FINISHED"
	rawSql := "A blank SQL"

	// init db_mock
	db := test_util.NewMockDB(ctrl)
	db.EXPECT().IsJobExist("Test").Return(false, nil)
	db.EXPECT().UpdateProgress("Test", gomock.Any()).DoAndReturn(
		func(_ string, progressJson string) error {
			var jobProgress JobProgress
			if err := json.Unmarshal([]byte(progressJson), &jobProgress); err != nil {
				t.Error(err)
			} else if jobProgress.CommitSeq != commitSeq {
				t.Errorf("UnExpect CommitSeq %v, need %v", jobProgress.CommitSeq, commitSeq)
			} else if jobProgress.SyncState != DBFullSync {
				t.Errorf("UnExpect SyncState %v, need %v", jobProgress.SyncState, DBFullSync)
			} else if jobProgress.SubSyncState != BeginCreateSnapshot {
				t.Errorf("UnExpect SubSyncState %v, need %v", jobProgress.SubSyncState, BeginCreateSnapshot)
			}
			return nil
		})

	// init factory
	metaFactory := NewMockIMetaFactory(ctrl)
	factory := NewFactory(rpc.NewRpcFactory(), metaFactory, base.NewISpecFactory())

	// init metaFactory
	metaFactory.EXPECT().NewMeta(&dbSrcSpec).Return(NewMockIMeta(ctrl))
	metaFactory.EXPECT().NewMeta(&dbDestSpec).DoAndReturn(func(_ *base.Spec) Metaer {
		mockMeta := NewMockIMeta(ctrl)
		dropSql := fmt.Sprintf("DROP TABLE %s FORCE", fmt.Sprint(tableBaseId))
		mockMeta.EXPECT().DbExec(dropSql).Return(nil)
		return mockMeta
	})

	// init job
	ctx := NewJobContext(dbSrcSpec, dbDestSpec, db, factory)
	job, err := NewJobFromService("Test", ctx)
	if err != nil {
		t.Error(err)
	}
	job.progress = NewJobProgress("Test", job.SyncType, db)
	job.progress.CommitSeq = commitSeq

	// init binlog
	binlog := newTestBinlog(festruct.TBinlogType_ALTER_JOB, nil)
	dataMap := make(map[string]interface{})
	dataMap["type"] = alterType
	dataMap["dbId"] = dbSrcSpec.DbId
	dataMap["tableId"] = tableBaseId
	dataMap["tableName"] = fmt.Sprint(tableBaseId)
	dataMap["jobId"] = jobId
	dataMap["jobState"] = jobState
	dataMap["rawSql"] = rawSql
	if data, err := json.Marshal(dataMap); err != nil {
		t.Error(err)
	} else {
		dataStr := string(data)
		binlog.SetData(&dataStr)
	}

	// test begin
	if err := job.handleAlterJob(binlog); err != nil {
		t.Error(err)
	}
}

func TestHandleLightningSchemaChange(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// init data
	testSql := fmt.Sprintf("`default_cluster:%s`.`%s` a test sql", tblSrcSpec.Database, tblSrcSpec.Table)

	// init db_mock
	db := test_util.NewMockDB(ctrl)
	db.EXPECT().IsJobExist("Test").Return(false, nil)

	// init factory
	iSpecFactory := NewMockISpecFactory(ctrl)
	factory := NewFactory(rpc.NewRpcFactory(), NewMetaFactory(), iSpecFactory)

	// init iSpecFactory
	iSpecFactory.EXPECT().NewISpec(&tblSrcSpec).DoAndReturn(func(_ *base.Spec) base.Specer {
		mockISpec := NewMockISpec(ctrl)
		mockISpec.EXPECT().Valid().Return(nil)
		return mockISpec
	})
	iSpecFactory.EXPECT().NewISpec(&tblDestSpec).DoAndReturn(func(_ *base.Spec) base.Specer {
		mockISpec := NewMockISpec(ctrl)
		execSql := fmt.Sprintf("`%s` a test sql", tblSrcSpec.Table)
		mockISpec.EXPECT().DbExec(execSql).Return(nil)
		mockISpec.EXPECT().Valid().Return(nil)
		return mockISpec
	})

	// init job
	ctx := NewJobContext(tblSrcSpec, tblDestSpec, db, factory)
	job, err := NewJobFromService("Test", ctx)
	if err != nil {
		t.Error(err)
	}
	job.progress = NewJobProgress("Test", job.SyncType, db)

	// init binlog
	binlog := newTestBinlog(festruct.TBinlogType_MODIFY_TABLE_ADD_OR_DROP_COLUMNS, nil)
	dataMap := make(map[string]interface{})
	dataMap["dbId"] = tblSrcSpec.DbId
	dataMap["tableId"] = tblSrcSpec.TableId
	dataMap["rawSql"] = testSql
	if data, err := json.Marshal(dataMap); err != nil {
		t.Error(err)
	} else {
		dataStr := string(data)
		binlog.SetData(&dataStr)
	}

	// test begin
	if err := job.handleLightningSchemaChange(binlog); err != nil {
		t.Error(err)
	}
}
