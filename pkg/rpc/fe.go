package rpc

import (
	"context"
	"fmt"
	"strconv"

	"github.com/cloudwego/kitex/client"
	"github.com/selectdb/ccr_syncer/pkg/ccr/base"
	festruct "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/frontendservice"
	feservice "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/frontendservice/frontendservice"
	tstatus "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/status"
	festruct_types "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/types"
	"github.com/selectdb/ccr_syncer/pkg/xerror"

	log "github.com/sirupsen/logrus"
)

const (
	LOCAL_REPO_NAME = ""
)

var (
	ErrFeNotMasterCompatible = xerror.XNew(xerror.FE, "not master compatible")
)

type IFeRpc interface {
	BeginTransaction(*base.Spec, string, []int64) (*festruct.TBeginTxnResult_, error)
	CommitTransaction(*base.Spec, int64, []*festruct_types.TTabletCommitInfo) (*festruct.TCommitTxnResult_, error)
	RollbackTransaction(spec *base.Spec, txnId int64) (*festruct.TRollbackTxnResult_, error)
	GetBinlog(*base.Spec, int64) (*festruct.TGetBinlogResult_, error)
	GetBinlogLag(*base.Spec, int64) (*festruct.TGetBinlogLagResult_, error)
	GetSnapshot(*base.Spec, string) (*festruct.TGetSnapshotResult_, error)
	RestoreSnapshot(*base.Spec, []*festruct.TTableRef, string, *festruct.TGetSnapshotResult_) (*festruct.TRestoreSnapshotResult_, error)
	GetMasterToken(*base.Spec) (string, error)
}

// TODO(Drogon): Add addrs to cached all spec clients
// now only cached master client, so callWithRetryAllClients only try with master clients(maybe not master now)
type FeRpc struct {
	masterClient *singleFeClient
	clients      map[string]*singleFeClient
}

func NewFeRpc(spec *base.Spec) (*FeRpc, error) {
	host := spec.Host
	// convert string to int32
	port, err := strconv.Atoi(spec.ThriftPort)
	if err != nil {
		return nil, xerror.Wrapf(err, xerror.RPC, "NewFeClient error by convert port %s to int32", spec.ThriftPort)
	}

	addr := fmt.Sprintf("%s:%d", host, port)
	client, err := newSingleFeClient(addr)
	if err != nil {
		return nil, xerror.Wrapf(err, xerror.RPC, "NewFeClient error: %v", err)
	}

	clients := make(map[string]*singleFeClient)
	clients[client.Address()] = client
	return &FeRpc{
		masterClient: client,
		clients:      clients,
	}, nil
}

type resultType interface {
	GetStatus() *tstatus.TStatus
	IsSetMasterAddress() bool
	GetMasterAddress() *festruct_types.TNetworkAddress
}
type callerType func(client *singleFeClient) (resultType, error)

func (rpc *FeRpc) callWithMasterRedirect(caller callerType) (resultType, error) {
	result, err := caller(rpc.masterClient)
	if err != nil {
		return result, err
	}

	if result.GetStatus().GetStatusCode() != tstatus.TStatusCode_NOT_MASTER {
		return result, err
	}

	// no compatible for master
	if !result.IsSetMasterAddress() {
		return result, xerror.XPanicWrapf(ErrFeNotMasterCompatible, "fe addr [%s]", rpc.masterClient.Address())
	}

	// switch to master
	masterAddr := result.GetMasterAddress()
	log.Infof("switch to master %s", masterAddr)
	var masterClient *singleFeClient
	addr := fmt.Sprintf("%s:%d", masterAddr.Hostname, masterAddr.Port)
	if client, ok := rpc.clients[addr]; ok {
		masterClient = client
	} else {
		masterClient, err = newSingleFeClient(addr)
		if err != nil {
			return nil, xerror.Wrapf(err, xerror.RPC, "NewFeClient error: %v", err)
		}
	}
	rpc.masterClient = masterClient
	rpc.clients[addr] = masterClient

	// retry
	return caller(rpc.masterClient)
}

type retryCallerType func(client *singleFeClient) (any, error)

func (rpc *FeRpc) callWithRetryAllClients(caller retryCallerType) (result any, err error) {
	client := rpc.masterClient
	if result, err = caller(client); err == nil {
		return result, nil
	}

	usedClientAddrs := make(map[string]bool)
	usedClientAddrs[client.Address()] = true
	for addr, client := range rpc.clients {
		if _, ok := usedClientAddrs[addr]; ok {
			continue
		}

		usedClientAddrs[addr] = true
		if result, err = caller(client); err == nil {
			return result, nil
		}
	}
	return result, err
}

func (rpc *FeRpc) BeginTransaction(spec *base.Spec, label string, tableIds []int64) (*festruct.TBeginTxnResult_, error) {
	// return rpc.masterClient.BeginTransaction(spec, label, tableIds)
	caller := func(client *singleFeClient) (resultType, error) {
		return client.BeginTransaction(spec, label, tableIds)
	}
	result, err := rpc.callWithMasterRedirect(caller)
	return result.(*festruct.TBeginTxnResult_), err
}

func (rpc *FeRpc) CommitTransaction(spec *base.Spec, txnId int64, commitInfos []*festruct_types.TTabletCommitInfo) (*festruct.TCommitTxnResult_, error) {
	// return rpc.masterClient.CommitTransaction(spec, txnId, commitInfos)
	caller := func(client *singleFeClient) (resultType, error) {
		return client.CommitTransaction(spec, txnId, commitInfos)
	}
	result, err := rpc.callWithMasterRedirect(caller)
	return result.(*festruct.TCommitTxnResult_), err
}

func (rpc *FeRpc) RollbackTransaction(spec *base.Spec, txnId int64) (*festruct.TRollbackTxnResult_, error) {
	// return rpc.masterClient.RollbackTransaction(spec, txnId)
	caller := func(client *singleFeClient) (resultType, error) {
		return client.RollbackTransaction(spec, txnId)
	}
	result, err := rpc.callWithMasterRedirect(caller)
	return result.(*festruct.TRollbackTxnResult_), err
}

func (rpc *FeRpc) GetBinlog(spec *base.Spec, commitSeq int64) (*festruct.TGetBinlogResult_, error) {
	// return rpc.masterClient.GetBinlog(spec, commitSeq)
	caller := func(client *singleFeClient) (any, error) {
		return client.GetBinlog(spec, commitSeq)
	}
	result, err := rpc.callWithRetryAllClients(caller)
	return result.(*festruct.TGetBinlogResult_), err
}

func (rpc *FeRpc) GetBinlogLag(spec *base.Spec, commitSeq int64) (*festruct.TGetBinlogLagResult_, error) {
	// return rpc.masterClient.GetBinlogLag(spec, commitSeq)
	caller := func(client *singleFeClient) (any, error) {
		return client.GetBinlogLag(spec, commitSeq)
	}
	result, err := rpc.callWithRetryAllClients(caller)
	return result.(*festruct.TGetBinlogLagResult_), err
}

func (rpc *FeRpc) GetSnapshot(spec *base.Spec, labelName string) (*festruct.TGetSnapshotResult_, error) {
	// return rpc.masterClient.GetSnapshot(spec, labelName)
	caller := func(client *singleFeClient) (resultType, error) {
		return client.GetSnapshot(spec, labelName)
	}
	result, err := rpc.callWithMasterRedirect(caller)
	return result.(*festruct.TGetSnapshotResult_), err
}

func (rpc *FeRpc) RestoreSnapshot(spec *base.Spec, tableRefs []*festruct.TTableRef, label string, snapshotResult *festruct.TGetSnapshotResult_) (*festruct.TRestoreSnapshotResult_, error) {
	// return rpc.masterClient.RestoreSnapshot(spec, tableRefs, label, snapshotResult)
	caller := func(client *singleFeClient) (resultType, error) {
		return client.RestoreSnapshot(spec, tableRefs, label, snapshotResult)
	}
	result, err := rpc.callWithMasterRedirect(caller)
	return result.(*festruct.TRestoreSnapshotResult_), err
}

func (rpc *FeRpc) GetMasterToken(spec *base.Spec) (string, error) {
	return rpc.masterClient.GetMasterToken(spec)
}

type Request interface {
	SetUser(*string)
	SetPasswd(*string)
	SetDb(*string)
}

// set auth info from spec
func setAuthInfo[T Request](request T, spec *base.Spec) {
	// set auth info
	request.SetUser(&spec.User)
	request.SetPasswd(&spec.Password)
	request.SetDb(&spec.Database)
}

type singleFeClient struct {
	addr   string
	client feservice.Client
}

func newSingleFeClient(addr string) (*singleFeClient, error) {
	// create kitex FrontendService client
	if fe_client, err := feservice.NewClient("FrontendService", client.WithHostPorts(addr)); err != nil {
		return nil, xerror.Wrapf(err, xerror.RPC, "NewFeClient error: %v, addr: %s", err, addr)
	} else {
		return &singleFeClient{
			addr:   addr,
			client: fe_client,
		}, nil
	}
}

func (rpc *singleFeClient) Address() string {
	return rpc.addr
}

// begin transaction
//
//	struct TBeginTxnRequest {
//	    1: optional string cluster
//	    2: required string user
//	    3: required string passwd
//	    4: required string db
//	    5: required list<string> tables
//	    6: optional string user_ip
//	    7: required string label
//	    8: optional i64 auth_code
//	    // The real value of timeout should be i32. i64 ensures the compatibility of interface.
//	    9: optional i64 timeout
//	    10: optional Types.TUniqueId request_id
//	    11: optional string token
//	}
func (rpc *singleFeClient) BeginTransaction(spec *base.Spec, label string, tableIds []int64) (*festruct.TBeginTxnResult_, error) {
	log.Debugf("BeginTransaction spec: %s, label: %s, tableIds: %v", spec, label, tableIds)

	client := rpc.client
	req := &festruct.TBeginTxnRequest{
		Label: &label,
	}
	setAuthInfo(req, spec)
	req.TableIds = tableIds

	log.Debugf("BeginTransaction user %s, label: %s, tableIds: %v", req.GetUser(), label, tableIds)
	if result, err := client.BeginTxn(context.Background(), req); err != nil {
		return nil, xerror.Wrapf(err, xerror.RPC, "BeginTransaction error: %v, req: %+v", err, req)
	} else {
		return result, nil
	}
}

//	struct TCommitTxnRequest {
//	    1: optional string cluster
//	    2: required string user
//	    3: required string passwd
//	    4: required string db
//	    5: optional string user_ip
//	    6: required i64 txnId
//	    7: optional list<Types.TTabletCommitInfo> commitInfos
//	    8: optional i64 auth_code
//	    9: optional TTxnCommitAttachment txnCommitAttachment
//	    10: optional i64 thrift_rpc_timeout_ms
//	    11: optional string token
//	    12: optional i64 db_id
//	}
func (rpc *singleFeClient) CommitTransaction(spec *base.Spec, txnId int64, commitInfos []*festruct_types.TTabletCommitInfo) (*festruct.TCommitTxnResult_, error) {
	log.Debugf("CommitTransaction spec: %s, txnId: %d, commitInfos: %v", spec, txnId, commitInfos)

	client := rpc.client
	req := &festruct.TCommitTxnRequest{}
	setAuthInfo(req, spec)
	req.TxnId = &txnId
	req.CommitInfos = commitInfos

	if result, err := client.CommitTxn(context.Background(), req); err != nil {
		return nil, xerror.Wrapf(err, xerror.RPC, "CommitTransaction error: %v, req: %+v", err, req)
	} else {
		return result, nil
	}
}

//	struct TRollbackTxnRequest {
//	    1: optional string cluster
//	    2: optional string user
//	    3: optional string passwd
//	    4: optional string db
//	    5: optional string user_ip
//	    6: optional i64 txn_id
//	    7: optional string reason
//	    9: optional i64 auth_code
//	    10: optional TTxnCommitAttachment txn_commit_attachment
//	    11: optional string token
//	    12: optional i64 db_id
//	}
func (rpc *singleFeClient) RollbackTransaction(spec *base.Spec, txnId int64) (*festruct.TRollbackTxnResult_, error) {
	log.Debugf("RollbackTransaction spec: %s, txnId: %d", spec, txnId)

	client := rpc.client
	req := &festruct.TRollbackTxnRequest{}
	setAuthInfo(req, spec)
	req.TxnId = &txnId

	if result, err := client.RollbackTxn(context.Background(), req); err != nil {
		return nil, xerror.Wrapf(err, xerror.RPC, "RollbackTransaction error: %v, req: %+v", err, req)
	} else {
		return result, nil
	}
}

//	struct TGetBinlogRequest {
//	    1: optional string cluster
//	    2: required string user
//	    3: required string passwd
//	    4: required string db
//	    5: optional string table
//	    6: optional string user_ip
//	    7: optional string token
//	    8: required i64 prev_commit_seq
//	}
func (rpc *singleFeClient) GetBinlog(spec *base.Spec, commitSeq int64) (*festruct.TGetBinlogResult_, error) {
	log.Debugf("GetBinlog, spec: %s, commit seq: %d", spec, commitSeq)

	client := rpc.client
	req := &festruct.TGetBinlogRequest{
		PrevCommitSeq: &commitSeq,
	}
	setAuthInfo(req, spec)

	if spec.Table != "" {
		req.Table = &spec.Table
		if spec.TableId != 0 {
			req.TableId = &spec.TableId
		}
	}

	log.Debugf("GetBinlog user %s, db %s, tableId %d, prev seq: %d", req.GetUser(), req.GetDb(),
		req.GetTableId(), req.GetPrevCommitSeq())
	if resp, err := client.GetBinlog(context.Background(), req); err != nil {
		return nil, xerror.Wrapf(err, xerror.RPC, "GetBinlog error: %v, req: %+v", err, req)
	} else {
		return resp, nil
	}
}

func (rpc *singleFeClient) GetBinlogLag(spec *base.Spec, commitSeq int64) (*festruct.TGetBinlogLagResult_, error) {
	log.Debugf("GetBinlogLag, spec: %s, commit seq: %d", spec, commitSeq)

	client := rpc.client
	req := &festruct.TGetBinlogRequest{
		PrevCommitSeq: &commitSeq,
	}
	setAuthInfo(req, spec)

	if spec.Table != "" {
		req.Table = &spec.Table
		req.TableId = &spec.TableId
		if spec.TableId != 0 {
			req.TableId = &spec.TableId
		}
	}

	log.Debugf("GetBinlog user %s, db %s, tableId %d, prev seq: %d", req.GetUser(), req.GetDb(),
		req.GetTableId(), req.GetPrevCommitSeq())
	if resp, err := client.GetBinlogLag(context.Background(), req); err != nil {
		return nil, xerror.Wrapf(err, xerror.RPC, "GetBinlogLag error: %v, req: %+v", err, req)
	} else {
		return resp, nil
	}
}

//	struct TGetSnapshotRequest {
//	    1: optional string cluster
//	    2: optional string user
//	    3: optional string passwd
//	    4: optional string db
//	    5: optional string table
//	    6: optional string token
//	    7: optional string label_name
//	    8: optional string snapshot_name
//	    9: optional TSnapshotType snapshot_type
//	}
func (rpc *singleFeClient) GetSnapshot(spec *base.Spec, labelName string) (*festruct.TGetSnapshotResult_, error) {
	log.Debugf("GetSnapshot %s, spec: %s", labelName, spec)

	client := rpc.client
	snapshotType := festruct.TSnapshotType_LOCAL
	snapshotName := ""
	req := &festruct.TGetSnapshotRequest{
		Table:        &spec.Table,
		LabelName:    &labelName,
		SnapshotType: &snapshotType,
		SnapshotName: &snapshotName,
	}
	setAuthInfo(req, spec)

	log.Debugf("GetSnapshotRequest user %s, db %s, table %s, label name %s, snapshot name %s, snapshot type %d",
		req.GetUser(), req.GetDb(), req.GetTable(), req.GetLabelName(), req.GetSnapshotName(), req.GetSnapshotType())
	if resp, err := client.GetSnapshot(context.Background(), req); err != nil {
		return nil, xerror.Wrapf(err, xerror.RPC, "GetSnapshot error: %v, req: %+v", err, req)
	} else {
		return resp, nil
	}
}

//	struct TRestoreSnapshotRequest {
//	    1: optional string cluster
//	    2: optional string user
//	    3: optional string passwd
//	    4: optional string db
//	    5: optional string table
//	    6: optional string token
//	    7: optional string label_name
//	    8: optional string repo_name
//	    9: optional list<TTableRef> table_refs
//	    10: optional map<string, string> properties
//	    11: optional binary meta
//	    12: optional binary job_info
//	}
//
// Restore Snapshot rpc
func (rpc *singleFeClient) RestoreSnapshot(spec *base.Spec, tableRefs []*festruct.TTableRef, label string, snapshotResult *festruct.TGetSnapshotResult_) (*festruct.TRestoreSnapshotResult_, error) {
	// NOTE: ignore meta, because it's too large
	log.Debugf("RestoreSnapshot, spec: %s", spec)

	client := rpc.client
	repoName := "__keep_on_local__"
	properties := make(map[string]string)
	properties["reserve_replica"] = "true"
	req := &festruct.TRestoreSnapshotRequest{
		Table:      &spec.Table,
		LabelName:  &label,    // TODO: check remove
		RepoName:   &repoName, // TODO: check remove
		TableRefs:  tableRefs,
		Properties: properties,
		Meta:       snapshotResult.GetMeta(),
		JobInfo:    snapshotResult.GetJobInfo(),
	}
	setAuthInfo(req, spec)

	// NOTE: ignore meta, because it's too large
	log.Debugf("RestoreSnapshotRequest user %s, db %s, table %s, label name %s, properties %v, job info %v",
		req.GetUser(), req.GetDb(), req.GetTable(), req.GetLabelName(), properties, snapshotResult.GetJobInfo())
	if resp, err := client.RestoreSnapshot(context.Background(), req); err != nil {
		return nil, xerror.Wrapf(err, xerror.RPC, "RestoreSnapshot failed, req: %+v", req)
	} else {
		return resp, nil
	}
}

func (rpc *singleFeClient) GetMasterToken(spec *base.Spec) (string, error) {
	log.Debugf("GetMasterToken, spec: %s", spec)

	client := rpc.client
	req := &festruct.TGetMasterTokenRequest{
		Cluster:  &spec.Cluster,
		User:     &spec.User,
		Password: &spec.Password,
	}

	log.Debugf("GetMasterToken user: %s", *req.User)
	if resp, err := client.GetMasterToken(context.Background(), req); err != nil {
		return "", xerror.Wrapf(err, xerror.RPC, "GetMasterToken failed, req: %+v", req)
	} else {
		return resp.GetToken(), nil
	}
}
