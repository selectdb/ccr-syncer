package rpc

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"strings"
	"sync"
	"time"

	festruct "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/frontendservice"
	feservice "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/frontendservice/frontendservice"
	tstatus "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/status"
	festruct_types "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/types"
	"github.com/selectdb/ccr_syncer/pkg/utils"
	"github.com/selectdb/ccr_syncer/pkg/xerror"

	"github.com/cloudwego/kitex/client"
	"github.com/cloudwego/kitex/client/callopt"
	"github.com/cloudwego/kitex/pkg/kerrors"
	"github.com/selectdb/ccr_syncer/pkg/ccr/base"
	log "github.com/sirupsen/logrus"
)

var (
	localRepoName    string
	commitTxnTimeout time.Duration
	connectTimeout   time.Duration
	rpcTimeout       time.Duration
)

var ErrFeNotMasterCompatible = xerror.NewWithoutStack(xerror.FE, "not master compatible")

func init() {
	flag.StringVar(&localRepoName, "local_repo_name", "", "local_repo_name")
	flag.DurationVar(&commitTxnTimeout, "commit_txn_timeout", 33*time.Second, "commmit_txn_timeout")
	flag.DurationVar(&connectTimeout, "connect_timeout", 10*time.Second, "connect timeout")
	flag.DurationVar(&rpcTimeout, "rpc_timeout", 30*time.Second, "rpc timeout")
}

// canUseNextAddr means can try next addr, err is a connection error, not a method not found or other error
func canUseNextAddr(err error) bool {
	if errors.Is(err, kerrors.ErrNoConnection) {
		return true
	}
	if errors.Is(err, kerrors.ErrNoResolver) {
		return true
	}
	if errors.Is(err, kerrors.ErrNoDestAddress) {
		return true
	}
	if errors.Is(err, kerrors.ErrRemoteOrNetwork) {
		return true
	}

	errMsg := err.Error()
	if strings.Contains(errMsg, "connection has been closed by peer") {
		return true
	}
	if strings.Contains(errMsg, "closed network connection") {
		return true
	}
	if strings.Contains(errMsg, "connection reset by peer") {
		return true
	}
	if strings.Contains(errMsg, "connection reset by peer") {
		return true
	}

	return false
}

type RestoreSnapshotRequest struct {
	TableRefs       []*festruct.TTableRef
	SnapshotName    string
	SnapshotResult  *festruct.TGetSnapshotResult_
	AtomicRestore   bool
	CleanPartitions bool
	CleanTables     bool
	Compress        bool
}

type IFeRpc interface {
	BeginTransaction(*base.Spec, string, []int64) (*festruct.TBeginTxnResult_, error)
	CommitTransaction(*base.Spec, int64, []*festruct_types.TTabletCommitInfo) (*festruct.TCommitTxnResult_, error)
	RollbackTransaction(spec *base.Spec, txnId int64) (*festruct.TRollbackTxnResult_, error)
	GetBinlog(*base.Spec, int64) (*festruct.TGetBinlogResult_, error)
	GetBinlogLag(*base.Spec, int64) (*festruct.TGetBinlogLagResult_, error)
	GetSnapshot(*base.Spec, string, bool) (*festruct.TGetSnapshotResult_, error)
	RestoreSnapshot(*base.Spec, *RestoreSnapshotRequest) (*festruct.TRestoreSnapshotResult_, error)
	GetMasterToken(*base.Spec) (*festruct.TGetMasterTokenResult_, error)
	GetDbMeta(spec *base.Spec) (*festruct.TGetMetaResult_, error)
	GetTableMeta(spec *base.Spec, tableIds []int64) (*festruct.TGetMetaResult_, error)
	GetBackends(spec *base.Spec) (*festruct.TGetBackendMetaResult_, error)

	Address() string
}

type FeRpc struct {
	spec          *base.Spec
	masterClient  IFeRpc
	clients       map[string]IFeRpc
	cachedFeAddrs map[string]bool
	lock          sync.RWMutex // for get client
}

func NewFeRpc(spec *base.Spec) (*FeRpc, error) {
	addr := fmt.Sprintf("%s:%s", spec.Host, spec.ThriftPort)
	client, err := newSingleFeClient(addr)
	if err != nil {
		return nil, xerror.Wrapf(err, xerror.RPC, "NewFeClient error: %v", err)
	}

	clients := make(map[string]IFeRpc)
	clients[client.Address()] = client
	cachedFeAddrs := make(map[string]bool)
	for _, fe := range spec.Frontends {
		addr := fmt.Sprintf("%s:%s", fe.Host, fe.ThriftPort)

		if _, ok := cachedFeAddrs[addr]; ok {
			continue
		}

		// for cached all spec clients
		if client, err := newSingleFeClient(addr); err != nil {
			log.Warnf("new fe client error: %+v", err)
		} else {
			clients[client.Address()] = client
		}
		cachedFeAddrs[addr] = true
	}

	return &FeRpc{
		spec:          spec,
		masterClient:  client,
		clients:       clients,
		cachedFeAddrs: cachedFeAddrs,
	}, nil
}

// get all fe addrs
// "[masterAddr],otherCachedFeAddrs" => "[127.0.0.1:1000],127.0.1:1001,127.0.1:1002"
func (rpc *FeRpc) Address() string {
	cachedFeAddrs := rpc.getCacheFeAddrs()
	masterClient := rpc.getMasterClient()

	var addrBuilder strings.Builder
	addrBuilder.WriteString(fmt.Sprintf("[%s]", masterClient.Address()))
	delete(cachedFeAddrs, masterClient.Address())
	for addr := range cachedFeAddrs {
		addrBuilder.WriteString(",")
		addrBuilder.WriteString(addr)
	}
	return addrBuilder.String()
}

type resultType interface {
	GetStatus() *tstatus.TStatus
	IsSetMasterAddress() bool
	GetMasterAddress() *festruct_types.TNetworkAddress
}
type callerType func(client IFeRpc) (resultType, error)

func (rpc *FeRpc) getMasterClient() IFeRpc {
	rpc.lock.RLock()
	defer rpc.lock.RUnlock()

	return rpc.masterClient
}

func (rpc *FeRpc) updateMasterClient(masterClient IFeRpc) {
	rpc.lock.Lock()
	defer rpc.lock.Unlock()

	rpc.clients[masterClient.Address()] = masterClient
	rpc.masterClient = masterClient
}

func (rpc *FeRpc) getClient(addr string) (IFeRpc, bool) {
	rpc.lock.RLock()
	defer rpc.lock.RUnlock()

	client, ok := rpc.clients[addr]
	return client, ok
}

func (rpc *FeRpc) addClient(client IFeRpc) {
	rpc.lock.Lock()
	defer rpc.lock.Unlock()

	rpc.clients[client.Address()] = client
}

func (rpc *FeRpc) getClients() map[string]IFeRpc {
	rpc.lock.RLock()
	defer rpc.lock.RUnlock()

	return utils.CopyMap(rpc.clients)
}

func (rpc *FeRpc) getCacheFeAddrs() map[string]bool {
	rpc.lock.RLock()
	defer rpc.lock.RUnlock()

	return utils.CopyMap(rpc.cachedFeAddrs)
}

type retryWithMasterRedirectAndCachedClientsRpc struct {
	rpc            *FeRpc
	caller         callerType
	notriedClients map[string]IFeRpc
}

type call0Result struct {
	canUseNextAddr bool
	resp           resultType
	err            error
	masterAddr     string
}

func (r *retryWithMasterRedirectAndCachedClientsRpc) call0(masterClient IFeRpc) *call0Result {
	caller := r.caller
	resp, err := caller(masterClient)
	log.Tracef("call resp: %.128v, error: %+v", resp, err)

	// Step 1: check error
	if err != nil {
		if !canUseNextAddr(err) {
			return &call0Result{
				canUseNextAddr: false,
				err:            xerror.Wrap(err, xerror.FE, "thrift error"),
			}
		} else {
			log.Warnf("call error: %+v, try next addr", err)
			return &call0Result{
				canUseNextAddr: true,
				err:            xerror.Wrap(err, xerror.FE, "thrift error"),
			}
		}
	}

	// Step 2: check need redirect
	if resp.GetStatus().GetStatusCode() != tstatus.TStatusCode_NOT_MASTER {
		return &call0Result{
			canUseNextAddr: false,
			resp:           resp,
			err:            nil,
		}
	}

	// no compatible for master
	if !resp.IsSetMasterAddress() {
		err = xerror.XPanicWrapf(ErrFeNotMasterCompatible, "fe addr [%s]", masterClient.Address())
		return &call0Result{
			canUseNextAddr: true,
			err:            err, // not nil
		}
	}

	// switch to master
	masterAddr := resp.GetMasterAddress()
	err = xerror.Errorf(xerror.FE, "addr [%s] is not master", masterAddr)
	return &call0Result{
		canUseNextAddr: true,
		resp:           resp,
		masterAddr:     fmt.Sprintf("%s:%d", masterAddr.Hostname, masterAddr.Port),
		err:            err, // not nil
	}
}

func (r *retryWithMasterRedirectAndCachedClientsRpc) call() (resultType, error) {
	rpc := r.rpc
	masterClient := rpc.masterClient

	// Step 1: try master
	result := r.call0(masterClient)
	log.Tracef("call0 result: %+v", result)
	if result.err == nil {
		return result.resp, nil
	}

	// Step 2: check error, if can't use next addr, return error
	// canUseNextAddr means can try next addr, contains ErrNoConnection, ErrNoResolver, ErrNoDestAddress => (feredirect && use next cached addr)
	if !result.canUseNextAddr {
		return nil, result.err
	}

	// Step 3: if set master addr, redirect to master
	// redirect to master
	if result.masterAddr != "" {
		masterAddr := result.masterAddr
		log.Infof("switch to master %s", masterAddr)

		var err error
		client, ok := rpc.getClient(masterAddr)
		if ok {
			masterClient = client
		} else {
			masterClient, err = newSingleFeClient(masterAddr)
			if err != nil {
				return nil, xerror.Wrapf(err, xerror.RPC, "NewFeClient [%s] error: %v", masterAddr, err)
			}
		}
		rpc.updateMasterClient(masterClient)
		return r.call()
	}

	// Step 4: try all cached fe clients
	if r.notriedClients == nil {
		r.notriedClients = rpc.getClients()
	}
	delete(r.notriedClients, masterClient.Address())
	if len(r.notriedClients) == 0 {
		return nil, result.err
	}
	// get first notried client
	var client IFeRpc
	for _, client = range r.notriedClients {
		break
	}
	// because call0 failed, so original masterClient is not master now, set client as masterClient for retry
	rpc.updateMasterClient(client)
	return r.call()
}

func (rpc *FeRpc) callWithMasterRedirect(caller callerType) (resultType, error) {
	r := &retryWithMasterRedirectAndCachedClientsRpc{
		rpc:    rpc,
		caller: caller,
	}
	return r.call()
}

func convertResult[T any](result any, err error) (*T, error) {
	if result == nil {
		return nil, err
	}

	return result.(*T), err
}

func (rpc *FeRpc) BeginTransaction(spec *base.Spec, label string, tableIds []int64) (*festruct.TBeginTxnResult_, error) {
	// return rpc.masterClient.BeginTransaction(spec, label, tableIds)
	caller := func(client IFeRpc) (resultType, error) {
		return client.BeginTransaction(spec, label, tableIds)
	}
	result, err := rpc.callWithMasterRedirect(caller)
	return convertResult[festruct.TBeginTxnResult_](result, err)
}

func (rpc *FeRpc) CommitTransaction(spec *base.Spec, txnId int64, commitInfos []*festruct_types.TTabletCommitInfo) (*festruct.TCommitTxnResult_, error) {
	// return rpc.masterClient.CommitTransaction(spec, txnId, commitInfos)
	caller := func(client IFeRpc) (resultType, error) {
		return client.CommitTransaction(spec, txnId, commitInfos)
	}
	result, err := rpc.callWithMasterRedirect(caller)
	return convertResult[festruct.TCommitTxnResult_](result, err)
}

func (rpc *FeRpc) RollbackTransaction(spec *base.Spec, txnId int64) (*festruct.TRollbackTxnResult_, error) {
	// return rpc.masterClient.RollbackTransaction(spec, txnId)
	caller := func(client IFeRpc) (resultType, error) {
		return client.RollbackTransaction(spec, txnId)
	}
	result, err := rpc.callWithMasterRedirect(caller)
	return convertResult[festruct.TRollbackTxnResult_](result, err)
}

func (rpc *FeRpc) GetBinlog(spec *base.Spec, commitSeq int64) (*festruct.TGetBinlogResult_, error) {
	// return rpc.masterClient.GetBinlog(spec, commitSeq)
	caller := func(client IFeRpc) (resultType, error) {
		return client.GetBinlog(spec, commitSeq)
	}
	result, err := rpc.callWithMasterRedirect(caller)
	return convertResult[festruct.TGetBinlogResult_](result, err)
}

func (rpc *FeRpc) GetBinlogLag(spec *base.Spec, commitSeq int64) (*festruct.TGetBinlogLagResult_, error) {
	// return rpc.masterClient.GetBinlogLag(spec, commitSeq)
	caller := func(client IFeRpc) (resultType, error) {
		return client.GetBinlogLag(spec, commitSeq)
	}
	result, err := rpc.callWithMasterRedirect(caller)
	return convertResult[festruct.TGetBinlogLagResult_](result, err)
}

func (rpc *FeRpc) GetSnapshot(spec *base.Spec, labelName string, compress bool) (*festruct.TGetSnapshotResult_, error) {
	// return rpc.masterClient.GetSnapshot(spec, labelName)
	caller := func(client IFeRpc) (resultType, error) {
		return client.GetSnapshot(spec, labelName, compress)
	}
	result, err := rpc.callWithMasterRedirect(caller)
	return convertResult[festruct.TGetSnapshotResult_](result, err)
}

func (rpc *FeRpc) RestoreSnapshot(spec *base.Spec, req *RestoreSnapshotRequest) (*festruct.TRestoreSnapshotResult_, error) {
	caller := func(client IFeRpc) (resultType, error) {
		return client.RestoreSnapshot(spec, req)
	}
	result, err := rpc.callWithMasterRedirect(caller)
	return convertResult[festruct.TRestoreSnapshotResult_](result, err)
}

func (rpc *FeRpc) GetMasterToken(spec *base.Spec) (*festruct.TGetMasterTokenResult_, error) {
	// return rpc.masterClient.GetMasterToken(spec)
	caller := func(client IFeRpc) (resultType, error) {
		return client.GetMasterToken(spec)
	}
	result, err := rpc.callWithMasterRedirect(caller)
	return convertResult[festruct.TGetMasterTokenResult_](result, err)
}

func (rpc *FeRpc) GetDbMeta(spec *base.Spec) (*festruct.TGetMetaResult_, error) {
	caller := func(client IFeRpc) (resultType, error) {
		return client.GetDbMeta(spec)
	}
	result, err := rpc.callWithMasterRedirect(caller)
	return convertResult[festruct.TGetMetaResult_](result, err)
}

func (rpc *FeRpc) GetTableMeta(spec *base.Spec, tableIds []int64) (*festruct.TGetMetaResult_, error) {
	caller := func(client IFeRpc) (resultType, error) {
		return client.GetTableMeta(spec, tableIds)
	}
	result, err := rpc.callWithMasterRedirect(caller)
	return convertResult[festruct.TGetMetaResult_](result, err)
}

func (rpc *FeRpc) GetBackends(spec *base.Spec) (*festruct.TGetBackendMetaResult_, error) {
	caller := func(client IFeRpc) (resultType, error) {
		return client.GetBackends(spec)
	}
	result, err := rpc.callWithMasterRedirect(caller)
	return convertResult[festruct.TGetBackendMetaResult_](result, err)
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
	if fe_client, err := feservice.NewClient("FrontendService", client.WithHostPorts(addr), client.WithConnectTimeout(connectTimeout), client.WithRPCTimeout(rpcTimeout)); err != nil {
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
	log.Debugf("Call BeginTransaction, addr: %s, spec: %s, label: %s, tableIds: %v", rpc.Address(), spec, label, tableIds)

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
	log.Debugf("Call CommitTransaction, addr: %s spec: %s, txnId: %d, commitInfos: %v", rpc.Address(), spec, txnId, commitInfos)

	client := rpc.client
	req := &festruct.TCommitTxnRequest{}
	setAuthInfo(req, spec)
	req.TxnId = &txnId
	req.CommitInfos = commitInfos

	if result, err := client.CommitTxn(context.Background(), req, callopt.WithRPCTimeout(commitTxnTimeout)); err != nil {
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
	log.Debugf("Call RollbackTransaction, addr: %s, spec: %s, txnId: %d", rpc.Address(), spec, txnId)

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
	log.Debugf("Call GetBinlog, addr: %s, spec: %s, commit seq: %d", rpc.Address(), spec, commitSeq)

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
	log.Debugf("Call GetBinlogLag, addr: %s, spec: %s, commit seq: %d", rpc.Address(), spec, commitSeq)

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
//	    10: optional bool enable_compress
//	}
func (rpc *singleFeClient) GetSnapshot(spec *base.Spec, labelName string, compress bool) (*festruct.TGetSnapshotResult_, error) {
	log.Debugf("Call GetSnapshot, addr: %s, spec: %s, label: %s", rpc.Address(), spec, labelName)

	client := rpc.client
	snapshotType := festruct.TSnapshotType_LOCAL
	snapshotName := ""
	req := &festruct.TGetSnapshotRequest{
		Table:          &spec.Table,
		LabelName:      &labelName,
		SnapshotType:   &snapshotType,
		SnapshotName:   &snapshotName,
		EnableCompress: &compress,
	}
	setAuthInfo(req, spec)

	log.Debugf("GetSnapshotRequest user %s, db %s, table %s, label name %s, snapshot name %s, snapshot type %d, enable compress %t",
		req.GetUser(), req.GetDb(), req.GetTable(), req.GetLabelName(), req.GetSnapshotName(), req.GetSnapshotType(), req.GetEnableCompress())
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
//	    13: optional bool clean_tables
//	    14: optional bool clean_partitions
//	    15: optional bool atomic_restore
//	    16: optional bool compressed
//	}
//
// Restore Snapshot rpc
func (rpc *singleFeClient) RestoreSnapshot(spec *base.Spec, restoreReq *RestoreSnapshotRequest) (*festruct.TRestoreSnapshotResult_, error) {
	// NOTE: ignore meta, because it's too large
	log.Debugf("Call RestoreSnapshot, addr: %s, spec: %s", rpc.Address(), spec)

	client := rpc.client
	repoName := "__keep_on_local__"
	properties := make(map[string]string)
	properties["reserve_replica"] = "true"

	// Support compressed snapshot
	meta := restoreReq.SnapshotResult.GetMeta()
	jobInfo := restoreReq.SnapshotResult.GetJobInfo()
	if restoreReq.Compress {
		var err error
		meta, err = utils.GZIPCompress(meta)
		if err != nil {
			return nil, xerror.Wrapf(err, xerror.Normal, "gzip compress snapshot meta error: %v", err)
		}
		jobInfo, err = utils.GZIPCompress(jobInfo)
		if err != nil {
			return nil, xerror.Wrapf(err, xerror.Normal, "gzip compress snapshot job info error: %v", err)
		}
	}

	req := &festruct.TRestoreSnapshotRequest{
		Table:           &spec.Table,
		LabelName:       &restoreReq.SnapshotName,
		RepoName:        &repoName,
		TableRefs:       restoreReq.TableRefs,
		Properties:      properties,
		Meta:            meta,
		JobInfo:         jobInfo,
		CleanTables:     &restoreReq.CleanTables,
		CleanPartitions: &restoreReq.CleanPartitions,
		AtomicRestore:   &restoreReq.AtomicRestore,
		Compressed:      utils.ThriftValueWrapper(restoreReq.Compress),
	}
	setAuthInfo(req, spec)

	// NOTE: ignore meta, because it's too large
	log.Debugf("RestoreSnapshotRequest user %s, db %s, table %s, label name %s, properties %v, clean tables: %t, clean partitions: %t, atomic restore: %t, compressed: %t",
		req.GetUser(), req.GetDb(), req.GetTable(), req.GetLabelName(), properties,
		restoreReq.CleanTables, restoreReq.CleanPartitions, restoreReq.AtomicRestore,
		req.GetCompressed())

	if resp, err := client.RestoreSnapshot(context.Background(), req); err != nil {
		return nil, xerror.Wrapf(err, xerror.RPC, "RestoreSnapshot failed")
	} else {
		return resp, nil
	}
}

func (rpc *singleFeClient) GetMasterToken(spec *base.Spec) (*festruct.TGetMasterTokenResult_, error) {
	log.Debugf("Call GetMasterToken, addr: %s, spec: %s", rpc.Address(), spec)

	client := rpc.client
	req := &festruct.TGetMasterTokenRequest{
		Cluster:  &spec.Cluster,
		User:     &spec.User,
		Password: &spec.Password,
	}

	log.Debugf("GetMasterToken user: %s", *req.User)
	if resp, err := client.GetMasterToken(context.Background(), req); err != nil {
		return nil, xerror.Wrapf(err, xerror.RPC, "GetMasterToken failed, req: %+v", req)
	} else {
		return resp, nil
	}
}

func (rpc *singleFeClient) getMeta(spec *base.Spec, reqTables []*festruct.TGetMetaTable) (*festruct.TGetMetaResult_, error) {
	client := rpc.client

	reqDb := festruct.NewTGetMetaDB() // festruct.NewTGetMetaTable()
	reqDb.Id = &spec.DbId
	reqDb.SetTables(reqTables)

	req := &festruct.TGetMetaRequest{
		User:   &spec.User,
		Passwd: &spec.Password,
		Db:     reqDb,
	}

	if resp, err := client.GetMeta(context.Background(), req); err != nil {
		return nil, xerror.Wrapf(err, xerror.RPC, "GetMeta failed, req: %+v", req)
	} else {
		return resp, nil
	}
}

func (rpc *singleFeClient) GetDbMeta(spec *base.Spec) (*festruct.TGetMetaResult_, error) {
	log.Debugf("GetMetaDb, addr: %s, spec: %s", rpc.Address(), spec)

	return rpc.getMeta(spec, nil)
}

func (rpc *singleFeClient) GetTableMeta(spec *base.Spec, tableIds []int64) (*festruct.TGetMetaResult_, error) {
	log.Debugf("GetMetaTable, addr: %s, tableIds: %v", rpc.Address(), tableIds)

	reqTables := make([]*festruct.TGetMetaTable, 0, len(tableIds))
	for _, tableId := range tableIds {
		reqTable := festruct.NewTGetMetaTable()
		reqTable.Id = &tableId
		reqTables = append(reqTables, reqTable)
	}

	return rpc.getMeta(spec, reqTables)
}

func (rpc *singleFeClient) GetBackends(spec *base.Spec) (*festruct.TGetBackendMetaResult_, error) {
	log.Debugf("GetBackends, addr: %s, spec: %s", rpc.Address(), spec)

	client := rpc.client
	req := &festruct.TGetBackendMetaRequest{
		Cluster: &spec.Cluster,
		User:    &spec.User,
		Passwd:  &spec.Password,
	}

	if resp, err := client.GetBackendMeta(context.Background(), req); err != nil {
		return nil, xerror.Wrapf(err, xerror.RPC, "GetBackendMeta failed, req: %+v", req)
	} else {
		return resp, nil
	}
}
