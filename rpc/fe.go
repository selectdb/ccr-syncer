package rpc

import (
	"context"

	"github.com/selectdb/ccr_syncer/ccr/base"
	festruct "github.com/selectdb/ccr_syncer/rpc/kitex_gen/frontendservice"
	feservice "github.com/selectdb/ccr_syncer/rpc/kitex_gen/frontendservice/frontendservice"
	festruct_types "github.com/selectdb/ccr_syncer/rpc/kitex_gen/types"

	"github.com/cloudwego/kitex/client"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

const (
	LOCAL_REPO_NAME = ""
)

type ThriftRpc struct {
	client feservice.Client
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

// TODO(Drogon): remove
// var _ *ccr.ThriftRpc = (*ThriftRpc)(nil)

func NewThriftRpc(spec *base.Spec) (*ThriftRpc, error) {
	// valid spec
	if err := spec.Valid(); err != nil {
		return nil, err
	}

	// create kitex FrontendService client
	if fe_client, err := feservice.NewClient("FrontendService", client.WithHostPorts(spec.Host+":"+spec.ThriftPort)); err != nil {
		return nil, errors.Wrapf(err, "NewFeClient error: %v, spec: %s", err, spec)
	} else {
		return &ThriftRpc{
			client: fe_client,
		}, nil
	}
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
func (rpc *ThriftRpc) BeginTransaction(spec *base.Spec, label string, tableIds []int64) (*festruct.TBeginTxnResult_, error) {
	log.Tracef("BeginTransaction spec: %s, label: %s, tableIds: %v", spec, label, tableIds)

	client := rpc.client
	req := &festruct.TBeginTxnRequest{
		Label: &label,
	}
	setAuthInfo(req, spec)
	req.TableIds = tableIds

	log.Tracef("BeginTransaction req: %+v", req)
	if result, err := client.BeginTxn(context.Background(), req); err != nil {
		return nil, errors.Wrapf(err, "BeginTransaction error: %v, req: %+v", err, req)
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
func (rpc *ThriftRpc) CommitTransaction(spec *base.Spec, txnId int64, commitInfos []*festruct_types.TTabletCommitInfo) (*festruct.TCommitTxnResult_, error) {
	log.Tracef("CommitTransaction spec: %s, txnId: %d, commitInfos: %v", spec, txnId, commitInfos)

	client := rpc.client
	req := &festruct.TCommitTxnRequest{}
	setAuthInfo(req, spec)
	req.TxnId = &txnId
	req.CommitInfos = commitInfos

	log.Tracef("CommitTransaction req: %+v", req)
	if result, err := client.CommitTxn(context.Background(), req); err != nil {
		return nil, errors.Wrapf(err, "CommitTransaction error: %v, req: %+v", err, req)
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
func (rpc *ThriftRpc) GetBinlog(spec *base.Spec, commitSeq int64) (*festruct.TGetBinlogResult_, error) {
	log.Tracef("GetBinlog, spec: %s, commit seq: %d", spec, commitSeq)

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

	log.Tracef("GetBinlog req: %+v", req)
	if resp, err := client.GetBinlog(context.Background(), req); err != nil {
		return nil, errors.Wrapf(err, "GetBinlog error: %v, req: %+v", err, req)
	} else {
		return resp, nil
	}
}

func (rpc *ThriftRpc) GetBinlogLag(spec *base.Spec, commitSeq int64) (*festruct.TGetBinlogLagResult_, error) {
	log.Tracef("GetBinlogLag, spec: %s, commit seq: %d", spec, commitSeq)

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

	log.Tracef("GetBinlogLag req: %+v", req)
	if resp, err := client.GetBinlogLag(context.Background(), req); err != nil {
		return nil, errors.Wrapf(err, "GetBinlogLag error: %v, req: %+v", err, req)
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
func (rpc *ThriftRpc) GetSnapshot(spec *base.Spec, labelName string) (*festruct.TGetSnapshotResult_, error) {
	log.Tracef("GetSnapshot %s, spec: %s", labelName, spec)

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

	log.Tracef("GetBinlog req: %+v", req)
	if resp, err := client.GetSnapshot(context.Background(), req); err != nil {
		return nil, errors.Wrapf(err, "GetSnapshot error: %v, req: %+v", err, req)
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
func (rpc *ThriftRpc) RestoreSnapshot(spec *base.Spec, label string, snapshotResult *festruct.TGetSnapshotResult_) (*festruct.TRestoreSnapshotResult_, error) {
	log.Tracef("RestoreSnapshot, spec: %s, snapshot result: %+v", spec, snapshotResult)

	client := rpc.client
	repoName := "__keep_on_local__"
	properties := make(map[string]string)
	properties["reserve_replica"] = "true"
	// log.Infof("meta: %v", string(snapshotResult.GetMeta()))
	req := &festruct.TRestoreSnapshotRequest{
		Table:      &spec.Table,
		LabelName:  &label,    // TODO: check remove
		RepoName:   &repoName, // TODO: check remove
		Properties: properties,
		Meta:       snapshotResult.GetMeta(),
		JobInfo:    snapshotResult.GetJobInfo(),
	}
	setAuthInfo(req, spec)

	log.Tracef("RestoreSnapshot req: %+v", req)
	if resp, err := client.RestoreSnapshot(context.Background(), req); err != nil {
		return nil, errors.Wrapf(err, "RestoreSnapshot failed, req: %+v", req)
	} else {
		return resp, nil
	}
}

func (rpc *ThriftRpc) GetMasterToken(spec *base.Spec) (string, error) {
	log.Tracef("GetMasterToken, spec: %s", spec)

	client := rpc.client
	req := &festruct.TGetMasterTokenRequest{
		Cluster:  &spec.Cluster,
		User:     &spec.User,
		Password: &spec.Password,
	}

	log.Tracef("GetMasterToken req: %+v", req)
	if resp, err := client.GetMasterToken(context.Background(), req); err != nil {
		return "", errors.Wrapf(err, "GetMasterToken failed, req: %+v", req)
	} else {
		return resp.GetToken(), nil
	}
}
