package rpc

import (
	"context"

	"github.com/selectdb/ccr_syncer/ccr/base"
	festruct "github.com/selectdb/ccr_syncer/rpc/kitex_gen/frontendservice"
	feservice "github.com/selectdb/ccr_syncer/rpc/kitex_gen/frontendservice/frontendservice"
	festruct_types "github.com/selectdb/ccr_syncer/rpc/kitex_gen/types"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

const (
	LOCAL_REPO_NAME = ""
)

type IFeRpc interface {
	BeginTransaction(*base.Spec, string, []int64) (*festruct.TBeginTxnResult_, error)
	CommitTransaction(*base.Spec, int64, []*festruct_types.TTabletCommitInfo) (*festruct.TCommitTxnResult_, error)
	RollbackTransaction(spec *base.Spec, txnId int64) (*festruct.TRollbackTxnResult_, error)
	GetBinlog(*base.Spec, int64) (*festruct.TGetBinlogResult_, error)
	GetBinlogLag(*base.Spec, int64) (*festruct.TGetBinlogLagResult_, error)
	GetSnapshot(*base.Spec, string) (*festruct.TGetSnapshotResult_, error)
	RestoreSnapshot(*base.Spec, string, *festruct.TGetSnapshotResult_) (*festruct.TRestoreSnapshotResult_, error)
	GetMasterToken(*base.Spec) (string, error)
}

type FeRpc struct {
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
func (rpc *FeRpc) BeginTransaction(spec *base.Spec, label string, tableIds []int64) (*festruct.TBeginTxnResult_, error) {
	log.Debugf("BeginTransaction spec: %s, label: %s, tableIds: %v", spec, label, tableIds)

	client := rpc.client
	req := &festruct.TBeginTxnRequest{
		Label: &label,
	}
	setAuthInfo(req, spec)
	req.TableIds = tableIds

	log.Debugf("BeginTransaction user %s, label: %s, tableIds: %v", req.GetUser(), label, tableIds)
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
func (rpc *FeRpc) CommitTransaction(spec *base.Spec, txnId int64, commitInfos []*festruct_types.TTabletCommitInfo) (*festruct.TCommitTxnResult_, error) {
	log.Debugf("CommitTransaction spec: %s, txnId: %d, commitInfos: %v", spec, txnId, commitInfos)

	client := rpc.client
	req := &festruct.TCommitTxnRequest{}
	setAuthInfo(req, spec)
	req.TxnId = &txnId
	req.CommitInfos = commitInfos

	if result, err := client.CommitTxn(context.Background(), req); err != nil {
		return nil, errors.Wrapf(err, "CommitTransaction error: %v, req: %+v", err, req)
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
func (rpc *FeRpc) RollbackTransaction(spec *base.Spec, txnId int64) (*festruct.TRollbackTxnResult_, error) {
	log.Debugf("RollbackTransaction spec: %s, txnId: %d", spec, txnId)

	client := rpc.client
	req := &festruct.TRollbackTxnRequest{}
	setAuthInfo(req, spec)
	req.TxnId = &txnId

	if result, err := client.RollbackTxn(context.Background(), req); err != nil {
		return nil, errors.Wrapf(err, "RollbackTransaction error: %v, req: %+v", err, req)
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
func (rpc *FeRpc) GetBinlog(spec *base.Spec, commitSeq int64) (*festruct.TGetBinlogResult_, error) {
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
		return nil, errors.Wrapf(err, "GetBinlog error: %v, req: %+v", err, req)
	} else {
		return resp, nil
	}
}

func (rpc *FeRpc) GetBinlogLag(spec *base.Spec, commitSeq int64) (*festruct.TGetBinlogLagResult_, error) {
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
func (rpc *FeRpc) GetSnapshot(spec *base.Spec, labelName string) (*festruct.TGetSnapshotResult_, error) {
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
func (rpc *FeRpc) RestoreSnapshot(spec *base.Spec, label string, snapshotResult *festruct.TGetSnapshotResult_) (*festruct.TRestoreSnapshotResult_, error) {
	log.Debugf("RestoreSnapshot, spec: %s, snapshot result: %+v", spec, snapshotResult)

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

	log.Debugf("RestoreSnapshotRequest user %s, db %s, table %s, label name %s, properties %v, meta %v, job info %v",
		req.GetUser(), req.GetDb(), req.GetTable(), req.GetLabelName(), properties, snapshotResult.GetMeta(), snapshotResult.GetJobInfo())
	if resp, err := client.RestoreSnapshot(context.Background(), req); err != nil {
		return nil, errors.Wrapf(err, "RestoreSnapshot failed, req: %+v", req)
	} else {
		return resp, nil
	}
}

func (rpc *FeRpc) GetMasterToken(spec *base.Spec) (string, error) {
	log.Debugf("GetMasterToken, spec: %s", spec)

	client := rpc.client
	req := &festruct.TGetMasterTokenRequest{
		Cluster:  &spec.Cluster,
		User:     &spec.User,
		Password: &spec.Password,
	}

	log.Debugf("GetMasterToken user: %s", *req.User)
	if resp, err := client.GetMasterToken(context.Background(), req); err != nil {
		return "", errors.Wrapf(err, "GetMasterToken failed, req: %+v", req)
	} else {
		return resp.GetToken(), nil
	}
}
