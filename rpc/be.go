package rpc

import (
	"context"

	"github.com/pkg/errors"
	"github.com/selectdb/ccr_syncer/ccr/base"
	bestruct "github.com/selectdb/ccr_syncer/rpc/kitex_gen/backendservice"
	beservice "github.com/selectdb/ccr_syncer/rpc/kitex_gen/backendservice/backendservice"

	log "github.com/sirupsen/logrus"
)

type IBeRpc interface {
	IngestBinlog(*bestruct.TIngestBinlogRequest) (*bestruct.TIngestBinlogResult_, error)
}

type BeRpc struct {
	backend *base.Backend
	client  beservice.Client
}

func (beRpc *BeRpc) IngestBinlog(req *bestruct.TIngestBinlogRequest) (*bestruct.TIngestBinlogResult_, error) {
	log.Debugf("IngestBinlog req: %+v, txnId: %d, be: %v", req, req.GetTxnId(), beRpc.backend)

	client := beRpc.client
	if result, err := client.IngestBinlog(context.Background(), req); err != nil {
		return nil, errors.Wrapf(err, "IngestBinlog error: %v", err)
	} else {
		return result, nil
	}
}
