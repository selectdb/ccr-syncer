package rpc

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/selectdb/ccr_syncer/ccr/base"
	bestruct "github.com/selectdb/ccr_syncer/rpc/kitex_gen/backendservice"
	beservice "github.com/selectdb/ccr_syncer/rpc/kitex_gen/backendservice/backendservice"

	"github.com/cloudwego/kitex/client"
	log "github.com/sirupsen/logrus"
)

type IBeRpc interface {
	IngestBinlog(*bestruct.TIngestBinlogRequest) (*bestruct.TIngestBinlogResult_, error)
}

type BeRpc struct {
	backend *base.Backend
	client  beservice.Client
}

func NewBeRpc(be *base.Backend) (*BeRpc, error) {
	log.Debugf("NewBeRpc be: %s", be)

	// create kitex FrontendService client
	if client, err := beservice.NewClient("FrontendService", client.WithHostPorts(fmt.Sprintf("%s:%d", be.Host, be.BePort))); err != nil {
		return nil, errors.Wrapf(err, "NewBeClient error: %v", err)
	} else {
		return &BeRpc{
			backend: be,
			client:  client,
		}, nil
	}
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
