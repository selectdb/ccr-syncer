package rpc

import (
	"context"
	"fmt"

	"github.com/selectdb/ccr_syncer/ccr/base"
	bestruct "github.com/selectdb/ccr_syncer/rpc/kitex_gen/backendservice"
	beservice "github.com/selectdb/ccr_syncer/rpc/kitex_gen/backendservice/backendservice"

	"github.com/cloudwego/kitex/client"
	log "github.com/sirupsen/logrus"
)

type BeThriftRpc struct {
	backend *base.Backend
	client  beservice.Client
}

func NewBeThriftRpc(be *base.Backend) (*BeThriftRpc, error) {
	// create kitex FrontendService client
	client, err := beservice.NewClient("FrontendService", client.WithHostPorts(fmt.Sprintf("%s:%d", be.Host, be.BePort)))

	if err != nil {
		log.Errorf("NewBeClient error: %v", err)
		return nil, err
	}
	log.Infof("NewBeClient success: %v", client)

	return &BeThriftRpc{
		backend: be,
		client:  client,
	}, err
}

func (beRpc *BeThriftRpc) IngestBinlog(req *bestruct.TIngestBinlogRequest) (*bestruct.TIngestBinlogResult_, error) {
	log.Infof("IngestBinlog")
	client := beRpc.client

	log.Infof("IngestBinlog req: %+v, txnId: %d", req, req.GetTxnId())
	return client.IngestBinlog(context.Background(), req)
}
