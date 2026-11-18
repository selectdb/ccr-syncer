package handle

import (
	"fmt"

	"github.com/selectdb/ccr_syncer/pkg/ccr"
	"github.com/selectdb/ccr_syncer/pkg/ccr/record"
	festruct "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/frontendservice"
	log "github.com/sirupsen/logrus"
)

func init() {
	ccr.RegisterJobHandle[*record.Dummy](festruct.TBinlogType_DUMMY, &DummyHandle{})
}

type DummyHandle struct {
	IdempotentJobHandle[*record.AddPartition]
}

func (h *DummyHandle) IsBinlogCommitted(job *ccr.Job, record *record.Dummy) (bool, error) {
	return true, nil
}

func (h *DummyHandle) Handle(j *ccr.Job, commitSeq int64, dummy *record.Dummy) error {
	info := fmt.Sprintf("handle dummy binlog, need full sync. SyncType: %v, seq: %v", j.SyncType, commitSeq)
	log.Infof("%s", info)

	return j.NewSnapshot(commitSeq, info)
}
