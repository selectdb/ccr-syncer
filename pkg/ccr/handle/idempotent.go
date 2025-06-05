package handle

import (
	"github.com/selectdb/ccr_syncer/pkg/ccr"
	"github.com/selectdb/ccr_syncer/pkg/ccr/record"
)

type IdempotentJobHandle[T record.Record] struct{}

func (h *IdempotentJobHandle[T]) IsIdempotent() bool {
	return true
}

func (h *IdempotentJobHandle[T]) IsBinlogCommitted(job *ccr.Job, record T) (bool, error) {
	return false, nil
}

func (h *IdempotentJobHandle[T]) WhenBinlogCommitted(job *ccr.Job, record T) error {
	// Do nothing, the idempotent job handle will not be called when the binlog is committed
	return nil
}
