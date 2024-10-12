package rpc

import (
	"flag"
	"sync"
)

var (
	FlagMaxIngestConcurrencyPerBackend int64
)

func init() {
	flag.Int64Var(&FlagMaxIngestConcurrencyPerBackend, "max_ingest_concurrency_per_backend", 48,
		"The max concurrency of the binlog ingesting per backend")
}

type ConcurrencyWindow struct {
	mu   *sync.Mutex
	cond *sync.Cond

	id        int64
	inflights int64
}

func newCongestionWindow(id int64) *ConcurrencyWindow {
	mu := &sync.Mutex{}
	return &ConcurrencyWindow{
		mu:        mu,
		cond:      sync.NewCond(mu),
		id:        id,
		inflights: 0,
	}
}

func (cw *ConcurrencyWindow) Acquire() {
	cw.mu.Lock()
	defer cw.mu.Unlock()

	for cw.inflights+1 > FlagMaxIngestConcurrencyPerBackend {
		cw.cond.Wait()
	}
	cw.inflights += 1
}

func (cw *ConcurrencyWindow) Release() {
	cw.mu.Lock()
	defer cw.mu.Unlock()

	if cw.inflights == 0 {
		return
	}

	cw.inflights -= 1
	cw.cond.Signal()
}

type ConcurrencyManager struct {
	windows sync.Map
}

func NewConcurrencyManager() *ConcurrencyManager {
	return &ConcurrencyManager{}
}

func (cm *ConcurrencyManager) GetWindow(id int64) *ConcurrencyWindow {
	value, ok := cm.windows.Load(id)
	if !ok {
		window := newCongestionWindow(id)
		value, ok = cm.windows.LoadOrStore(id, window)
	}
	return value.(*ConcurrencyWindow)
}
