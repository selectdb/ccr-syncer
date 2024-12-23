// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License
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
