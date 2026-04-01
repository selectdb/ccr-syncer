// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License
package ccr

import (
	"fmt"
	"sync"

	"github.com/selectdb/ccr_syncer/pkg/ccr/base"
)

const (
	backupLimitResource  = "backup"
	restoreLimitResource = "restore"
)

type backupRestoreLimiter struct {
	lock    sync.Mutex
	permits map[string]int
}

func newBackupRestoreLimiter() *backupRestoreLimiter {
	return &backupRestoreLimiter{
		permits: make(map[string]int),
	}
}

func (l *backupRestoreLimiter) TryAcquire(key string, limit int) bool {
	if key == "" || limit <= 0 {
		return true
	}

	l.lock.Lock()
	defer l.lock.Unlock()

	if l.permits[key] >= limit {
		return false
	}

	l.permits[key] += 1
	return true
}

func (l *backupRestoreLimiter) Observe(key string) {
	if key == "" {
		return
	}

	l.lock.Lock()
	defer l.lock.Unlock()

	l.permits[key] += 1
}

func (l *backupRestoreLimiter) Release(key string) {
	if key == "" {
		return
	}

	l.lock.Lock()
	defer l.lock.Unlock()

	inflight, ok := l.permits[key]
	if !ok || inflight <= 0 {
		return
	}

	inflight -= 1
	if inflight == 0 {
		delete(l.permits, key)
	} else {
		l.permits[key] = inflight
	}
}

func buildBackupRestoreLimitKey(resource string, spec base.Spec) string {
	if spec.Table == "" {
		return fmt.Sprintf("%s:%s:%s:%s:db", spec.Host, spec.Port, spec.ThriftPort, resource)
	} else {
		return fmt.Sprintf("%s:%s:%s:%s:table", spec.Host, spec.Port, spec.ThriftPort, resource)
	}
}
