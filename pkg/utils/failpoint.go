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
package utils

import (
	"fmt"
	"sync"
	"sync/atomic"
)

var (
	failpointEnabled = atomic.Bool{}
	failpoints       = sync.Map{}
)

type FailpointValue interface{}

func IsFailpointEnabled() bool {
	return failpointEnabled.Load()
}

func EnableFailpoint() {
	failpointEnabled.Store(true)
}

func DisableFailpoint() {
	failpointEnabled.Store(false)
}

func InjectJobFailpoint(jobName, name string, value FailpointValue) {
	failpoint := getJobFailpointName(jobName, name)
	failpoints.Store(failpoint, value)
}

func RemoveJobFailpoint(jobName, name string) {
	failpoint := getJobFailpointName(jobName, name)
	failpoints.Delete(failpoint)
}

func HasJobFailpoint(jobName, name string) bool {
	if !IsFailpointEnabled() {
		return false
	}

	failpoint := getJobFailpointName(jobName, name)
	_, ok := failpoints.Load(failpoint)
	return ok
}

func getJobFailpointName(jobName, name string) string {
	return fmt.Sprintf("/job/%s/%s", jobName, name)
}
