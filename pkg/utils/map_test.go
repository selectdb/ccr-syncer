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
package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCopyMap(t *testing.T) {
	// Test with string keys and int values
	m1 := map[string]int{"a": 1, "b": 2, "c": 3}
	m2 := CopyMap(m1)
	assert.Equal(t, m1, m2)
	// update
	m1["c"] = 4
	assert.NotEqual(t, m1, m2)

	// Test with int keys and string values
	m3 := map[int]string{1: "a", 2: "b", 3: "c"}
	m4 := CopyMap(m3)
	assert.Equal(t, m3, m4)
	// update
	m3[3] = "d"
	assert.NotEqual(t, m3, m4)

	// Test with float keys and bool values
	m5 := map[float64]bool{1.1: true, 2.2: false, 3.3: true}
	m6 := CopyMap(m5)
	assert.Equal(t, m5, m6)
	// update
	m5[3.3] = false
	assert.NotEqual(t, m5, m6)
}
