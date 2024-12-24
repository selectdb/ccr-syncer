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

// CopyMap returns a new map with the same key-value pairs as the input map.
// The input map must have keys and values of comparable types.
// but key and value is not deep copy
func CopyMap[K, V comparable](m map[K]V) map[K]V {
	result := make(map[K]V)
	for k, v := range m {
		result[k] = v
	}
	return result
}

// MergeMap returns a new map with all key-value pairs from both input maps.
func MergeMap[K comparable, V any](m1, m2 map[K]V) map[K]V {
	if m1 == nil {
		m1 = make(map[K]V, len(m2))
	}
	for k, v := range m2 {
		m1[k] = v
	}
	return m1
}
