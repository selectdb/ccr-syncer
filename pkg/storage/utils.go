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
package storage

import (
	"fmt"
	"sort"

	"github.com/selectdb/ccr_syncer/pkg/xerror"
)

type LoadInfo struct {
	NowLoad   int
	AddedLoad int
	HostInfo  string
}

func (l *LoadInfo) GetLoad() int {
	return l.AddedLoad + l.NowLoad
}

func (l *LoadInfo) String() string {
	return fmt.Sprintf("[NowLoad: %d, AddedLoad: %d, HostInfo: %s]", l.NowLoad, l.AddedLoad, l.HostInfo)
}

type LoadSlice []LoadInfo

func (ls LoadSlice) Len() int {
	return len(ls)
}

func (ls LoadSlice) Less(i, j int) bool {
	return ls[i].GetLoad() < ls[j].GetLoad()
}

func (ls LoadSlice) Swap(i, j int) {
	ls[i], ls[j] = ls[j], ls[i]
}

func filterHighLoadSyncer(sumLoad int, loadList LoadSlice) (LoadSlice, int, error) {
	sort.Sort(loadList)
	if len(loadList) == 0 || sumLoad == 0 {
		return make(LoadSlice, 0), 0, nil
	}
	averageLoad := float64(sumLoad) / float64(len(loadList))
	for i := len(loadList) - 1; i >= 0; i-- {
		if float64(loadList[i].GetLoad()) < averageLoad {
			return loadList[:i+1], sumLoad, nil
		}
		sumLoad -= loadList[i].GetLoad()
	}
	return nil, 0, xerror.Errorf(xerror.Normal, "There is no available syncer!")
}

func RebalanceLoad(additionalLoad int, currentLoad int, loadList LoadSlice) (LoadSlice, error) {
	load, sumLoad, err := filterHighLoadSyncer(additionalLoad+currentLoad, loadList)
	if err != nil {
		return nil, err
	}

	if sumLoad == 0 || len(load) == 0 {
		return make(LoadSlice, 0), nil
	}

	averageLoad := sumLoad / len(load)
	for i := range load {
		difference := averageLoad - load[i].GetLoad()
		load[i].AddedLoad += difference
		additionalLoad -= difference
	}

	for i := 0; additionalLoad > 0; i++ {
		load[i].AddedLoad++
		additionalLoad--
	}

	return load, nil
}
