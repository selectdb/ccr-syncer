package storage

import (
	"fmt"
	"sort"
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

func filterHighLoadSyncer(sumLoad int, loadList LoadSlice) (LoadSlice, int) {
	sort.Sort(loadList)
	averageLoad := sumLoad / len(loadList)
	for i := len(loadList)-1; i >= 0; i-- {
		if loadList[i].GetLoad() < averageLoad {
			return loadList[:i+1], sumLoad
		}
		sumLoad -= loadList[i].GetLoad()
	}
	return nil, 0
}

func RebalanceLoad(additionalLoad int, currentLoad int, loadList LoadSlice) LoadSlice {
	load, sumLoad := filterHighLoadSyncer(additionalLoad + currentLoad, loadList)
	if (load == nil) {
		return nil
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

	return load
}