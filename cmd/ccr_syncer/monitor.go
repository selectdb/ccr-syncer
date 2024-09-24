package main

import (
	"runtime"
	"strings"
	"time"

	"github.com/selectdb/ccr_syncer/pkg/ccr"
	log "github.com/sirupsen/logrus"
)

const (
	MONITOR_DURATION = time.Second * 60
)

type Monitor struct {
	jobManager *ccr.JobManager
	stop       chan struct{}
}

func NewMonitor(jm *ccr.JobManager) *Monitor {
	return &Monitor{
		jobManager: jm,
		stop:       make(chan struct{}),
	}
}

func (m *Monitor) dump() {
	log.Infof("[GOROUTINE] Total = %v", runtime.NumGoroutine())

	mb := func(b uint64) uint64 {
		return b / 1024 / 1024
	}

	// see: https://golang.org/pkg/runtime/#MemStats
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	liveObjects := stats.Mallocs - stats.Frees
	log.Infof("[MEMORY STATS] Alloc = %v MiB, TotalAlloc = %v MiB, Sys = %v MiB, NumGC = %v, LiveObjects = %v",
		mb(stats.Alloc), mb(stats.TotalAlloc), mb(stats.Sys), stats.NumGC, liveObjects)

	jobs := m.jobManager.ListJobs()
	numJobs := len(jobs)
	numRunning := 0
	numFullSync := 0
	numIncremental := 0
	numPartialSync := 0
	numTableSync := 0
	numDbSync := 0
	for _, job := range jobs {
		if strings.HasPrefix(job.ProgressState, "DB") {
			numDbSync += 1
		} else {
			numTableSync += 1
		}
		if job.State == "running" {
			numRunning += 1
			if strings.Contains(job.ProgressState, "FullSync") {
				numFullSync += 1
			} else if strings.Contains(job.ProgressState, "PartialSync") {
				numPartialSync += 1
			} else if strings.Contains(job.ProgressState, "IncrementalSync") {
				numIncremental += 1
			}
		}
	}

	log.Infof("[JOB STATS] Total = %v, Running = %v, DBSync = %v, TableSync = %v",
		numJobs, numRunning, numDbSync, numTableSync)
	log.Infof("[JOB STATUS] FullSync = %v, PartialSync = %v, IncrementalSync = %v",
		numFullSync, numPartialSync, numIncremental)
}

func (m *Monitor) Start() {
	ticker := time.NewTicker(MONITOR_DURATION)
	defer ticker.Stop()

	for {
		select {
		case <-m.stop:
			log.Info("monitor stopped")
			return
		case <-ticker.C:
			m.dump()
		}
	}
}

func (m *Monitor) Stop() {
	log.Info("monitor stopping")
	close(m.stop)
}
