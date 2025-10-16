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
package main

import (
	"encoding/json"
	"time"

	"github.com/selectdb/ccr_syncer/pkg/ccr"
	"github.com/selectdb/ccr_syncer/pkg/ccr/base"
	"github.com/selectdb/ccr_syncer/pkg/storage"
	"github.com/selectdb/ccr_syncer/pkg/xerror"
	"github.com/selectdb/ccr_syncer/pkg/xmetrics"
	log "github.com/sirupsen/logrus"
)

type JobCollector struct {
	db       storage.DB
	hostInfo string
	factory  *ccr.Factory
	stop     chan struct{}
}

func NewJobCollector(db storage.DB, hostInfo string, factory *ccr.Factory) *JobCollector {
	log.Infof("JobCollector initialized with hostInfo: %s", hostInfo)
	return &JobCollector{
		db:       db,
		hostInfo: hostInfo,
		factory:  factory,
		stop:     make(chan struct{}),
	}
}

func (c *JobCollector) Collect() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.stop:
			return
		case <-ticker.C:
			if err := c.updateMetrics(); err != nil {
				log.Warnf("update metrics failed: %+v", err)
			}
		}
	}
}

func (c *JobCollector) Stop() {
	close(c.stop)
}

func (c *JobCollector) updateMetrics() error {
	_, jobs, err := c.db.GetStampAndJobs(c.hostInfo)
	if err != nil {
		return xerror.Wrapf(err, xerror.Normal, "get jobs by belong_to %s failed", c.hostInfo)
	}

	log.Debugf("JobCollector fetched %d jobs for hostInfo: %s", len(jobs), c.hostInfo)

	runningJobNum := 0
	for _, jobName := range jobs {
		jobInfo, err := loadJobInfo(jobName, c.db)
		if err != nil {
			log.Warnf("load job %s info failed: %+v", jobName, err)
			continue
		}

		jobProgress, err := loadJobProgress(jobName, c.db)
		if err != nil {
			log.Warnf("load job %s progress failed: %+v", jobName, err)
			continue
		}

		if jobInfo.State == ccr.JobRunning {
			runningJobNum++
		}

		srcSpec := &jobInfo.Src
		commitSeq := jobProgress.CommitSeq
		lag, interval, err := c.getJobLag(srcSpec, commitSeq)
		if err != nil {
			log.Warnf("get job %s lag failed: %+v", jobName, err)
			continue
		}

		xmetrics.UpdateJobLag(jobName, lag, interval)
		xmetrics.UpdateJobSyncState(jobName, int(jobProgress.SyncState), jobProgress.SubSyncState.State)
	}
	xmetrics.UpdateJobNum(runningJobNum)

	return nil
}

func loadJobProgress(jobName string, db storage.DB) (*ccr.JobProgress, error) {
	progressData, err := db.GetProgress(jobName)
	if err != nil {
		return nil, xerror.Wrapf(err, xerror.Normal, "get job progress data failed")
	}

	var jobProgress ccr.JobProgress
	err = json.Unmarshal([]byte(progressData), &jobProgress)
	if err != nil {
		return nil, xerror.Wrapf(err, xerror.Normal, "unmarshal get job progress failed")
	}

	return &jobProgress, nil
}

func loadJobInfo(jobName string, db storage.DB) (*ccr.Job, error) {
	jobInfo, err := db.GetJobInfo(jobName)
	if err != nil {
		return nil, xerror.Wrapf(err, xerror.Normal, "get job info failed")
	}

	var job ccr.Job
	err = json.Unmarshal([]byte(jobInfo), &job)
	if err != nil {
		return nil, xerror.Wrapf(err, xerror.Normal, "unmarshal get job info failed")
	}

	return &job, nil
}

func (c *JobCollector) getJobLag(spec *base.Spec, commitSeq int64) (int64, float64, error) {
	feRpc, err := c.factory.NewFeRpc(spec)
	if err != nil {
		return 0, 0, xerror.Wrapf(err, xerror.Normal, "new fe rpc failed")
	}

	resp, err := feRpc.GetBinlogLag(spec, commitSeq)
	if err != nil {
		return 0, 0, xerror.Wrapf(err, xerror.Normal, "rpc get bin log failed")
	}

	lag := resp.GetLag()
	nextTimestampMs := resp.GetNextBinlogTimestamp()
	lastTimestampMs := resp.GetLastBinlogTimestamp()
	intervals := float64(lastTimestampMs-nextTimestampMs) / 1000.0
	if intervals <= 0 || lag <= 0 {
		intervals = 0
	}
	return lag, intervals, nil
}
