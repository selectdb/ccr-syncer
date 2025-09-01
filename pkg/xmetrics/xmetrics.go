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
package xmetrics

import (
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/selectdb/ccr_syncer/pkg/xerror"
)

var (
	registry = prometheus.NewRegistry()

	runningJobGauge              prometheus.Gauge
	runningJobSyncStateGauges    *prometheus.GaugeVec
	runningJobSubSyncStateGauges *prometheus.GaugeVec
	runningJobLagGauges          *prometheus.GaugeVec
	runningJobLagSecondsGauges   *prometheus.GaugeVec

	errorCounters                *prometheus.CounterVec
	feRpcCounters                *prometheus.CounterVec
	feRpcHistograms              *prometheus.HistogramVec
	beRpcCounters                *prometheus.CounterVec
	beRpcHistograms              *prometheus.HistogramVec
	binlogGauges                 *prometheus.GaugeVec
	handleBinlogCounters         *prometheus.CounterVec
	handleBinlogHistograms       *prometheus.HistogramVec
	jobProgressPersistCounters   *prometheus.CounterVec
	jobProgressPersistHistograms *prometheus.HistogramVec

	sqlExecCounters   *prometheus.CounterVec
	sqlExecHistograms *prometheus.HistogramVec

	LargeBuckets = []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50}
)

func init() {
	registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	errorCounters = promauto.With(registry).NewCounterVec(prometheus.CounterOpts{
		Name: "ccr_error_total",
		Help: "The number of errors",
	}, []string{"error", "type"})

	feRpcCounters = promauto.With(registry).NewCounterVec(prometheus.CounterOpts{
		Name: "ccr_fe_rpc_total",
		Help: "The number of frontend rpc",
	}, []string{"method", "addr"})

	feRpcHistograms = promauto.With(registry).NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ccr_fe_rpc_duration_seconds",
		Help:    "The frontend rpc duration in seconds",
		Buckets: prometheus.DefBuckets,
	}, []string{"method", "addr"})

	beRpcCounters = promauto.With(registry).NewCounterVec(prometheus.CounterOpts{
		Name: "ccr_be_rpc_total",
		Help: "The number of backend rpc",
	}, []string{"method", "addr"})

	beRpcHistograms = promauto.With(registry).NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ccr_be_rpc_duration_seconds",
		Help:    "The backend rpc duration in seconds",
		Buckets: LargeBuckets,
	}, []string{"method", "addr"})

	binlogGauges = promauto.With(registry).NewGaugeVec(prometheus.GaugeOpts{
		Name: "ccr_binlog_commit_seq",
		Help: "The commit seq of the last binlog",
	}, []string{"name"})

	handleBinlogCounters = promauto.With(registry).NewCounterVec(prometheus.CounterOpts{
		Name: "ccr_handle_binlog_total",
		Help: "The number of handled binlog",
	}, []string{"name"})

	handleBinlogHistograms = promauto.With(registry).NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ccr_handle_binlog_duration_seconds",
		Help:    "The handle binlog duration in seconds",
		Buckets: LargeBuckets,
	}, []string{"name"})

	runningJobGauge = promauto.With(registry).NewGauge(prometheus.GaugeOpts{
		Name: "ccr_job_running_total",
		Help: "The number of running jobs",
	})

	runningJobSyncStateGauges = promauto.With(registry).NewGaugeVec(prometheus.GaugeOpts{
		Name: "ccr_job_running_sync_state",
		Help: "The sync state of running jobs",
	}, []string{"name"})

	runningJobSubSyncStateGauges = promauto.With(registry).NewGaugeVec(prometheus.GaugeOpts{
		Name: "ccr_job_running_sub_sync_state",
		Help: "The sub sync state of running jobs",
	}, []string{"name"})

	runningJobLagGauges = promauto.With(registry).NewGaugeVec(prometheus.GaugeOpts{
		Name: "ccr_job_running_lag_total",
		Help: "The lag of running jobs",
	}, []string{"name"})

	runningJobLagSecondsGauges = promauto.With(registry).NewGaugeVec(prometheus.GaugeOpts{
		Name: "ccr_job_running_lag_seconds",
		Help: "The lag of running jobs in seconds",
	}, []string{"name"})

	jobProgressPersistCounters = promauto.With(registry).NewCounterVec(prometheus.CounterOpts{
		Name: "ccr_job_progress_persist_total",
		Help: "The number of job progress persist",
	}, []string{"name"})

	jobProgressPersistHistograms = promauto.With(registry).NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ccr_job_progress_persist_duration_seconds",
		Help:    "The job progress persist duration in seconds",
		Buckets: prometheus.DefBuckets,
	}, []string{"name"})

	sqlExecCounters = promauto.With(registry).NewCounterVec(prometheus.CounterOpts{
		Name: "ccr_sql_exec_total",
		Help: "The number of sql exec",
	}, []string{"host", "db"})
	sqlExecHistograms = promauto.With(registry).NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ccr_sql_exec_duration_seconds",
		Help:    "The sql exec duration in seconds",
		Buckets: prometheus.DefBuckets,
	}, []string{"host", "db"})
}

func RecordError(err *xerror.XError) {
	errorCounters.With(ErrorLabels(err)).Inc()
}

func RecordFeRpc(method, addr string) func() {
	start := time.Now()
	return func() {
		feRpcCounters.With(prometheus.Labels{"method": method, "addr": addr}).Inc()
		feRpcHistograms.With(prometheus.Labels{"method": method, "addr": addr}).Observe(time.Since(start).Seconds())
	}
}

func RecordBeRpc(method, ip string, port uint16) func() {
	addr := net.JoinHostPort(ip, fmt.Sprintf("%d", port))
	start := time.Now()
	return func() {
		beRpcCounters.With(prometheus.Labels{"method": method, "addr": addr}).Inc()
		beRpcHistograms.With(prometheus.Labels{"method": method, "addr": addr}).Observe(time.Since(start).Seconds())
	}
}

func UpdateJobSyncState(jobName string, syncState, subSyncState int) {
	runningJobSyncStateGauges.With(prometheus.Labels{"name": jobName}).Set(float64(syncState))
	runningJobSubSyncStateGauges.With(prometheus.Labels{"name": jobName}).Set(float64(subSyncState))
}

func UpdateJobLag(jobName string, lag int64, interval float64) {
	runningJobLagGauges.With(prometheus.Labels{"name": jobName}).Set(float64(lag))
	runningJobLagSecondsGauges.With(prometheus.Labels{"name": jobName}).Set(interval)
}

func UpdateJobNum(num int) {
	runningJobGauge.Set(float64(num))
}

func RecordHandlingBinlog(jobName string, commitSeq int64) func() {
	binlogGauges.With(prometheus.Labels{"name": jobName}).Set(float64(commitSeq))
	handleBinlogCounters.With(prometheus.Labels{"name": jobName}).Inc()

	start := time.Now()
	return func() {
		handleBinlogHistograms.With(prometheus.Labels{"name": jobName}).Observe(time.Since(start).Seconds())
	}
}

func RecordJobProgressPersist(jobName string) func() {
	start := time.Now()
	return func() {
		jobProgressPersistCounters.With(prometheus.Labels{"name": jobName}).Inc()
		jobProgressPersistHistograms.With(prometheus.Labels{"name": jobName}).Observe(time.Since(start).Seconds())
	}
}

func RecordSqlExec(ip, port, db string) func() {
	host := net.JoinHostPort(ip, port)
	start := time.Now()
	return func() {
		sqlExecCounters.With(prometheus.Labels{"host": host, "db": db}).Inc()
		sqlExecHistograms.With(prometheus.Labels{"host": host, "db": db}).Observe(time.Since(start).Seconds())
	}
}

func GetHttpHandler() http.Handler {
	return promhttp.HandlerFor(registry, promhttp.HandlerOpts{Registry: registry})
}
