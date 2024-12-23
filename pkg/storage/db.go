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
package storage

import (
	"database/sql"
	"errors"
	"flag"
)

var (
	ErrJobExists    = errors.New("job exists")
	ErrJobNotExists = errors.New("job not exists")
)

const (
	InvalidCheckTimestamp int64  = -1
	remoteDBName          string = "ccr"
	defaultMaxOpenConns   int    = 20
	defaultMaxIdleConns   int    = 5
)

var maxOpenConns int
var maxAllowedPacket int64

func init() {
	flag.Int64Var(&maxAllowedPacket, "mysql_max_allowed_packet", defaultMaxAllowedPacket,
		"Config the max allowed packet to send to mysql server, the upper limit is 1GB")
	flag.IntVar(&maxOpenConns, "db_max_open_conns", defaultMaxOpenConns,
		"Config the max open connections for db user")
}

type DB interface {
	// Add ccr job
	AddJob(jobName string, jobInfo string, hostInfo string) error
	// Update ccr job
	UpdateJob(jobName string, jobInfo string) error
	// Remove ccr job
	RemoveJob(jobName string) error
	// Check Job exist
	IsJobExist(jobName string) (bool, error)
	// Get job_info
	GetJobInfo(jobName string) (string, error)
	// Get job_belong
	GetJobBelong(jobName string) (string, error)

	// Update ccr sync progress
	UpdateProgress(jobName string, progress string) error
	// IsProgressExist
	IsProgressExist(jobName string) (bool, error)
	// Get ccr sync progress
	GetProgress(jobName string) (string, error)

	// AddSyncer
	AddSyncer(hostInfo string) error
	// RefreshSyncer
	RefreshSyncer(hostInfo string, lastStamp int64) (int64, error)
	// GetStampAndJobs
	GetStampAndJobs(hostInfo string) (int64, []string, error)
	// GetOrphanJobs
	GetDeadSyncers(expiredTime int64) ([]string, error)
	// rebalance load
	RebalanceLoadFromDeadSyncers(syncers []string) error

	// GetAllData
	GetAllData() (map[string][]string, error)
}

func SetDBOptions(db *sql.DB) {
	db.SetMaxOpenConns(maxOpenConns)
	if maxOpenConns > 0 {
		db.SetMaxIdleConns(maxOpenConns / 4)
	} else {
		db.SetMaxIdleConns(defaultMaxIdleConns)
	}
}
