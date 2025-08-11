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
package service

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/selectdb/ccr_syncer/pkg/ccr"
	"github.com/selectdb/ccr_syncer/pkg/ccr/base"
	"github.com/selectdb/ccr_syncer/pkg/rpc"
	"github.com/selectdb/ccr_syncer/pkg/storage"
	"github.com/selectdb/ccr_syncer/pkg/utils"
	"github.com/selectdb/ccr_syncer/pkg/version"
	"github.com/selectdb/ccr_syncer/pkg/xerror"
	"github.com/selectdb/ccr_syncer/pkg/xmetrics"

	"github.com/olekukonko/tablewriter"
	log "github.com/sirupsen/logrus"
)

// TODO(Drogon): impl a generic http request handle parse json

func writeJson(w http.ResponseWriter, data interface{}) {
	// if exit in redirect, data == nil, do not write data
	if data == nil || (reflect.ValueOf(data).Kind() == reflect.Ptr && reflect.ValueOf(data).IsNil()) {
		return
	}

	if data, err := json.Marshal(data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	} else {
		w.Write(data)
	}
}

type defaultResult struct {
	Success  bool   `json:"success"`
	ErrorMsg string `json:"error_msg,omitempty"`
}

func newErrorResult(errMsg string) *defaultResult {
	return &defaultResult{
		Success:  false,
		ErrorMsg: errMsg,
	}
}

func newSuccessResult() *defaultResult {
	return &defaultResult{
		Success: true,
	}
}

type HttpService struct {
	port     int
	server   *http.Server
	mux      *http.ServeMux
	hostInfo string

	db         storage.DB
	jobManager *ccr.JobManager
}

func NewHttpServer(host string, port int, db storage.DB, jobManager *ccr.JobManager) *HttpService {
	return &HttpService{
		port:     port,
		mux:      http.NewServeMux(),
		hostInfo: fmt.Sprintf("%s:%d", host, port),

		db:         db,
		jobManager: jobManager,
	}
}

type CreateCcrRequest struct {
	// must need all fields required
	Name      string    `json:"name"`
	Src       base.Spec `json:"src"`
	Dest      base.Spec `json:"dest"`
	SkipError bool      `json:"skip_error"`
	// For table sync, allow to create ccr job even if the target table already exists.
	AllowTableExists bool `json:"allow_table_exists"`
	ReuseBinlogLabel bool `json:"reuse_binlog_label"`
	// 是否为集群级同步，如果为true，将获取源集群所有数据库并为每个数据库创建同步任务
	ClusterSync bool `json:"cluster_sync"`
}

// Stringer
func (r *CreateCcrRequest) String() string {
	return fmt.Sprintf("name: %s, src: %v, dest: %v", r.Name, r.Src, r.Dest)
}

// version Handler
// versionHandler handles the HTTP request for getting the version of the service.
// It returns the version as a JSON object with a "version" field.
func (s *HttpService) versionHandler(w http.ResponseWriter, r *http.Request) {
	// Log the request
	log.Infof("get version")

	// Define the version result struct
	type versionResult struct {
		Version string `json:"version"`
	}

	// Create the result object with the current version
	result := versionResult{Version: version.GetVersion()}
	writeJson(w, result)
}

// createCcr creates a new CCR job and adds it to the job manager.
// It takes a CreateCcrRequest as input and returns an error if there was a problem creating the job or adding it to the job manager.
func createCcr(request *CreateCcrRequest, db storage.DB, jobManager *ccr.JobManager) error {
	log.Infof("create ccr %s", request)

	ctx := &ccr.JobContext{
		Context:          context.Background(),
		Src:              request.Src,
		Dest:             request.Dest,
		SkipError:        request.SkipError,
		AllowTableExists: request.AllowTableExists,
		ReuseBinlogLabel: request.ReuseBinlogLabel,
		Db:               db,
		Factory:          jobManager.GetFactory(),
	}
	job, err := ccr.NewJobFromService(request.Name, ctx)
	if err != nil {
		return err
	}

	// add to job manager
	err = jobManager.AddJob(job)
	if err != nil {
		return err
	}

	return nil
}

// createClusterCcr 创建集群级别的CCR同步任务
// 获取源集群的所有数据库，为每个数据库创建一个同步任务
func createClusterCcr(request *CreateCcrRequest, db storage.DB, jobManager *ccr.JobManager) error {
	log.Infof("create cluster ccr %s", request)

	// 获取源集群的所有数据库列表
	databases, err := getDatabaseList(&request.Src)
	if err != nil {
		return xerror.Wrapf(err, xerror.Normal, "Failed to get database list from source cluster")
	}

	if len(databases) == 0 {
		return xerror.Errorf(xerror.Normal, "No databases found in source cluster")
	}

	log.Infof("Found %d databases, starting to create cluster-level sync tasks: %v", len(databases), databases)

	var errors []string
	successCount := 0

	for _, dbName := range databases {
		// 为每个数据库创建一个新的请求
		dbRequest := &CreateCcrRequest{
			Name:             fmt.Sprintf("%s_%s", request.Name, dbName), // 任务名称加上数据库名称
			Src:              request.Src,
			Dest:             request.Dest,
			SkipError:        request.SkipError,
			AllowTableExists: request.AllowTableExists,
			ReuseBinlogLabel: request.ReuseBinlogLabel,
			ClusterSync:      false, // 设置为false，避免递归调用
		}

		dbRequest.Src.Database = dbName
		dbRequest.Dest.Database = dbName

		if err := createCcr(dbRequest, db, jobManager); err != nil {
			errMsg := fmt.Sprintf("Failed to create sync task for database %s: %v", dbName, err)
			log.Warnf(errMsg)
			errors = append(errors, errMsg)
		} else {
			successCount++
			log.Infof("Successfully created sync task for database %s", dbName)
		}
	}

	if len(errors) > 0 {
		if successCount == 0 {
			return xerror.Errorf(xerror.Normal, "All database sync tasks creation failed: %s", strings.Join(errors, "; "))
		} else {
			log.Warnf("Partial cluster sync tasks creation failed, success: %d, failed: %d, errors: %s",
				successCount, len(errors), strings.Join(errors, "; "))
		}
	}

	log.Infof("Cluster-level sync tasks creation completed, success: %d, failed: %d", successCount, len(errors))

	// 启动守护任务，周期性检测源集群新增数据库，传入已有的数据库列表
	go startDatabaseMonitor(request, db, jobManager, databases)

	return nil
}

// startDatabaseMonitor 启动一个守护任务，周期性检测源集群新增和删除的数据库，并创建或删除对应的同步任务
func startDatabaseMonitor(request *CreateCcrRequest, db storage.DB, jobManager *ccr.JobManager, initialDatabases []string) {
	log.Infof("Starting database monitor daemon, task name prefix: %s", request.Name)

	existingDatabases := initializeDatabaseTracking(initialDatabases)
	log.Infof("Initialized database monitoring, currently have %d databases", len(existingDatabases))

	checkInterval := 2 * time.Minute
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	for range ticker.C {
		monitorDatabaseChanges(request, db, jobManager, existingDatabases)
	}
}

func initializeDatabaseTracking(initialDatabases []string) map[string]bool {
	existingDatabases := make(map[string]bool)
	for _, dbName := range initialDatabases {
		existingDatabases[dbName] = true
	}
	return existingDatabases
}

// monitorDatabaseChanges 检测数据库变化并处理
func monitorDatabaseChanges(request *CreateCcrRequest, db storage.DB, jobManager *ccr.JobManager, existingDatabases map[string]bool) {
	currentDatabases, err := request.Src.GetAllDatabases()
	if err != nil {
		log.Errorf("Failed to get database list: %v", err)
		return
	}

	currentDatabaseMap := make(map[string]bool)
	for _, dbName := range currentDatabases {
		if dbName == "" {
			continue
		}
		currentDatabaseMap[dbName] = true
	}

	newDatabases := identifyNewDatabases(currentDatabases, existingDatabases)
	deletedDatabases := identifyDeletedDatabases(existingDatabases, currentDatabaseMap)

	handleNewDatabases(newDatabases, request, db, jobManager)
	handleDeletedDatabases(deletedDatabases, request, jobManager)
	logMonitoringStatus(newDatabases, deletedDatabases, existingDatabases)
}

func identifyNewDatabases(currentDatabases []string, existingDatabases map[string]bool) []string {
	var newDatabases []string
	for _, dbName := range currentDatabases {
		if !existingDatabases[dbName] {
			newDatabases = append(newDatabases, dbName)
			existingDatabases[dbName] = true
		}
	}
	return newDatabases
}

func identifyDeletedDatabases(existingDatabases map[string]bool, currentDatabaseMap map[string]bool) []string {
	var deletedDatabases []string
	for dbName := range existingDatabases {
		if !currentDatabaseMap[dbName] {
			deletedDatabases = append(deletedDatabases, dbName)
			delete(existingDatabases, dbName)
		}
	}
	return deletedDatabases
}

func handleNewDatabases(newDatabases []string, request *CreateCcrRequest, db storage.DB, jobManager *ccr.JobManager) {
	if len(newDatabases) == 0 {
		return
	}

	log.Infof("Found %d new databases: %v", len(newDatabases), newDatabases)

	for _, dbName := range newDatabases {
		if dbName == "" {
			log.Warnf("Skipping empty database name")
			continue
		}

		jobName := fmt.Sprintf("%s_%s", request.Name, dbName)
		jobExists, err := db.IsJobExist(jobName)
		if err != nil {
			log.Warnf("Error checking if job %s exists: %v", jobName, err)
			continue
		}

		if jobExists {
			log.Warnf("Job %s already exists, skipping sync task creation for database %s", jobName, dbName)
			continue
		}

		dbRequest := &CreateCcrRequest{
			Name:             jobName,
			Src:              request.Src,
			Dest:             request.Dest,
			SkipError:        request.SkipError,
			AllowTableExists: request.AllowTableExists,
			ReuseBinlogLabel: request.ReuseBinlogLabel,
			ClusterSync:      false, // 设置为false，避免递归调用
		}

		dbRequest.Src.Database = dbName
		dbRequest.Dest.Database = dbName

		maxRetries := 3
		for i := 0; i < maxRetries; i++ {
			if err := createCcr(dbRequest, db, jobManager); err != nil {
				if i == maxRetries-1 {
					log.Warnf("Failed to create sync task for new database %s (attempt %d/%d): %v", dbName, i+1, maxRetries, err)
				} else {
					log.Warnf("Failed to create sync task for new database %s (attempt %d/%d): %v, will retry", dbName, i+1, maxRetries, err)
					time.Sleep(time.Second * time.Duration(i+1)) // 指数退避
				}
			} else {
				log.Infof("Successfully created sync task for new database %s", dbName)
				break
			}
		}
	}
}

func handleDeletedDatabases(deletedDatabases []string, request *CreateCcrRequest, jobManager *ccr.JobManager) {
	if len(deletedDatabases) == 0 {
		return
	}

	log.Infof("Found %d deleted databases: %v", len(deletedDatabases), deletedDatabases)

	for _, dbName := range deletedDatabases {
		if dbName == "" {
			log.Warnf("Skipping empty database name")
			continue
		}

		jobName := fmt.Sprintf("%s_%s", request.Name, dbName)

		maxRetries := 3
		for i := 0; i < maxRetries; i++ {
			if err := jobManager.RemoveJob(jobName); err != nil {
				if i == maxRetries-1 {
					log.Warnf("Failed to remove sync task for deleted database %s (attempt %d/%d): %v", dbName, i+1, maxRetries, err)
				} else {
					log.Warnf("Failed to remove sync task for deleted database %s (attempt %d/%d): %v, will retry", dbName, i+1, maxRetries, err)
					time.Sleep(time.Second * time.Duration(i+1)) // 指数退避
				}
			} else {
				log.Infof("Successfully removed sync task for deleted database %s", dbName)
				break
			}
		}
	}
}

// logMonitoringStatus 记录监控状态
func logMonitoringStatus(newDatabases []string, deletedDatabases []string, existingDatabases map[string]bool) {
	if len(newDatabases) == 0 && len(deletedDatabases) == 0 {
		log.Infof("No database changes detected, currently have %d databases", len(existingDatabases))
	}
}

func getDatabaseList(spec *base.Spec) ([]string, error) {
	log.Infof("Getting database list for cluster %s", spec.Host)

	// 使用Specer接口的GetAllDatabases方法
	databases, err := spec.GetAllDatabases()
	if err != nil {
		return nil, xerror.Wrapf(err, xerror.Normal, "Failed to get database list")
	}

	log.Infof("Got %d user databases: %v", len(databases), databases)
	return databases, nil
}

// return exit(bool)
func (s *HttpService) redirect(jobName string, w http.ResponseWriter, r *http.Request) bool {
	if jobExist, err := s.db.IsJobExist(jobName); err != nil {
		log.Warnf("get job %s exist failed: %+v, uri is %s", jobName, err, r.RequestURI)
		result := newErrorResult(err.Error())
		writeJson(w, result)
		return true
	} else if !jobExist {
		log.Warnf("job %s not exist, uri is %s", jobName, r.RequestURI)
		result := newErrorResult(fmt.Sprintf("job %s not exist", jobName))
		writeJson(w, result)
		return true
	}

	belongHost, err := s.db.GetJobBelong(jobName)
	if err != nil {
		log.Warnf("get job %s belong failed: %+v, uri is %s", jobName, err, r.RequestURI)
		result := newErrorResult(err.Error())
		writeJson(w, result)
		return true
	}

	if belongHost == s.hostInfo {
		return false
	}

	log.Infof("%s is located in syncer %s, please redirect to %s", jobName, belongHost, belongHost)
	redirectUrl := fmt.Sprintf("http://%s", belongHost+r.RequestURI)
	http.Redirect(w, r, redirectUrl, http.StatusSeeOther)
	log.Infof("the redirect url is %s", redirectUrl)
	return true
}

// HttpServer serving /create_ccr by json http rpc
func (s *HttpService) createHandler(w http.ResponseWriter, r *http.Request) {
	log.Infof("create ccr")

	var createResult *defaultResult
	defer func() { writeJson(w, createResult) }()

	// Parse the JSON request body
	var request CreateCcrRequest
	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		log.Warnf("create ccr failed: %+v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if request.ClusterSync {
		if err = createClusterCcr(&request, s.db, s.jobManager); err != nil {
			log.Warnf("create cluster ccr failed: %+v", err)
			createResult = newErrorResult(err.Error())
		} else {
			createResult = newSuccessResult()
		}
	} else {
		if err = createCcr(&request, s.db, s.jobManager); err != nil {
			log.Warnf("create ccr failed: %+v", err)
			createResult = newErrorResult(err.Error())
		} else {
			createResult = newSuccessResult()
		}
	}
}

type CcrCommonRequest struct {
	// must need all fields required
	Name string `json:"name"`
}

// GetLag service
func (s *HttpService) getLagHandler(w http.ResponseWriter, r *http.Request) {
	log.Infof("get lag")

	type result struct {
		*defaultResult
		Lag                  int64   `json:"lag"`
		FirstCommitSeq       int64   `json:"first_commit_seq"`
		LastCommitSeq        int64   `json:"last_commit_seq"`
		FirstBinlogTimestamp string  `json:"first_binlog_timestamp"`
		LastBinlogTimestamp  string  `json:"last_binlog_timestamp"`
		TimeInterval         float64 `json:"time_interval_secs"`
		NextCommitSeq        int64   `json:"next_commit_seq"`
		NextBinlogTimestamp  string  `json:"next_binlog_timestamp"`
	}
	var lagResult *result
	defer func() { writeJson(w, lagResult) }()

	// Parse the JSON request body
	var request CcrCommonRequest
	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		log.Warnf("get lag failed: %+v", err)

		lagResult = &result{
			defaultResult: newErrorResult(err.Error()),
		}
		return
	}

	if request.Name == "" {
		log.Warnf("get lag failed: name is empty")

		lagResult = &result{
			defaultResult: newErrorResult("name is empty"),
		}
		return
	}

	var job ccr.Job
	var jobProgress ccr.JobProgress

	jobInfo, err := s.db.GetJobInfo(request.Name)
	if err != nil {
		log.Warnf("db get job info failed: %+v", err)
		lagResult = &result{
			defaultResult: newErrorResult(err.Error()),
		}
		return
	}

	err = json.Unmarshal([]byte(jobInfo), &job)
	if err != nil {
		log.Warnf("unmarshal get job info failed: %+v", err)
		lagResult = &result{
			defaultResult: newErrorResult(err.Error()),
		}
		return
	}

	jobProgressData, err := s.db.GetProgress(request.Name)
	if err != nil {
		log.Warnf("db get job progress failed: %+v", err)
		lagResult = &result{
			defaultResult: newErrorResult(err.Error()),
		}
		return
	}

	err = json.Unmarshal([]byte(jobProgressData), &jobProgress)
	if err != nil {
		log.Warnf("unmarshal get job progress failed: %+v", err)
		lagResult = &result{
			defaultResult: newErrorResult(err.Error()),
		}
		return
	}

	srcSpec := &job.Src
	feRpc, err := rpc.NewFeRpc(srcSpec)
	if err != nil {
		log.Warnf("new fe rpc failed: %+v", err)
		lagResult = &result{
			defaultResult: newErrorResult(err.Error()),
		}
		return
	}

	commitSeq := jobProgress.CommitSeq
	resp, err := feRpc.GetBinlogLag(srcSpec, commitSeq)
	if err != nil {
		log.Warnf("rpc get binlog failed: %+v", err)
		lagResult = &result{
			defaultResult: newErrorResult(err.Error()),
		}
		return
	}

	lag := resp.GetLag()
	firstCommitSeq := resp.GetFirstCommitSeq()
	lastCommitSeq := resp.GetLastCommitSeq()
	nextCommitSeq := resp.GetNextCommitSeq()
	var nextBinlogTimestamp, lastBinlogTimestamp, firstBinlogTimestamp string

	if ts := resp.GetNextBinlogTimestamp(); ts != -1 {
		nextBinlogTimestamp = ConvertTimestampToString(ts)
	} else {
		nextBinlogTimestamp = "1970-01-01 08:00:00"
	}
	if ts := resp.GetLastBinlogTimestamp(); ts != -1 {
		lastBinlogTimestamp = ConvertTimestampToString(ts)
	} else {
		lastBinlogTimestamp = "1970-01-01 08:00:00"
	}
	if ts := resp.GetFirstBinlogTimestamp(); ts != -1 {
		firstBinlogTimestamp = ConvertTimestampToString(ts)
	} else {
		firstBinlogTimestamp = "1970-01-01 08:00:00"
	}

	timeInterval := CalculateTimeDifferenceInSeconds(lastBinlogTimestamp, nextBinlogTimestamp)

	lagResult = &result{
		defaultResult:        newSuccessResult(),
		Lag:                  lag,
		FirstCommitSeq:       firstCommitSeq,
		LastCommitSeq:        lastCommitSeq,
		FirstBinlogTimestamp: firstBinlogTimestamp,
		LastBinlogTimestamp:  lastBinlogTimestamp,
		TimeInterval:         timeInterval,
		NextCommitSeq:        nextCommitSeq,
		NextBinlogTimestamp:  nextBinlogTimestamp,
	}
}

// Pause service
func (s *HttpService) pauseHandler(w http.ResponseWriter, r *http.Request) {
	log.Infof("pause job")

	var pauseResult *defaultResult
	defer func() { writeJson(w, pauseResult) }()

	// Parse the JSON request body
	var request CcrCommonRequest
	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		log.Warnf("pause job failed: %+v", err)

		pauseResult = newErrorResult(err.Error())
		return
	}

	if request.Name == "" {
		log.Warnf("pause job failed: name is empty")

		pauseResult = newErrorResult("name is empty")
		return
	}

	if s.redirect(request.Name, w, r) {
		return
	}

	if err = s.jobManager.Pause(request.Name); err != nil {
		log.Warnf("pause job failed: %+v", err)

		pauseResult = newErrorResult(err.Error())
		return
	} else {
		pauseResult = newSuccessResult()
	}
}

// Resume service
func (s *HttpService) resumeHandler(w http.ResponseWriter, r *http.Request) {
	log.Infof("resume job")

	var resumeResult *defaultResult
	defer func() { writeJson(w, resumeResult) }()

	// Parse the JSON request body
	var request CcrCommonRequest
	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		log.Warnf("resume job failed: %+v", err)

		resumeResult = newErrorResult(err.Error())
		return
	}

	if request.Name == "" {
		log.Warnf("resume job failed: name is empty")

		resumeResult = newErrorResult("name is empty")
		return
	}

	if s.redirect(request.Name, w, r) {
		return
	}

	if err = s.jobManager.Resume(request.Name); err != nil {
		log.Warnf("resume job failed: %+v", err)

		resumeResult = newErrorResult(err.Error())
		return
	} else {
		resumeResult = newSuccessResult()
	}
}

func (s *HttpService) deleteHandler(w http.ResponseWriter, r *http.Request) {
	log.Infof("delete job")

	var deleteResult *defaultResult
	defer func() { writeJson(w, deleteResult) }()

	// Parse the JSON request body
	var request CcrCommonRequest
	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		log.Warnf("delete job failed: %+v", err)

		deleteResult = newErrorResult(err.Error())
		return
	}

	if request.Name == "" {
		log.Warnf("delete job failed: name is empty")

		deleteResult = newErrorResult("name is empty")
		return
	}

	if s.redirect(request.Name, w, r) {
		return
	}

	if err = s.jobManager.RemoveJob(request.Name); err != nil {
		log.Warnf("delete job failed: %+v", err)

		deleteResult = newErrorResult(err.Error())
		return
	} else {
		deleteResult = newSuccessResult()
	}
}

func (s *HttpService) statusHandler(w http.ResponseWriter, r *http.Request) {
	log.Infof("get job status")

	type result struct {
		*defaultResult
		JobStatus *ccr.JobStatus `json:"status,omitempty"`
	}
	var jobStatusResult *result
	defer func() { writeJson(w, jobStatusResult) }()

	// Parse the JSON request body
	var request CcrCommonRequest
	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		log.Warnf("get job status failed: %+v", err)

		jobStatusResult = &result{
			defaultResult: newErrorResult(err.Error()),
		}
		return
	}

	if request.Name == "" {
		log.Warnf("get job status failed: name is empty")

		jobStatusResult = &result{
			defaultResult: newErrorResult("name is empty"),
		}
		return
	}

	if s.redirect(request.Name, w, r) {
		return
	}

	if jobStatus, err := s.jobManager.GetJobStatus(request.Name); err != nil {
		log.Warnf("get job status failed: %+v", err)

		jobStatusResult = &result{
			defaultResult: newErrorResult(err.Error()),
		}
	} else {
		jobStatusResult = &result{
			defaultResult: newSuccessResult(),
			JobStatus:     jobStatus,
		}
	}
}

func (s *HttpService) desyncHandler(w http.ResponseWriter, r *http.Request) {
	log.Infof("desync job")

	var desyncResult *defaultResult
	defer func() { writeJson(w, desyncResult) }()

	// Parse the JSON request body
	var request CcrCommonRequest
	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		log.Warnf("desync job failed: %+v", err)

		desyncResult = newErrorResult(err.Error())
		return
	}

	if request.Name == "" {
		log.Warnf("desync job failed: name is empty")

		desyncResult = newErrorResult("name is empty")
		return
	}

	if s.redirect(request.Name, w, r) {
		return
	}

	if err := s.jobManager.Desync(request.Name); err != nil {
		log.Warnf("desync job failed: %+v", err)

		desyncResult = newErrorResult(err.Error())
	} else {
		desyncResult = newSuccessResult()
	}
}

func (s *HttpService) syncHandler(w http.ResponseWriter, r *http.Request) {
	log.Infof("sync job")

	var syncResult *defaultResult
	defer func() { writeJson(w, syncResult) }()

	// Parse the JSON request body
	var request CcrCommonRequest
	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		log.Warnf("sync job failed: %+v", err)

		syncResult = newErrorResult(err.Error())
		return
	}

	if request.Name == "" {
		log.Warnf("sync job failed: name is empty")

		syncResult = newErrorResult("name is empty")
		return
	}

	if s.redirect(request.Name, w, r) {
		return
	}

	if err := s.jobManager.Sync(request.Name); err != nil {
		log.Warnf("sync job failed: %+v", err)

		syncResult = newErrorResult(err.Error())
	} else {
		syncResult = newSuccessResult()
	}
}

func (s *HttpService) getAllJobs() ([]string, error) {
	var datum map[string][]string
	var err error
	// use GetAllData to get all jobs
	if datum, err = s.db.GetAllData(); err != nil {
		log.Warnf("when list jobs, get all data failed: %+v", err)
		return nil, err
	}
	allJobs := make([]string, 0)
	for _, eachJob := range datum["jobs"] {
		allJobs = append(allJobs, strings.Trim(strings.Split(eachJob, ",")[0], " "))
	}
	return allJobs, nil
}

// ListJobs service
func (s *HttpService) listJobsHandler(w http.ResponseWriter, r *http.Request) {
	log.Infof("list jobs")

	type result struct {
		*defaultResult
		Jobs []string `json:"jobs,omitempty"`
	}

	var jobResult *result
	defer func() { writeJson(w, jobResult) }()

	if allJobs, err := s.getAllJobs(); err != nil {
		log.Warnf("when list jobs, get all data failed: %+v", err)

		jobResult = &result{
			defaultResult: newErrorResult(err.Error()),
		}
	} else {
		jobResult = &result{
			defaultResult: newSuccessResult(),
			Jobs:          allJobs,
		}
	}
}

// show all jobs state
func (s *HttpService) showJobStateHandler(w http.ResponseWriter, r *http.Request) {
	log.Infof("show job state")

	var result string

	defer func() { w.Write([]byte(result)) }()

	allJobs, err := s.getAllJobs()
	if err != nil {
		log.Warnf("when show jobs state, get all data failed: %+v", err)
		result = err.Error()
		return
	}
	data := [][]string{}
	for _, jobName := range allJobs {
		line := []string{}
		// job name
		line = append(line, jobName)
		// job type
		var job *ccr.Job

		jobInfo, err := s.db.GetJobInfo(jobName)
		if err != nil {
			log.Warnf("db get job info failed: %+v", err)
			result = err.Error()
			return
		}

		err = json.Unmarshal([]byte(jobInfo), &job)
		if err != nil {
			log.Warnf("unmarshal get job info failed: %+v", err)
			result = err.Error()
			return
		}

		if job.IsTableSyncWithAlias() {
			line = append(line, "table_sync_with_alias")
		} else {
			line = append(line, job.SyncType.String())
		}

		// lag
		var jobProgress ccr.JobProgress
		if jobProgressData, err := s.db.GetProgress(jobName); err != nil {
			log.Warnf("get job progress failed: %+v", err)
			result = err.Error()
			return
		} else {
			err := json.Unmarshal([]byte(jobProgressData), &jobProgress)
			if err != nil {
				log.Warnf("unmarshal get job progress error")
				result = err.Error()
				return
			}
			jobProgress.PersistData = ""
		}
		srcSpec := &job.Src
		feRpc, err := rpc.NewFeRpc(srcSpec)
		if err != nil {
			log.Warnf("new fe rpc failed: %+v", err)
			result = err.Error()
			return
		}

		commitSeq := jobProgress.CommitSeq
		resp, err := feRpc.GetBinlogLag(srcSpec, commitSeq)
		if err != nil {
			log.Warnf("rpc get binlog failed: %+v", err)
			result = err.Error()
			return
		}

		lag := resp.GetLag()
		line = append(line, fmt.Sprintf("%v", lag))
		// lag(secs)
		var lastBinlogTimestamp, firstBinlogTimestamp string

		if ts := resp.GetLastBinlogTimestamp(); ts != -1 {
			lastBinlogTimestamp = ConvertTimestampToString(ts)
		} else {
			lastBinlogTimestamp = "1970-01-01 08:00:00"
		}
		if ts := resp.GetFirstBinlogTimestamp(); ts != -1 {
			firstBinlogTimestamp = ConvertTimestampToString(ts)
		} else {
			firstBinlogTimestamp = "1970-01-01 08:00:00"
		}

		totalTime := CalculateTimeDifferenceInSeconds(lastBinlogTimestamp, firstBinlogTimestamp)
		line = append(line, fmt.Sprintf("%v", lag/int64(totalTime)))
		// sync state
		line = append(line, jobProgress.SyncState.String())
		// sub sync state
		line = append(line, jobProgress.SubSyncState.String())
		// last fullsync time
		line = append(line, fmt.Sprintf("%v", jobProgress.FullSyncStartAt))
		// last fullsync reason
		line = append(line, jobProgress.FullSyncInfo.Info)
		// is hostmapping
		line = append(line, fmt.Sprintf("%v", len(job.Dest.HostMapping)+len(job.Src.HostMapping) != 0))
		// other args
		line = append(line, "")
		// append to data
		data = append(data, line)
	}
	// the type default is html
	if r.URL.RawQuery == "" {
		r.URL.RawQuery = "type=html"
	}
	header := []string{"Job Name", "Type", "Lag", "Lag(Secs)", "Sync State", "Sub Sync State", "Last Fullsync Time", "Last Fullsync Reason", "Is Hostmapping", "Other Args"}
	var sb strings.Builder
	switch r.URL.RawQuery {
	case "type=html":
		sb.WriteString("<!DOCTYPE html>\n")
		sb.WriteString("<html>\n<head>\n<title>Jobs State</title>\n</head>\n<body>\n")
		sb.WriteString("<table border='1' style='border-collapse: collapse;'>\n")
		sb.WriteString("<tr><th>" + strings.Join(header, "</th><th>") + "</th></tr>\n")

		for _, states := range data {
			sb.WriteString("<tr><td>" + strings.Join(states, "</td><td>") + "</td></tr>\n")
		}

		sb.WriteString("</table>\n</body>\n</html>\n")
	case "type=table":
		// using table writer to render the table
		table := tablewriter.NewWriter(&sb)
		table.SetHeader(header)
		table.SetRowLine(true)
		for _, line := range data {
			table.Append(line)
		}
		table.Render()
	case "type=raw":
		// raw type output would using tab
		sb.WriteString(strings.Join(header, "\t") + "\n")
		for _, states := range data {
			sb.WriteString(strings.Join(states, "\t") + "\n")
		}
	default:
		log.Warnf("show job state with unknow type: %+v", r.URL.RawQuery)
		result = fmt.Sprintf("show job state with unknow type: %+v", r.URL.RawQuery)
		return
	}
	result = sb.String()
}

// get job progress
func (s *HttpService) jobProgressHandler(w http.ResponseWriter, r *http.Request) {
	log.Infof("get job progress")

	type result struct {
		*defaultResult
		JobProgress ccr.JobProgress `json:"job_progress"`
	}

	var jobResult *result
	defer func() { writeJson(w, jobResult) }()

	// Parse the JSON request body
	var request CcrCommonRequest
	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		log.Warnf("get job progress failed: %+v", err)

		jobResult = &result{
			defaultResult: newErrorResult(err.Error()),
		}
		return
	}

	if request.Name == "" {
		log.Warnf("get job progress failed: name is empty")

		jobResult = &result{
			defaultResult: newErrorResult("name is empty"),
		}
		return
	}

	if s.redirect(request.Name, w, r) {
		return
	}

	if jobProgressData, err := s.db.GetProgress(request.Name); err != nil {
		log.Warnf("get job progress failed: %+v", err)
		jobResult = &result{
			defaultResult: newErrorResult(err.Error()),
		}
	} else {
		var jobProgress ccr.JobProgress
		err := json.Unmarshal([]byte(jobProgressData), &jobProgress)
		if err != nil {
			log.Warnf("unmarshal get job progress error")
			jobResult = &result{
				defaultResult: newErrorResult(err.Error()),
			}
			return
		}
		jobProgress.PersistData = ""
		jobResult = &result{
			defaultResult: newSuccessResult(),
			JobProgress:   jobProgress,
		}
	}

}

// get job details
func (s *HttpService) jobDetailHandler(w http.ResponseWriter, r *http.Request) {
	log.Infof("get job detail")

	type result struct {
		*defaultResult
		JobDetail *ccr.Job `json:"job_detail"`
	}

	var jobResult *result
	defer func() { writeJson(w, jobResult) }()

	// Parse the JSON request body
	var request CcrCommonRequest
	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		log.Warnf("get job detail failed: %+v", err)

		jobResult = &result{
			defaultResult: newErrorResult(err.Error()),
		}
		return
	}

	if request.Name == "" {
		log.Warnf("get job detail failed: name is empty")

		jobResult = &result{
			defaultResult: newErrorResult("name is empty"),
		}
		return
	}

	if s.redirect(request.Name, w, r) {
		return
	}

	var jobDetail ccr.Job
	if jobDetailStr, err := s.db.GetJobInfo(request.Name); err != nil {
		log.Warnf("get job info failed: %+v", err)
		jobResult = &result{
			defaultResult: newErrorResult(err.Error()),
		}
	} else if err = json.Unmarshal([]byte(jobDetailStr), &jobDetail); err != nil {
		log.Warnf("unmarshal job info failed: %+v", err)
		jobResult = &result{
			defaultResult: newErrorResult(err.Error()),
		}
	} else {
		jobResult = &result{
			defaultResult: newSuccessResult(),
			JobDetail:     &jobDetail,
		}
	}
}

func (s *HttpService) forceFullsyncHandler(w http.ResponseWriter, r *http.Request) {
	log.Infof("force job fullsync")

	var result *defaultResult
	defer func() { writeJson(w, result) }()

	// Parse the JSON request body
	var request CcrCommonRequest
	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		log.Warnf("force job fullsync failed: %+v", err)
		result = newErrorResult(err.Error())
		return
	}

	if request.Name == "" {
		log.Warnf("force job fullsync: name is empty")
		result = newErrorResult("job name is empty")
		return
	}

	if s.redirect(request.Name, w, r) {
		return
	}

	params := ccr.SkipBinlogParams{SkipBy: ccr.SkipByFullSync}
	if err := s.jobManager.SkipBinlog(request.Name, params); err != nil {
		log.Warnf("force fullsync failed: %+v", err)
		result = newErrorResult(err.Error())
	} else {
		result = newSuccessResult()
	}
}

func (s *HttpService) featuresHandler(w http.ResponseWriter, r *http.Request) {
	type flagValue struct {
		Feature  string `json:"feature"`
		Value    bool   `json:"value"`
		DefValue string `json:"default"`
	}
	type flagListResult struct {
		*defaultResult
		Flags []flagValue `json:"flags"`
	}

	var result flagListResult
	result.defaultResult = newSuccessResult()
	defer func() { writeJson(w, &result) }()

	flag.VisitAll(func(flag *flag.Flag) {
		if !strings.HasPrefix(flag.Name, "feature") {
			return
		}

		valueStr := flag.Value.String()
		value, err := strconv.ParseBool(valueStr)
		if err != nil {
			// ignore any non-bool flags
			return
		}

		result.Flags = append(result.Flags, flagValue{
			Feature: flag.Name, Value: value, DefValue: flag.DefValue,
		})
	})
}

func (s *HttpService) updateHostMappingHandler(w http.ResponseWriter, r *http.Request) {
	log.Infof("update host mapping")

	var result *defaultResult
	defer func() { writeJson(w, result) }()

	// Parse the JSON request body
	var request struct {
		CcrCommonRequest
		SrcHostMapping  map[string]string `json:"src_host_mapping"`
		DestHostMapping map[string]string `json:"dest_host_mapping"`
	}
	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		log.Warnf("update host mapping failed: %+v", err)
		result = newErrorResult(err.Error())
		return
	}

	if request.Name == "" {
		log.Warnf("update host mapping failed: name is empty")
		result = newErrorResult("name is empty")
		return
	}

	if len(request.SrcHostMapping) == 0 && len(request.DestHostMapping) == 0 {
		log.Warnf("update host mapping failed: src/dest_host_mapping is empty")
		result = newErrorResult("host_mapping is empty")
		return
	}

	if err := s.jobManager.UpdateHostMapping(request.Name, request.SrcHostMapping, request.DestHostMapping); err != nil {
		log.Warnf("update host mapping failed: %+v", err)
		result = newErrorResult(err.Error())
	} else {
		result = newSuccessResult()
	}
}

func (s *HttpService) skipBinlogHandler(w http.ResponseWriter, r *http.Request) {
	var result *defaultResult
	defer func() { writeJson(w, result) }()

	// Parse the JSON request body
	var request struct {
		CcrCommonRequest
		SkipCommitSeq int64  `json:"skip_commit_seq"`
		SkipBy        string `json:"skip_by"`
		SkipTable     string `json:"skip_table"`
		SkipTableId   int64  `json:"skip_table_id"`
	}
	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		log.Warnf("skip binlog failed: %+v", err)
		result = newErrorResult(err.Error())
		return
	}

	if request.Name == "" {
		log.Warnf("skip binlog failed: name is empty")
		result = newErrorResult("name is empty")
		return
	}

	if s.redirect(request.Name, w, r) {
		return
	}

	log.Infof("skip binlog with %s, commit seq %d, skip table %s, skip table id %d, job %s",
		request.SkipBy, request.SkipCommitSeq, request.SkipTable, request.SkipTableId, request.Name)
	params := ccr.SkipBinlogParams{
		SkipBy:        strings.ToLower(request.SkipBy),
		SkipCommitSeq: request.SkipCommitSeq,
		SkipTable:     request.SkipTable,
		SkipTableId:   request.SkipTableId,
	}
	if err := s.jobManager.SkipBinlog(request.Name, params); err != nil {
		log.Warnf("skip binlog failed: %+v", err)
		result = newErrorResult(err.Error())
	} else {
		result = newSuccessResult()
	}
}

func (s *HttpService) failpointHandler(w http.ResponseWriter, r *http.Request) {
	log.Infof("inject failpoint")

	var result *defaultResult
	defer func() { writeJson(w, result) }()

	// Parse the JSON request body
	var request struct {
		Name      string      `json:"name"` // the ccr job name
		Failpoint string      `json:"failpoint"`
		Value     interface{} `json:"value"`
	}
	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		log.Warnf("inject failpoint failed: %+v", err)
		result = newErrorResult(err.Error())
		return
	}

	if request.Name == "" {
		log.Warnf("inject failpoint failed: job name is empty")
		result = newErrorResult("job name is empty")
		return
	} else if request.Failpoint == "" {
		log.Infof("disable all failpoints")
		utils.DisableFailpoint()
	} else if request.Value != nil {
		log.Infof("inject failpoint %s with value %+v, job %s",
			request.Failpoint, request.Value, request.Name)
		utils.InjectJobFailpoint(request.Name, request.Failpoint, request.Value)
		if !utils.IsFailpointEnabled() {
			utils.EnableFailpoint()
		}
	} else {
		utils.RemoveJobFailpoint(request.Name, request.Failpoint)
	}

	result = newSuccessResult()
}

func (s *HttpService) RegisterHandlers() {
	s.mux.HandleFunc("/version", s.versionHandler)
	s.mux.HandleFunc("/create_ccr", s.createHandler)
	s.mux.HandleFunc("/pause", s.pauseHandler)
	s.mux.HandleFunc("/resume", s.resumeHandler)
	s.mux.HandleFunc("/delete", s.deleteHandler)
	s.mux.HandleFunc("/desync", s.desyncHandler)
	s.mux.HandleFunc("/get_lag", s.getLagHandler)
	s.mux.HandleFunc("/list_jobs", s.listJobsHandler)
	s.mux.HandleFunc("/job_detail", s.jobDetailHandler)
	s.mux.HandleFunc("/job_status", s.statusHandler)
	s.mux.HandleFunc("/job_progress", s.jobProgressHandler)
	s.mux.HandleFunc("/force_fullsync", s.forceFullsyncHandler)
	s.mux.HandleFunc("/features", s.featuresHandler)
	s.mux.HandleFunc("/update_host_mapping", s.updateHostMappingHandler)
	s.mux.HandleFunc("/job_skip_binlog", s.skipBinlogHandler)
	s.mux.HandleFunc("/failpoint", s.failpointHandler)
	s.mux.Handle("/metrics", xmetrics.GetHttpHandler())
	s.mux.HandleFunc("/sync", s.syncHandler)
	s.mux.HandleFunc("/view", s.showJobStateHandler)
}

func (s *HttpService) Start() error {
	addr := fmt.Sprintf(":%d", s.port)
	log.Infof("Server listening on %s", addr)

	s.mux = http.NewServeMux()
	s.RegisterHandlers()

	s.server = &http.Server{Addr: addr, Handler: s.mux}
	err := s.server.ListenAndServe()
	if err == nil {
		return nil
	} else if err == http.ErrServerClosed {
		log.Info("http server closed")
		return nil
	} else {
		return xerror.Wrapf(err, xerror.Normal, "http server start on %s failed", addr)
	}
}

// Stop stops the HTTP server gracefully.
// It returns an error if the server shutdown fails.
func (s *HttpService) Stop() error {
	if err := s.server.Shutdown(context.TODO()); err != nil {
		return xerror.Wrapf(err, xerror.Normal, "http server close failed")
	}
	return nil
}

func ConvertTimestampToString(timestamp int64) string {
	return time.Unix(0, timestamp*int64(time.Millisecond)).Format(time.DateTime)
}

func CalculateTimeDifferenceInSeconds(timeStr1, timeStr2 string) float64 {
	t1, _ := time.Parse(time.DateTime, timeStr1)
	t2, _ := time.Parse(time.DateTime, timeStr2)
	diff := t1.Sub(t2)
	return diff.Seconds()
}
