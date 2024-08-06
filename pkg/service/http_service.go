package service

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strings"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/selectdb/ccr_syncer/pkg/ccr"
	"github.com/selectdb/ccr_syncer/pkg/ccr/base"
	"github.com/selectdb/ccr_syncer/pkg/storage"
	"github.com/selectdb/ccr_syncer/pkg/version"
	"github.com/selectdb/ccr_syncer/pkg/xerror"

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
	Name      string    `json:"name,required"`
	Src       base.Spec `json:"src,required"`
	Dest      base.Spec `json:"dest,required"`
	SkipError bool      `json:"skip_error"`
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
	type vesionResult struct {
		Version string `json:"version"`
	}

	// Create the result object with the current version
	result := vesionResult{Version: version.GetVersion()}
	writeJson(w, result)
}

// createCcr creates a new CCR job and adds it to the job manager.
// It takes a CreateCcrRequest as input and returns an error if there was a problem creating the job or adding it to the job manager.
func createCcr(request *CreateCcrRequest, db storage.DB, jobManager *ccr.JobManager) error {
	log.Infof("create ccr %s", request)

	ctx := ccr.NewJobContext(request.Src, request.Dest, request.SkipError, db, jobManager.GetFactory())
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

	// Call the createCcr function to create the CCR
	if err = createCcr(&request, s.db, s.jobManager); err != nil {
		log.Warnf("create ccr failed: %+v", err)
		createResult = newErrorResult(err.Error())
	} else {
		createResult = newSuccessResult()
	}
}

type CcrCommonRequest struct {
	// must need all fields required
	Name string `json:"name,required"`
}

// GetLag service
func (s *HttpService) getLagHandler(w http.ResponseWriter, r *http.Request) {
	log.Infof("get lag")

	type result struct {
		*defaultResult
		Lag int64 `json:"lag"`
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

	if exit := s.redirect(request.Name, w, r); exit {
		return
	}

	if lag, err := s.jobManager.GetLag(request.Name); err != nil {
		log.Warnf("get lag failed: %+v", err)

		lagResult = &result{
			defaultResult: newErrorResult(err.Error()),
		}
	} else {
		lagResult = &result{
			defaultResult: newSuccessResult(),
			Lag:           lag,
		}
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

type UpdateJobRequest struct {
	Name      string `json:"name,required"`
	SkipError bool   `json:"skip_error"`
}

func (s *HttpService) updateJobHandler(w http.ResponseWriter, r *http.Request) {
	log.Infof("update job")

	var updateJobResult *defaultResult
	defer func() { writeJson(w, updateJobResult) }()

	// Parse the JSON request body
	var request UpdateJobRequest
	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		log.Warnf("update job failed: %+v", err)

		updateJobResult = newErrorResult(err.Error())
		return
	}

	if request.Name == "" {
		log.Warnf("update job failed: name is empty")

		updateJobResult = newErrorResult("name is empty")
		return
	}

	if s.redirect(request.Name, w, r) {
		return
	}

	if err := s.jobManager.UpdateJobSkipError(request.Name, request.SkipError); err != nil {
		log.Warnf("desync job failed: %+v", err)

		updateJobResult = newErrorResult(err.Error())
	} else {
		updateJobResult = newSuccessResult()
	}
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

	// use GetAllData to get all jobs
	if ans, err := s.db.GetAllData(); err != nil {
		log.Warnf("when list jobs, get all data failed: %+v", err)

		jobResult = &result{
			defaultResult: newErrorResult(err.Error()),
		}
	} else {
		var jobData []string
		jobData = ans["jobs"]
		allJobs := make([]string, 0)
		for _, eachJob := range jobData {
			allJobs = append(allJobs, strings.Trim(strings.Split(eachJob, ",")[0], " "))
		}

		jobResult = &result{
			defaultResult: newSuccessResult(),
			Jobs:          allJobs,
		}
	}
}

// get job progress
func (s *HttpService) jobProgressHandler(w http.ResponseWriter, r *http.Request) {
	log.Infof("get job progress")

	type result struct {
		*defaultResult
		JobProgress string `json:"job_progress"`
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

	if jobProgress, err := s.db.GetProgress(request.Name); err != nil {
		log.Warnf("get job progress failed: %+v", err)
		jobResult = &result{
			defaultResult: newErrorResult(err.Error()),
		}
	} else {
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
		JobDetail string `json:"job_detail"`
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

	if jobDetail, err := s.db.GetJobInfo(request.Name); err != nil {
		log.Warnf("get job info failed: %+v", err)

		jobResult = &result{
			defaultResult: newErrorResult(err.Error()),
		}
	} else {
		jobResult = &result{
			defaultResult: newSuccessResult(),
			JobDetail:     jobDetail,
		}
	}
}

func (s *HttpService) RegisterHandlers() {
	s.mux.HandleFunc("/version", s.versionHandler)
	s.mux.HandleFunc("/create_ccr", s.createHandler)
	s.mux.HandleFunc("/get_lag", s.getLagHandler)
	s.mux.HandleFunc("/pause", s.pauseHandler)
	s.mux.HandleFunc("/resume", s.resumeHandler)
	s.mux.HandleFunc("/delete", s.deleteHandler)
	s.mux.HandleFunc("/job_status", s.statusHandler)
	s.mux.HandleFunc("/desync", s.desyncHandler)
	s.mux.HandleFunc("/update_job", s.updateJobHandler)
	s.mux.HandleFunc("/list_jobs", s.listJobsHandler)
	s.mux.HandleFunc("/job_detail", s.jobDetailHandler)
	s.mux.HandleFunc("/job_progress", s.jobProgressHandler)
	s.mux.Handle("/metrics", promhttp.Handler())
}

func (s *HttpService) Start() error {
	addr := fmt.Sprintf(":%d", s.port)
	log.Infof("Server listening on %s", addr)

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
