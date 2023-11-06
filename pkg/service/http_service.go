package service

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/selectdb/ccr_syncer/pkg/ccr"
	"github.com/selectdb/ccr_syncer/pkg/ccr/base"
	"github.com/selectdb/ccr_syncer/pkg/storage"
	"github.com/selectdb/ccr_syncer/pkg/version"
	"github.com/selectdb/ccr_syncer/pkg/xerror"

	log "github.com/sirupsen/logrus"
)

// TODO(Drogon): impl a generic http request handle parse json

func writeJson(w http.ResponseWriter, data interface{}) {
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
	Name string    `json:"name,required"`
	Src  base.Spec `json:"src,required"`
	Dest base.Spec `json:"dest,required"`
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

	// _job
	ctx := ccr.NewJobContext(request.Src, request.Dest, db, jobManager.GetFactory())
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

func (s *HttpService) isRedirected(jobName string, w http.ResponseWriter) (bool, error) {
	belongHost, err := s.db.GetJobBelong(jobName)
	if err != nil {
		return false, err
	}

	if belongHost != s.hostInfo {
		w.Write([]byte(fmt.Sprintf("%s is located in syncer %s, please redirect to %s", jobName, belongHost, belongHost)))
		return true, nil
	}

	return false, nil
}

// HttpServer serving /create_ccr by json http rpc
func (s *HttpService) createHandler(w http.ResponseWriter, r *http.Request) {
	log.Infof("create ccr")

	var createResult *defaultResult
	defer writeJson(w, &createResult)

	// Parse the JSON request body
	var request CreateCcrRequest
	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Call the createCcr function to create the CCR
	if err = createCcr(&request, s.db, s.jobManager); err != nil {
		log.Errorf("create ccr failed: %+v", err)
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
		Lag int64 `json:"lag,omitempty"`
	}
	var lagResult *result
	defer writeJson(w, &lagResult)

	// Parse the JSON request body
	var request CcrCommonRequest
	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		lagResult = &result{
			defaultResult: newErrorResult(err.Error()),
		}
		return
	}

	if request.Name == "" {
		lagResult = &result{
			defaultResult: newErrorResult("name is empty"),
		}
		return
	}

	if isRedirected, err := s.isRedirected(request.Name, w); err != nil {
		lagResult = &result{
			defaultResult: newErrorResult(err.Error()),
		}
		return
	} else if isRedirected {
		return
	}

	lag, err := s.jobManager.GetLag(request.Name)
	if err != nil {
		lagResult = &result{
			defaultResult: newErrorResult(err.Error()),
		}
		return
	}

	lagResult = &result{
		defaultResult: newSuccessResult(),
		Lag:           lag,
	}
}

// Pause service
func (s *HttpService) pauseHandler(w http.ResponseWriter, r *http.Request) {
	log.Infof("pause job")

	var pauseResult *defaultResult
	defer writeJson(w, &pauseResult)

	// Parse the JSON request body
	var request CcrCommonRequest
	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		pauseResult = newErrorResult(err.Error())
		return
	}

	if request.Name == "" {
		pauseResult = newErrorResult("name is empty")
		return
	}

	if isRedirected, err := s.isRedirected(request.Name, w); err != nil {
		pauseResult = newErrorResult(err.Error())
	} else if isRedirected {
		return
	}

	err = s.jobManager.Pause(request.Name)
	if err != nil {
		pauseResult = newErrorResult(err.Error())
		return
	}

	pauseResult = newSuccessResult()
}

// Resume service
func (s *HttpService) resumeHandler(w http.ResponseWriter, r *http.Request) {
	log.Infof("resume job")

	var resumeResult *defaultResult
	defer writeJson(w, &resumeResult)

	// Parse the JSON request body
	var request CcrCommonRequest
	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		resumeResult = newErrorResult(err.Error())
		return
	}

	if request.Name == "" {
		resumeResult = newErrorResult("name is empty")
		return
	}

	if isRedirected, err := s.isRedirected(request.Name, w); err != nil {
		resumeResult = newErrorResult(err.Error())
	} else if isRedirected {
		return
	}

	err = s.jobManager.Resume(request.Name)
	if err != nil {
		resumeResult = newErrorResult(err.Error())
		return
	}

	resumeResult = newSuccessResult()
}

func (s *HttpService) deleteHandler(w http.ResponseWriter, r *http.Request) {
	log.Infof("delete job")

	var deleteResult *defaultResult
	defer writeJson(w, &deleteResult)

	// Parse the JSON request body
	var request CcrCommonRequest
	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		deleteResult = newErrorResult(err.Error())
		return
	}

	if request.Name == "" {
		deleteResult = newErrorResult("name is empty")
		return
	}

	if isRedirected, err := s.isRedirected(request.Name, w); err != nil {
		deleteResult = newErrorResult(err.Error())
	} else if isRedirected {
		return
	}

	err = s.jobManager.RemoveJob(request.Name)
	if err != nil {
		deleteResult = newErrorResult(err.Error())
		return
	}

	deleteResult = newSuccessResult()
}

func (s *HttpService) statusHandler(w http.ResponseWriter, r *http.Request) {
	log.Infof("get job status")

	type result struct {
		*defaultResult
		JobStatus *ccr.JobStatus `json:"status,omitempty"`
	}
	var jobStatusResult *result
	defer writeJson(w, &jobStatusResult)

	// Parse the JSON request body
	var request CcrCommonRequest
	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		jobStatusResult = &result{
			defaultResult: newErrorResult(err.Error()),
		}
		return
	}

	if request.Name == "" {
		jobStatusResult = &result{
			defaultResult: newErrorResult("name is empty"),
		}
		return
	}

	if isRedirected, err := s.isRedirected(request.Name, w); err != nil {
		jobStatusResult = &result{
			defaultResult: newErrorResult(err.Error()),
		}
		return
	} else if isRedirected {
		return
	}

	if jobStatus, err := s.jobManager.GetJobStatus(request.Name); err != nil {
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
	defer writeJson(w, &desyncResult)

	// Parse the JSON request body
	var request CcrCommonRequest
	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		desyncResult = newErrorResult(err.Error())
		writeJson(w, desyncResult)
		return
	}

	if request.Name == "" {
		desyncResult = newErrorResult("name is empty")
		return
	}

	if isRedirected, err := s.isRedirected(request.Name, w); err != nil {
		desyncResult = newErrorResult(err.Error())
		return
	} else if isRedirected {
		return
	}

	if err := s.jobManager.Desync(request.Name); err != nil {
		desyncResult = newErrorResult(err.Error())
	} else {
		desyncResult = newSuccessResult()
	}
}

// ListJobs service
func (s *HttpService) listJobsHandler(w http.ResponseWriter, r *http.Request) {
	log.Infof("list jobs")

	type result struct {
		Jobs []*ccr.JobStatus `json:"jobs"`
	}

	jobs := s.jobManager.ListJobs()
	jobResult := result{Jobs: jobs}
	writeJson(w, jobResult)
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
	s.mux.HandleFunc("/list_jobs", s.listJobsHandler)
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
