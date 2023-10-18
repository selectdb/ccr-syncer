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

func writeJson(w http.ResponseWriter, data interface{}) {
	if data, err := json.Marshal(data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	} else {
		w.Write(data)
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
func (s *HttpService) createCcr(request *CreateCcrRequest) error {
	log.Infof("create ccr %s", request)

	// _job
	ctx := ccr.NewJobContext(request.Src, request.Dest, s.db, s.jobManager.GetFactory())
	job, err := ccr.NewJobFromService(request.Name, ctx)
	if err != nil {
		return err
	}

	// add to job manager
	err = s.jobManager.AddJob(job)
	if err != nil {
		return err
	}

	return nil
}

func (s *HttpService) isRedirected(jobName string, w http.ResponseWriter) (bool, error) {
	belong, err := s.db.GetJobBelong(jobName)
	if err != nil {
		return false, err
	}

	if belong != s.hostInfo {
		w.Write([]byte(fmt.Sprintf("%s is located in syncer %s, please redirect to %s", jobName, belong, belong)))
		return true, nil
	}

	return false, nil
}

// HttpServer serving /create_ccr by json http rpc
func (s *HttpService) createHandler(w http.ResponseWriter, r *http.Request) {
	log.Infof("create ccr")

	// Parse the JSON request body
	var request CreateCcrRequest
	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Call the createCcr function to create the CCR
	err = s.createCcr(&request)
	if err != nil {
		log.Errorf("create ccr failed: %+v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	type result struct {
		Success bool `json:"success"`
	}
	createResult := result{Success: true}
	writeJson(w, createResult)
}

type CcrCommonRequest struct {
	// must need all fields required
	Name string `json:"name,required"`
}

// GetLag service
func (s *HttpService) getLagHandler(w http.ResponseWriter, r *http.Request) {
	log.Infof("get lag")

	// Parse the JSON request body
	var request CcrCommonRequest
	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if request.Name == "" {
		http.Error(w, "name is empty", http.StatusBadRequest)
		return
	}

	if isRedirected, err := s.isRedirected(request.Name, w); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	} else if isRedirected {
		return
	}

	lag, err := s.jobManager.GetLag(request.Name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	type result struct {
		Lag int64 `json:"lag"`
	}
	lagResult := result{Lag: lag}
	writeJson(w, lagResult)
}

// Pause service
func (s *HttpService) pauseHandler(w http.ResponseWriter, r *http.Request) {
	log.Infof("pause job")

	// Parse the JSON request body
	var request CcrCommonRequest
	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if request.Name == "" {
		http.Error(w, "name is empty", http.StatusBadRequest)
		return
	}

	if isRedirected, err := s.isRedirected(request.Name, w); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	} else if isRedirected {
		return
	}

	err = s.jobManager.Pause(request.Name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	type result struct {
		Success bool `json:"success"`
	}
	pauseResult := result{Success: true}
	writeJson(w, pauseResult)
}

// Resume service
func (s *HttpService) resumeHandler(w http.ResponseWriter, r *http.Request) {
	log.Infof("resume job")

	// Parse the JSON request body
	var request CcrCommonRequest
	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if request.Name == "" {
		http.Error(w, "name is empty", http.StatusBadRequest)
		return
	}

	if isRedirected, err := s.isRedirected(request.Name, w); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	} else if isRedirected {
		return
	}

	err = s.jobManager.Resume(request.Name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	type result struct {
		Success bool `json:"success"`
	}
	resumeResult := result{Success: true}
	writeJson(w, resumeResult)
}

func (s *HttpService) deleteHandler(w http.ResponseWriter, r *http.Request) {
	log.Infof("delete job")

	// Parse the JSON request body
	var request CcrCommonRequest
	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if request.Name == "" {
		http.Error(w, "name is empty", http.StatusBadRequest)
		return
	}

	if isRedirected, err := s.isRedirected(request.Name, w); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	} else if isRedirected {
		return
	}

	err = s.jobManager.RemoveJob(request.Name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	type result struct {
		Success bool `json:"success"`
	}
	deleteResult := result{Success: true}
	writeJson(w, deleteResult)
}

func (s *HttpService) statusHandler(w http.ResponseWriter, r *http.Request) {
	log.Infof("get job status")

	// Parse the JSON request body
	var request CcrCommonRequest
	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if request.Name == "" {
		http.Error(w, "name is empty", http.StatusBadRequest)
		return
	}

	if isRedirected, err := s.isRedirected(request.Name, w); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	} else if isRedirected {
		return
	}

	jobStatus, err := s.jobManager.GetJobStatus(request.Name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeJson(w, jobStatus)
}

func (s *HttpService) desyncHandler(w http.ResponseWriter, r *http.Request) {
	log.Infof("desync job")

	// Parse the JSON request body
	var request CcrCommonRequest
	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if request.Name == "" {
		http.Error(w, "name is empty", http.StatusBadRequest)
		return
	}

	if isRedirected, err := s.isRedirected(request.Name, w); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	} else if isRedirected {
		return
	}

	if err := s.jobManager.Desync(request.Name); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	type result struct {
		Success bool `json:"success"`
	}
	desyncResult := result{Success: true}
	writeJson(w, desyncResult)
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
