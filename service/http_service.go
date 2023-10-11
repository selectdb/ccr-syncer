package service

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/selectdb/ccr_syncer/ccr"
	"github.com/selectdb/ccr_syncer/ccr/base"
	"github.com/selectdb/ccr_syncer/storage"
	"github.com/selectdb/ccr_syncer/xerror"

	log "github.com/sirupsen/logrus"
)

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

	// Write a success response
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("create ccr success"))
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
	w.Write([]byte(fmt.Sprintf("lag: %d", lag)))
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
	w.Write([]byte("pause success"))
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
	w.Write([]byte("resume success"))
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
	w.Write([]byte("delete success"))
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

	jobStatus, err := s.jobManager.JobStatus(request.Name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// write jobStatus as json
	if data, err := json.Marshal(jobStatus); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	} else {
		w.Write(data)
	}
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
	w.Write([]byte("desync success"))
}

func (s *HttpService) RegisterHandlers() {
	s.mux.HandleFunc("/create_ccr", s.createHandler)
	s.mux.HandleFunc("/get_lag", s.getLagHandler)
	s.mux.HandleFunc("/pause", s.pauseHandler)
	s.mux.HandleFunc("/resume", s.resumeHandler)
	s.mux.HandleFunc("/delete", s.deleteHandler)
	s.mux.HandleFunc("/job_status", s.statusHandler)
	s.mux.HandleFunc("/desync", s.desyncHandler)
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

func (s *HttpService) Stop() error {
	if err := s.server.Shutdown(context.TODO()); err != nil {
		return xerror.Wrapf(err, xerror.Normal, "http server close failed")
	}
	return nil
}
