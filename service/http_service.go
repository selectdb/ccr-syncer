package service

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/selectdb/ccr_syncer/ccr"
	"github.com/selectdb/ccr_syncer/ccr/base"
	"github.com/selectdb/ccr_syncer/storage"

	log "github.com/sirupsen/logrus"
)

type HttpService struct {
	port int
	mux  *http.ServeMux

	db         storage.DB
	jobManager *ccr.JobManager
}

func NewHttpServer(port int, db storage.DB, jobManager *ccr.JobManager) *HttpService {
	return &HttpService{
		port: port,
		mux:  http.NewServeMux(),

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
	job, err := ccr.NewJobFromService(request.Name, request.Src, request.Dest, s.db)
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

// HttpServer serving /create_ccr by json http rpc
func (s *HttpService) createCcrHandler(w http.ResponseWriter, r *http.Request) {
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
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Write a success response
	w.WriteHeader(http.StatusOK)
}

func (s *HttpService) RegisterHandlers() {
	s.mux.HandleFunc("/create_ccr", s.createCcrHandler)
}

func (s *HttpService) Start() error {
	addr := fmt.Sprintf(":%d", s.port)
	fmt.Printf("Server listening on %s\n", addr)
	s.RegisterHandlers()
	return http.ListenAndServe(addr, s.mux)
}
