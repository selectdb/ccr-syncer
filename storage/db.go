package storage

import "errors"

var (
	ErrJobExists    = errors.New("job exists")
	ErrJobNotExists = errors.New("job not exists")
)

type DB interface {
	// Add ccr job
	AddJob(jobName string, jobInfo string) error
	// Update ccr job
	UpdateJob(jobName string, jobInfo string) error
	// Check Job exist
	IsJobExist(jobName string) (bool, error)
	// Get all jobs
	GetAllJobs() (map[string]string, error)

	// Update ccr sync progress
	UpdateProgress(jobName string, progress string) error
	// IsProgressExist
	IsProgressExist(jobName string) (bool, error)
	// Get ccr sync progress
	GetProgress(jobName string) (string, error)
}
