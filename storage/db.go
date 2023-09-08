package storage

import "errors"

var (
	ErrJobExists    = errors.New("job exists")
	ErrJobNotExists = errors.New("job not exists")
)

const (
	InvalidCheckTimestamp int64 = -1
	remoteDBName string = "ccr"
)

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
