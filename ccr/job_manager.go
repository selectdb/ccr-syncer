package ccr

import (
	"fmt"
	"sync"

	"github.com/selectdb/ccr_syncer/storage"
)

const (
	ErrJobExist = "job exist"
)

// job manager is thread safety
type JobManager struct {
	db   storage.DB
	jobs map[string]*Job
	lock sync.RWMutex
	stop chan struct{}
	wg   sync.WaitGroup
}

func NewJobManager(db storage.DB) *JobManager {
	return &JobManager{
		db:   db,
		jobs: make(map[string]*Job),
		stop: make(chan struct{}),
	}
}

// add job to job manager && run job
func (jm *JobManager) AddJob(job *Job) error {
	jm.lock.Lock()
	defer jm.lock.Unlock()

	// check job exist
	if _, ok := jm.jobs[job.Name]; ok {
		return fmt.Errorf("%s: %s", ErrJobExist, job.Name)
	}

	if err := job.FirstRunCheck(); err != nil {
		return err
	}

	jm.jobs[job.Name] = job
	jm.runJob(job)

	return nil
}

// remove job
func (jm *JobManager) RemoveJob(name string) error {
	jm.lock.Lock()
	defer jm.lock.Unlock()

	// check job exist
	if job, ok := jm.jobs[name]; ok {
		// stop job
		job.Stop()
		delete(jm.jobs, name)
		return nil
	} else {
		return fmt.Errorf("job not exist: %s", name)
	}
}

// go run all jobs and wait for stop chan
func (jm *JobManager) Start() error {
	jm.lock.RLock()
	for _, job := range jm.jobs {
		jm.runJob(job)
	}
	jm.lock.RUnlock()

	<-jm.stop
	return nil
}

// stop job manager
// first stop all jobs, then stop job manager
func (jm *JobManager) Stop() error {
	jm.lock.RLock()

	// stop all jobs
	for _, job := range jm.jobs {
		job.Stop()
	}
	jm.lock.RUnlock()

	// stop job manager
	close(jm.stop)
	jm.wg.Wait()
	return nil
}

// run job loop in job manager
func (jm *JobManager) runJob(job *Job) {
	jm.wg.Add(1)

	go func() {
		job.Run()
		jm.wg.Done()
	}()
}
