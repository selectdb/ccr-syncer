package ccr

import (
	"encoding/json"
	"sync"

	"github.com/selectdb/ccr_syncer/storage"
	"github.com/selectdb/ccr_syncer/xerror"

	log "github.com/sirupsen/logrus"
)

const (
	ErrJobExist = "job exist"
)

// job manager is thread safety
type JobManager struct {
	db       storage.DB
	jobs     map[string]*Job
	lock     sync.RWMutex
	factory  *Factory
	hostInfo string
	stop     chan struct{}
	wg       sync.WaitGroup
}

func NewJobManager(db storage.DB, factory *Factory, hostInfo string) *JobManager {
	return &JobManager{
		db:       db,
		jobs:     make(map[string]*Job),
		factory:  factory,
		hostInfo: hostInfo,
		stop:     make(chan struct{}),
	}
}

func (jm *JobManager) GetFactory() *Factory {
	return jm.factory
}

// add job to job manager && run job
func (jm *JobManager) AddJob(job *Job) error {
	log.Infof("add job: %s", job.Name)

	jm.lock.Lock()
	defer jm.lock.Unlock()

	// Step 1: check job exist
	if _, ok := jm.jobs[job.Name]; ok {
		return xerror.Errorf(xerror.Normal, "%s: %s", ErrJobExist, job.Name)
	}

	// Step 2: check job first run, mostly for dest/src fe db/table info
	if err := job.FirstRun(); err != nil {
		return err
	}

	// Step 3: add job info to db
	data, err := json.Marshal(job)
	if err != nil {
		return xerror.Wrap(err, xerror.Normal, "marshal job error")
	}
	if err := jm.db.AddJob(job.Name, string(data), jm.hostInfo); err != nil {
		return err
	}

	// Step 4: run job
	jm.jobs[job.Name] = job
	jm.runJob(job)

	return nil
}

func (jm *JobManager) Recover(jobNames []string) error {
	log.Info("job manager recover")

	jm.lock.Lock()
	defer jm.lock.Unlock()

	jobs := make([]*Job, 0)
	for _, jobName := range jobNames {
		if _, ok := jm.jobs[jobName]; ok {
			continue
		}
		log.Infof("recover job: %s", jobName)

		if jobInfo, err := jm.db.GetJobInfo(jobName); err != nil {
			return err
		} else if job, err := NewJobFromJson(jobInfo, jm.db, jm.factory); err != nil {
			return err
		} else {
			jobs = append(jobs, job)
		}
	}

	for _, job := range jobs {
		jm.jobs[job.Name] = job
		jm.runJob(job)
	}
	return nil
}

// remove job
func (jm *JobManager) RemoveJob(name string) error {
	log.Infof("remove job: %s", name)

	jm.lock.Lock()
	defer jm.lock.Unlock()

	// check job exist
	if job, ok := jm.jobs[name]; ok {
		// stop job
		job.Stop()
		delete(jm.jobs, name)
		return jm.db.RemoveJob(name)
	} else {
		return xerror.Errorf(xerror.Normal, "job not exist: %s", name)
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
		err := job.Run()
		if err != nil {
			log.Errorf("job run failed, job name: %s, error: %+v", job.Name, err)
		}
		jm.wg.Done()
	}()
}

func (jm *JobManager) GetLag(jobName string) (int64, error) {
	jm.lock.RLock()
	defer jm.lock.RUnlock()

	if job, ok := jm.jobs[jobName]; ok {
		return job.GetLag()
	} else {
		return 0, xerror.Errorf(xerror.Normal, "job not exist: %s", jobName)
	}
}

func (jm *JobManager) dealJob(jobName string, dealFunc func(job *Job) error) error {
	jm.lock.RLock()
	defer jm.lock.RUnlock()

	if job, ok := jm.jobs[jobName]; ok {
		return dealFunc(job)
	} else {
		return xerror.Errorf(xerror.Normal, "job not exist: %s", jobName)
	}
}

func (jm *JobManager) Pause(jobName string) error {
	return jm.dealJob(jobName, func(job *Job) error {
		return job.Pause()
	})
}

func (jm *JobManager) Resume(jobName string) error {
	return jm.dealJob(jobName, func(job *Job) error {
		return job.Resume()
	})
}

func (jm *JobManager) JobStatus(jobName string) (*JobStatus, error) {
	jm.lock.RLock()
	defer jm.lock.RUnlock()

	if job, ok := jm.jobs[jobName]; ok {
		return job.Status(), nil
	} else {
		return nil, xerror.Errorf(xerror.Normal, "job not exist: %s", jobName)
	}
}

func (jm *JobManager) Desync(jobName string) error {
	jm.lock.RLock()
	defer jm.lock.RUnlock()

	if job, ok := jm.jobs[jobName]; ok {
		return job.Desync()
	} else {
		return xerror.Errorf(xerror.Normal, "job not exist: %s", jobName)
	}
}
