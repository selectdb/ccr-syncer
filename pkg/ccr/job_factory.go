package ccr

import "context"

type JobFactory struct{}

// create job factory
func NewJobFactory() *JobFactory {
	return &JobFactory{}
}

func (jf *JobFactory) CreateJob(ctx context.Context, job *Job, jobType string) (Jober, error) {
	switch jobType {
	case "IngestBinlog":
		return NewIngestBinlogJob(ctx, job)
	case "Snapshot":
		return nil, nil
		// return NewSnapshotJob(ctx, job)
	default:
		return nil, nil
	}
}
