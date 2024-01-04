package xmetrics

import "github.com/selectdb/ccr_syncer/pkg/xerror"

type IMetricsTag interface {
	Tag() []string
}

type metricsTag struct {
	tags []string
}

// dashboard metrics
type dashboardMetrics struct {
	metricsTag
}

func DashboardMetrics() *dashboardMetrics {
	return &dashboardMetrics{
		metricsTag: metricsTag{[]string{"dashboard"}},
	}
}

func (d *dashboardMetrics) Tag() []string {
	return d.tags
}

func (d *dashboardMetrics) JobNum() IMetricsTag {
	d.tags = append(d.tags, "jobNum")
	return d
}

func (d *dashboardMetrics) BinlogNum() IMetricsTag {
	d.tags = append(d.tags, "binlogNum")
	return d
}

// job metrics
type jobMetrics struct {
	metricsTag
	name string
}

func JobMetrics(jobName string) *jobMetrics {
	return &jobMetrics{
		metricsTag: metricsTag{[]string{"job"}},
		name:       jobName,
	}
}

func (j *jobMetrics) Tag() []string {
	j.tags = append(j.tags, j.name)
	return j.tags
}

func (j *jobMetrics) PrevCommitSeq() IMetricsTag {
	j.tags = append(j.tags, "prevCommitSeq")
	return j
}

func (j *jobMetrics) HandlingCommitSeq() IMetricsTag {
	j.tags = append(j.tags, "handlingCommitSeq")
	return j
}

func (j *jobMetrics) HandledBinlogNum() IMetricsTag {
	j.tags = append(j.tags, "handledBinlogNum")
	return j
}

// error metrics
type errorMetrics struct {
	metricsTag
}

func ErrorMetrics(err *xerror.XError) IMetricsTag {
	errMetrics := &errorMetrics{
		metricsTag: metricsTag{[]string{"error", err.ErrType.String()}},
	}

	// use switch instead of ifelse maybe
	if err.IsRecoverable() {
		errMetrics.tags = append(errMetrics.tags, "recoverable")
	} else if err.IsPanic() {
		errMetrics.tags = append(errMetrics.tags, "panic")
	} else {
		errMetrics.tags = append(errMetrics.tags, "unknown")
	}

	return errMetrics
}

func (e *errorMetrics) Tag() []string {
	return e.tags
}
