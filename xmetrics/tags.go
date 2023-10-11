package xmetrics

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

func ErrorMetrics() *errorMetrics {
	return &errorMetrics{
		metricsTag: metricsTag{[]string{"error"}},
	}
}

func (e *errorMetrics) Tag() []string {
	return e.tags
}

func (e *errorMetrics) Type(typeName string) IMetricsTag {
	e.tags = append(e.tags, typeName)
	return e
}
