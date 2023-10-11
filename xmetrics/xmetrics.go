package xmetrics

import (
	"github.com/hashicorp/go-metrics"
	"github.com/hashicorp/go-metrics/prometheus"
	"github.com/selectdb/ccr_syncer/xerror"
)

func InitGlobal(serviceName string) error {
	sink, err := prometheus.NewPrometheusSink()
	if err != nil {
		return xerror.Wrap(err, xerror.Normal, "init prometheus sink falied")
	}

	if _, err := metrics.NewGlobal(metrics.DefaultConfig(serviceName), sink); err != nil {
		return xerror.Wrap(err, xerror.Normal, "new global metrics falied")
	}

	return nil
}

func AddErrorWithType(errorType xerror.ErrType) {
	metrics.IncrCounter(ErrorMetrics().Type(errorType.String()).Tag(), 1)
}

func AddNewJob(jobName string) {
	metrics.SetGauge(JobMetrics(jobName).HandlingCommitSeq().Tag(), -1)

	metrics.IncrCounter(DashboardMetrics().JobNum().Tag(), 1)
}

func HandlingBinlog(jobName string, commitSeq int64) {
	metrics.SetGauge(JobMetrics(jobName).HandlingCommitSeq().Tag(), float32(commitSeq))
}

func Rollback(jobName string, commitSeq int64) {
	metrics.SetGauge(JobMetrics(jobName).HandlingCommitSeq().Tag(), float32(commitSeq))
	metrics.SetGauge(JobMetrics(jobName).PrevCommitSeq().Tag(), float32(commitSeq))
}

func ConsumeBinlog(jobName string, commitSeq int64) {
	metrics.SetGauge(JobMetrics(jobName).PrevCommitSeq().Tag(), float32(commitSeq))
	metrics.IncrCounter(JobMetrics(jobName).HandledBinlogNum().Tag(), 1)

	metrics.IncrCounter(DashboardMetrics().BinlogNum().Tag(), 1)
}
