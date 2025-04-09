package handle

import (
	"github.com/selectdb/ccr_syncer/pkg/ccr"
	"github.com/selectdb/ccr_syncer/pkg/ccr/record"
	festruct "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/frontendservice"
)

func init() {
	ccr.RegisterJobHandle[*record.ModifyDistributionBucketNum](festruct.TBinlogType_MODIFY_DISTRIBUTION_BUCKET_NUM, &ModifyDistributionBucketNumHandle{})
}

type ModifyDistributionBucketNumHandle struct {
	// The modify distribution bucket num binlog is idempotent
	IdempotentJobHandle[*record.ModifyDistributionBucketNum]
}

func (h *ModifyDistributionBucketNumHandle) Handle(j *ccr.Job, commitSeq int64, modifyDistributionBucketNum *record.ModifyDistributionBucketNum) error {
	var destTableName string
	if j.SyncType == ccr.TableSync {
		destTableName = j.Dest.Table
	} else {
		table, err := j.GetDestMeta().GetTable(modifyDistributionBucketNum.TableId)
		if err != nil {
			return err
		}
		destTableName = table.Name
	}
	bucketType := modifyDistributionBucketNum.Type
	autoBucket := modifyDistributionBucketNum.AutoBucket
	bucketNum := modifyDistributionBucketNum.BucketNum
	columnsName := modifyDistributionBucketNum.ColumnsName
	return j.IDest.ModifyDistributionBucketNum(destTableName, bucketType, autoBucket, bucketNum, columnsName)
}
