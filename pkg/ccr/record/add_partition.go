package record

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/selectdb/ccr_syncer/pkg/utils"
	"github.com/selectdb/ccr_syncer/pkg/xerror"

	log "github.com/sirupsen/logrus"
)

type DistributionInfo struct {
	BucketNum           int    `json:"bucketNum"`
	Type                string `json:"type"`
	DistributionColumns []struct {
		Name string `json:"name"`
	} `json:"distributionColumns"`
}

type AddPartition struct {
	DbId      int64  `json:"dbId"`
	TableId   int64  `json:"tableId"`
	Sql       string `json:"sql"`
	IsTemp    bool   `json:"isTempPartition"`
	Partition struct {
		DistributionInfoOld *DistributionInfo `json:"distributionInfo"`
		DistributionInfoNew *DistributionInfo `json:"di"`
	} `json:"partition"`
}

func NewAddPartitionFromJson(data string) (*AddPartition, error) {
	var addPartition AddPartition
	err := json.Unmarshal([]byte(data), &addPartition)
	if err != nil {
		return nil, xerror.Wrap(err, xerror.Normal, "unmarshal add partition error")
	}

	if addPartition.Sql == "" {
		return nil, xerror.Errorf(xerror.Normal, "add partition sql is empty")
	}

	if addPartition.TableId == 0 {
		return nil, xerror.Errorf(xerror.Normal, "table id not found")
	}

	return &addPartition, nil
}

func (addPartition *AddPartition) getDistributionInfo() *DistributionInfo {
	if addPartition.Partition.DistributionInfoOld != nil {
		return addPartition.Partition.DistributionInfoOld
	}
	return addPartition.Partition.DistributionInfoNew
}

func (addPartition *AddPartition) getDistributionColumns() []string {
	var distributionColumns []string
	for _, column := range addPartition.getDistributionInfo().DistributionColumns {
		distributionColumns = append(distributionColumns, column.Name)
	}
	return distributionColumns
}

func (addPartition *AddPartition) GetSql(destTableName string) string {
	// addPartitionSql = "ALTER TABLE " + sql
	addPartitionSql := fmt.Sprintf("ALTER TABLE %s %s", utils.FormatKeywordName(destTableName), addPartition.Sql)
	// remove last ';' and add BUCKETS num
	addPartitionSql = strings.TrimRight(addPartitionSql, ";")
	// check contains BUCKETS num, ignore case
	if strings.Contains(strings.ToUpper(addPartitionSql), "BUCKETS") {
		// if not contains BUCKETS AUTO, return directly
		if !strings.Contains(strings.ToUpper(addPartitionSql), "BUCKETS AUTO") {
			log.Infof("addPartitionSql contains BUCKETS declaration, sql: %s", addPartitionSql)
			return addPartitionSql
		}

		log.Info("addPartitionSql contains BUCKETS AUTO, remove it")
		// BUCKETS AUTO is in the end of sql, remove it, so we not care about the string after BUCKETS AUTO
		// Remove BUCKETS AUTO case, but not change other sql case
		// find BUCKETS AUTO index, remove it from origin sql
		bucketsAutoIndex := strings.LastIndex(strings.ToUpper(addPartitionSql), "BUCKETS AUTO")
		addPartitionSql = addPartitionSql[:bucketsAutoIndex]
	}

	// check contain DISTRIBUTED BY
	// if not contain
	// create like below sql
	// ALTER TABLE my_table
	// ADD PARTITION p1 VALUES LESS THAN ("2015-01-01")
	// DISTRIBUTED BY HASH(k1) BUCKETS 20;
	// or DISTRIBUTED BY RANDOM  BUCKETS 20;
	distributionInfo := addPartition.getDistributionInfo()
	if !strings.Contains(strings.ToUpper(addPartitionSql), "DISTRIBUTED BY") {
		if distributionInfo.Type == "HASH" {
			addPartitionSql = fmt.Sprintf("%s DISTRIBUTED BY HASH(%s)", addPartitionSql,
				"`"+strings.Join(addPartition.getDistributionColumns(), "`,`")+"`")
		} else {
			addPartitionSql = fmt.Sprintf("%s DISTRIBUTED BY RANDOM", addPartitionSql)
		}
	}
	bucketNum := distributionInfo.BucketNum
	addPartitionSql = fmt.Sprintf("%s BUCKETS %d", addPartitionSql, bucketNum)

	return addPartitionSql
}
