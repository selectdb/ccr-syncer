package handle

import (
	"github.com/selectdb/ccr_syncer/pkg/ccr"
	"github.com/selectdb/ccr_syncer/pkg/ccr/record"
	festruct "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/frontendservice"
	log "github.com/sirupsen/logrus"
)

func init() {
	ccr.RegisterJobHandle[*record.ModifyTableProperty](festruct.TBinlogType_MODIFY_TABLE_PROPERTY, &ModifyTablePropertyHandle{})
}

type ModifyTablePropertyHandle struct {
	// The modify table property binlog is idempotent
	IdempotentJobHandle[*record.ModifyTableProperty]
}

func (h *ModifyTablePropertyHandle) Handle(j *ccr.Job, commitSeq int64, modifyProperty *record.ModifyTableProperty) error {
	if ccr.FeatureOverrideReplicationNum() && j.ReplicationNum > 0 {
		if _, exists := modifyProperty.Properties["default.replication_allocation"]; exists {
			delete(modifyProperty.Properties, "default.replication_allocation")
			log.Debugf("delete default.replication_allocation from modify table property")
		}
	}

	destTableName, err := j.GetDestNameBySrcId(modifyProperty.TableId)
	if err != nil {
		return err
	}
	return j.Dest.ModifyTableProperty(destTableName, modifyProperty)
}
