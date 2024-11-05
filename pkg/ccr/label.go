package ccr

import (
	"fmt"
	"time"
)

// snapshot name format "ccrs_${ccr_name}_${sync_id}"
func NewSnapshotLabelPrefix(ccrName string, syncId int64) string {
	return fmt.Sprintf("ccrs_%s_%d", ccrName, syncId)
}

// snapshot name format "ccrp_${ccr_name}_${sync_id}"
func NewPartialSnapshotLabelPrefix(ccrName string, syncId int64) string {
	return fmt.Sprintf("ccrp_%s_%d", ccrName, syncId)
}

func NewLabelWithTs(prefix string) string {
	return fmt.Sprintf("%s_%d", prefix, time.Now().Unix())
}

func NewRestoreLabel(snapshotName string) string {
	if snapshotName == "" {
		return ""
	}

	// use current seconds
	return fmt.Sprintf("%s_r_%d", snapshotName, time.Now().Unix())
}

func TableAlias(tableName string) string {
	return fmt.Sprintf("__ccr_%s_%d", tableName, time.Now().Unix())
}
