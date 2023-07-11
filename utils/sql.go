package utils

import "database/sql"

func MakeSingleColScanArgs[T interface{}](prefix int, col *T, suffix int) []interface{} {
	prefixCols := make([]sql.RawBytes, prefix)
	suffixCols := make([]sql.RawBytes, suffix)
	scanArgs := make([]interface{}, 0, prefix+suffix+1)
	for i := range prefixCols {
		scanArgs = append(scanArgs, &prefixCols[i])
	}
	scanArgs = append(scanArgs, col)
	for i := range suffixCols {
		scanArgs = append(scanArgs, &suffixCols[i])
	}

	return scanArgs
}
