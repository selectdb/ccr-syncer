package utils

import (
	"database/sql"
	"strconv"

	"github.com/pkg/errors"
)

type RowParser struct {
	columns map[string]*sql.RawBytes
}

func NewRowParser() *RowParser {
	return &RowParser{
		columns: make(map[string]*sql.RawBytes),
	}
}

func (r *RowParser) Parse(row *sql.Rows) error {
	cols, err := row.Columns()
	if err != nil {
		return err
	}

	rowData := make([]sql.RawBytes, len(cols))
	rowPointer := make([]interface{}, len(cols))
	for i := range rowPointer {
		rowPointer[i] = &rowData[i]
	}

	if err := row.Scan(rowPointer...); err != nil {
		return err
	}

	for i, colName := range cols {
		r.columns[colName] = rowPointer[i].(*sql.RawBytes)
	}

	return nil
}

func (r *RowParser) GetBytesPointer(columnName string) (*sql.RawBytes, error) {
	resBytes, ok := r.columns[columnName]
	if !ok {
		return nil, errors.Errorf("column %s is not in this table", columnName)
	}
	return resBytes, nil
}

func (r *RowParser) GetInt64(columnName string) (int64, error) {
	resBytes, ok := r.columns[columnName]
	if !ok {
		return 0, errors.Errorf("column %s is not in this table", columnName)
	}
	resInt64, err := strconv.ParseInt(string(*resBytes), 10, 64)
	if err != nil {
		return 0, err
	}

	return resInt64, nil
}

func (r *RowParser) GetBool(columnName string) (bool, error) {
	resBytes, ok := r.columns[columnName]
	if !ok {
		return false, errors.Errorf("column %s is not in this table", columnName)
	}
	resBool, err := strconv.ParseBool(string(*resBytes))
	if err != nil {
		return false, err
	}
	return resBool, nil
}

func (r *RowParser) GetString(columnName string) (string, error) {
	resBytes, ok := r.columns[columnName]
	if !ok {
		return "", errors.Errorf("column %s is not in this table", columnName)
	}

	return string(*resBytes), nil
}
