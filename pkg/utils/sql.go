// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License
package utils

import (
	"database/sql"
	"strconv"
	"strings"

	"github.com/selectdb/ccr_syncer/pkg/xerror"
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
		return nil, xerror.Errorf(xerror.Normal, "column %s is not in this table", columnName)
	}
	return resBytes, nil
}

func (r *RowParser) GetInt64(columnName string) (int64, error) {
	resBytes, ok := r.columns[columnName]
	if !ok {
		return 0, xerror.Errorf(xerror.Normal, "column %s is not in this table", columnName)
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
		return false, xerror.Errorf(xerror.Normal, "column %s is not in this table", columnName)
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
		return "", xerror.Errorf(xerror.Normal, "column %s is not in this table", columnName)
	}

	return string(*resBytes), nil
}

func FormatKeywordName(name string) string {
	return "`" + strings.TrimSpace(name) + "`"
}

func EscapeStringValue(value string) string {
	escaped := strings.ReplaceAll(value, "\\", "\\\\")
	escaped = strings.ReplaceAll(escaped, "\"", "\\\"")
	escaped = strings.ReplaceAll(escaped, "'", "\\'")
	return escaped
}
