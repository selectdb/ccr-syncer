// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License
package record

const (
	INDEX_TYPE_BITMAP      = "BITMAP"
	INDEX_TYPE_INVERTED    = "INVERTED"
	INDEX_TYPE_BLOOMFILTER = "BLOOMFILTER"
	INDEX_TYPE_NGRAM_BF    = "NGRAM_BF"
)

type Index struct {
	IndexId         int64             `json:"indexId"`
	IndexName       string            `json:"indexName"`
	Columns         []string          `json:"columns"`
	IndexType       string            `json:"indexType"`
	Properties      map[string]string `json:"properties"`
	Comment         string            `json:"comment"`
	ColumnUniqueIds []int             `json:"columnUniqueIds"`

	IndexIdAlternative         int64             `json:"i"`
	IndexNameAlternative       string            `json:"in"`
	ColumnsAlternative         []string          `json:"c"`
	IndexTypeAlternative       string            `json:"it"`
	PropertiesAlternative      map[string]string `json:"pt"`
	CommentAlternative         string            `json:"ct"`
	ColumnUniqueIdsAlternative []int             `json:"cui"`
}

func (index *Index) GetIndexName() string {
	if index.IndexName != "" {
		return index.IndexName
	}
	return index.IndexNameAlternative
}

func (index *Index) GetColumns() []string {
	if len(index.Columns) > 0 {
		return index.Columns
	}
	return index.ColumnsAlternative
}

func (index *Index) GetComment() string {
	if index.Comment != "" {
		return index.Comment
	}
	return index.CommentAlternative
}

func (index *Index) GetIndexType() string {
	if index.IndexType != "" {
		return index.IndexType
	}
	return index.IndexTypeAlternative
}

func (index *Index) IsInvertedIndex() bool {
	return index.GetIndexType() == INDEX_TYPE_INVERTED
}
