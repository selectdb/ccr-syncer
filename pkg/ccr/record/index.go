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
