package record

type Record interface {
	Deserialize(data string) error

	GetTableId() int64
}
