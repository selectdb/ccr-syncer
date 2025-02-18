package record

// A basic interface for all binlog records
type Record interface {
	// Deserialize the binlog data to the record
	Deserialize(data string) error

	// Get the table id of this record
	GetTableId() int64
}
