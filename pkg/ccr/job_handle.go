package ccr

import (
	"reflect"

	"github.com/cloudwego/kitex/tool/internal_pkg/log"
	"github.com/selectdb/ccr_syncer/pkg/ccr/record"
	festruct "github.com/selectdb/ccr_syncer/pkg/rpc/kitex_gen/frontendservice"
	"github.com/selectdb/ccr_syncer/pkg/xerror"
)

var (
	// A map to save all the job handles
	jobHandles = make(map[festruct.TBinlogType]JobHandleMethodTable)
)

type HandleFn func(*Job, *festruct.TBinlog) error
type CommittedFn func(*Job, *festruct.TBinlog) (bool, error)
type JobHandleMethodTable struct {
	handle    HandleFn
	committed CommittedFn
}

// A handle interface for job, to handle different binlog types
type JobHandle[T record.Record] interface {
	// Handle the binlog with the record
	Handle(job *Job, commitSeq int64, record T) error

	// Whether the binlog is idempotent
	IsIdempotent() bool

	// Check if the binlog is committed in the dest cluster
	IsBinlogCommitted(job *Job, record T) (bool, error)
}

// A function to instance a new generic record
func newGenericRecord[T record.Record]() T {
	var record T
	recordType := reflect.TypeOf(record)
	if recordType.Kind() == reflect.Ptr {
		record = reflect.New(recordType.Elem()).Interface().(T)
	} else {
		record = reflect.New(recordType).Elem().Interface().(T)
	}
	return record
}

func buildGenericHandleMethod[T record.Record](handle JobHandle[T]) HandleFn {
	return func(job *Job, binlog *festruct.TBinlog) error {
		progress := job.progress

		log.Infof("handle %s, progress prev commit seq: %d, commit seq: %d",
			binlog.GetType(), progress.PrevCommitSeq, progress.CommitSeq)

		data := binlog.GetData()
		record := newGenericRecord[T]()
		if err := record.Deserialize(data); err != nil {
			return err
		}

		tableId := record.GetTableId()
		if job.isBinlogCommitted(tableId, progress.CommitSeq) {
			return nil
		}

		commitSeq := binlog.GetCommitSeq()
		return handle.Handle(job, commitSeq, record)
	}
}

func buildGenericCommittedMethod[T record.Record](handle JobHandle[T]) CommittedFn {
	return func(job *Job, binlog *festruct.TBinlog) (bool, error) {
		if handle.IsIdempotent() {
			return true, nil
		}

		data := binlog.GetData()
		record := newGenericRecord[T]()
		if err := record.Deserialize(data); err != nil {
			return false, err
		}

		return handle.IsBinlogCommitted(job, record)
	}
}

func buildGenericHandleMethodTable[T record.Record](handle JobHandle[T]) JobHandleMethodTable {
	return JobHandleMethodTable{
		handle:    buildGenericHandleMethod(handle),
		committed: buildGenericCommittedMethod(handle),
	}
}

// Register a handle for a specific binlog type, if there is already a handle for the binlog
// type, it will be replaced
func RegisterJobHandle[T record.Record](binlogType festruct.TBinlogType, handle JobHandle[T]) {
	methodTable := buildGenericHandleMethodTable(handle)
	RegisterJobHandleMethodTable(binlogType, methodTable)
}

// Register a handle method table for a specific binlog type, if there is already a handle for
// the binlog type, it will be replaced
func RegisterJobHandleMethodTable(binlogType festruct.TBinlogType, methodTable JobHandleMethodTable) {
	jobHandles[binlogType] = methodTable
}

// Handle the binlog with the job
func HandleBinlog(job *Job, binlog *festruct.TBinlog) error {
	binlogType := binlog.GetType()
	table, ok := jobHandles[binlogType]
	if !ok {
		return xerror.Errorf(xerror.Normal, "no handle for binlog type %s", binlogType)
	}
	return table.handle(job, binlog)
}

// Check if the binlog is committed in the dest cluster
func IsBinlogCommitted(job *Job, binlog *festruct.TBinlog) (bool, error) {
	binlogType := binlog.GetType()
	handle, ok := jobHandles[binlogType]
	if !ok {
		return false, xerror.Errorf(xerror.Normal, "no handle for binlog type %s", binlogType)
	}
	return handle.committed(job, binlog)
}

// Check if the binlog type is registered
func IsJobHandleRegistered(binlogType festruct.TBinlogType) bool {
	_, ok := jobHandles[binlogType]
	return ok
}
