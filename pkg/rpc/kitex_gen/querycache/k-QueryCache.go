// Code generated by Kitex v0.8.0. DO NOT EDIT.

package querycache

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/cloudwego/kitex/pkg/protocol/bthrift"
)

// unused protection
var (
	_ = fmt.Formatter(nil)
	_ = (*bytes.Buffer)(nil)
	_ = (*strings.Builder)(nil)
	_ = reflect.Type(nil)
	_ = thrift.TProtocol(nil)
	_ = bthrift.BinaryWriter(nil)
)

func (p *TQueryCacheParam) FastRead(buf []byte) (int, error) {
	var err error
	var offset int
	var l int
	var fieldTypeId thrift.TType
	var fieldId int16
	_, l, err = bthrift.Binary.ReadStructBegin(buf)
	offset += l
	if err != nil {
		goto ReadStructBeginError
	}

	for {
		_, fieldTypeId, fieldId, l, err = bthrift.Binary.ReadFieldBegin(buf[offset:])
		offset += l
		if err != nil {
			goto ReadFieldBeginError
		}
		if fieldTypeId == thrift.STOP {
			break
		}
		switch fieldId {
		case 1:
			if fieldTypeId == thrift.I32 {
				l, err = p.FastReadField1(buf[offset:])
				offset += l
				if err != nil {
					goto ReadFieldError
				}
			} else {
				l, err = bthrift.Binary.Skip(buf[offset:], fieldTypeId)
				offset += l
				if err != nil {
					goto SkipFieldError
				}
			}
		case 2:
			if fieldTypeId == thrift.STRING {
				l, err = p.FastReadField2(buf[offset:])
				offset += l
				if err != nil {
					goto ReadFieldError
				}
			} else {
				l, err = bthrift.Binary.Skip(buf[offset:], fieldTypeId)
				offset += l
				if err != nil {
					goto SkipFieldError
				}
			}
		case 3:
			if fieldTypeId == thrift.MAP {
				l, err = p.FastReadField3(buf[offset:])
				offset += l
				if err != nil {
					goto ReadFieldError
				}
			} else {
				l, err = bthrift.Binary.Skip(buf[offset:], fieldTypeId)
				offset += l
				if err != nil {
					goto SkipFieldError
				}
			}
		case 4:
			if fieldTypeId == thrift.MAP {
				l, err = p.FastReadField4(buf[offset:])
				offset += l
				if err != nil {
					goto ReadFieldError
				}
			} else {
				l, err = bthrift.Binary.Skip(buf[offset:], fieldTypeId)
				offset += l
				if err != nil {
					goto SkipFieldError
				}
			}
		case 5:
			if fieldTypeId == thrift.BOOL {
				l, err = p.FastReadField5(buf[offset:])
				offset += l
				if err != nil {
					goto ReadFieldError
				}
			} else {
				l, err = bthrift.Binary.Skip(buf[offset:], fieldTypeId)
				offset += l
				if err != nil {
					goto SkipFieldError
				}
			}
		case 6:
			if fieldTypeId == thrift.I64 {
				l, err = p.FastReadField6(buf[offset:])
				offset += l
				if err != nil {
					goto ReadFieldError
				}
			} else {
				l, err = bthrift.Binary.Skip(buf[offset:], fieldTypeId)
				offset += l
				if err != nil {
					goto SkipFieldError
				}
			}
		case 7:
			if fieldTypeId == thrift.I64 {
				l, err = p.FastReadField7(buf[offset:])
				offset += l
				if err != nil {
					goto ReadFieldError
				}
			} else {
				l, err = bthrift.Binary.Skip(buf[offset:], fieldTypeId)
				offset += l
				if err != nil {
					goto SkipFieldError
				}
			}
		default:
			l, err = bthrift.Binary.Skip(buf[offset:], fieldTypeId)
			offset += l
			if err != nil {
				goto SkipFieldError
			}
		}

		l, err = bthrift.Binary.ReadFieldEnd(buf[offset:])
		offset += l
		if err != nil {
			goto ReadFieldEndError
		}
	}
	l, err = bthrift.Binary.ReadStructEnd(buf[offset:])
	offset += l
	if err != nil {
		goto ReadStructEndError
	}

	return offset, nil
ReadStructBeginError:
	return offset, thrift.PrependError(fmt.Sprintf("%T read struct begin error: ", p), err)
ReadFieldBeginError:
	return offset, thrift.PrependError(fmt.Sprintf("%T read field %d begin error: ", p, fieldId), err)
ReadFieldError:
	return offset, thrift.PrependError(fmt.Sprintf("%T read field %d '%s' error: ", p, fieldId, fieldIDToName_TQueryCacheParam[fieldId]), err)
SkipFieldError:
	return offset, thrift.PrependError(fmt.Sprintf("%T field %d skip type %d error: ", p, fieldId, fieldTypeId), err)
ReadFieldEndError:
	return offset, thrift.PrependError(fmt.Sprintf("%T read field end error", p), err)
ReadStructEndError:
	return offset, thrift.PrependError(fmt.Sprintf("%T read struct end error: ", p), err)
}

func (p *TQueryCacheParam) FastReadField1(buf []byte) (int, error) {
	offset := 0

	if v, l, err := bthrift.Binary.ReadI32(buf[offset:]); err != nil {
		return offset, err
	} else {
		offset += l
		p.NodeId = &v

	}
	return offset, nil
}

func (p *TQueryCacheParam) FastReadField2(buf []byte) (int, error) {
	offset := 0

	if v, l, err := bthrift.Binary.ReadBinary(buf[offset:]); err != nil {
		return offset, err
	} else {
		offset += l

		p.Digest = []byte(v)

	}
	return offset, nil
}

func (p *TQueryCacheParam) FastReadField3(buf []byte) (int, error) {
	offset := 0

	_, _, size, l, err := bthrift.Binary.ReadMapBegin(buf[offset:])
	offset += l
	if err != nil {
		return offset, err
	}
	p.OutputSlotMapping = make(map[int32]int32, size)
	for i := 0; i < size; i++ {
		var _key int32
		if v, l, err := bthrift.Binary.ReadI32(buf[offset:]); err != nil {
			return offset, err
		} else {
			offset += l

			_key = v

		}

		var _val int32
		if v, l, err := bthrift.Binary.ReadI32(buf[offset:]); err != nil {
			return offset, err
		} else {
			offset += l

			_val = v

		}

		p.OutputSlotMapping[_key] = _val
	}
	if l, err := bthrift.Binary.ReadMapEnd(buf[offset:]); err != nil {
		return offset, err
	} else {
		offset += l
	}
	return offset, nil
}

func (p *TQueryCacheParam) FastReadField4(buf []byte) (int, error) {
	offset := 0

	_, _, size, l, err := bthrift.Binary.ReadMapBegin(buf[offset:])
	offset += l
	if err != nil {
		return offset, err
	}
	p.TabletToRange = make(map[int64]string, size)
	for i := 0; i < size; i++ {
		var _key int64
		if v, l, err := bthrift.Binary.ReadI64(buf[offset:]); err != nil {
			return offset, err
		} else {
			offset += l

			_key = v

		}

		var _val string
		if v, l, err := bthrift.Binary.ReadString(buf[offset:]); err != nil {
			return offset, err
		} else {
			offset += l

			_val = v

		}

		p.TabletToRange[_key] = _val
	}
	if l, err := bthrift.Binary.ReadMapEnd(buf[offset:]); err != nil {
		return offset, err
	} else {
		offset += l
	}
	return offset, nil
}

func (p *TQueryCacheParam) FastReadField5(buf []byte) (int, error) {
	offset := 0

	if v, l, err := bthrift.Binary.ReadBool(buf[offset:]); err != nil {
		return offset, err
	} else {
		offset += l
		p.ForceRefreshQueryCache = &v

	}
	return offset, nil
}

func (p *TQueryCacheParam) FastReadField6(buf []byte) (int, error) {
	offset := 0

	if v, l, err := bthrift.Binary.ReadI64(buf[offset:]); err != nil {
		return offset, err
	} else {
		offset += l
		p.EntryMaxBytes = &v

	}
	return offset, nil
}

func (p *TQueryCacheParam) FastReadField7(buf []byte) (int, error) {
	offset := 0

	if v, l, err := bthrift.Binary.ReadI64(buf[offset:]); err != nil {
		return offset, err
	} else {
		offset += l
		p.EntryMaxRows = &v

	}
	return offset, nil
}

// for compatibility
func (p *TQueryCacheParam) FastWrite(buf []byte) int {
	return 0
}

func (p *TQueryCacheParam) FastWriteNocopy(buf []byte, binaryWriter bthrift.BinaryWriter) int {
	offset := 0
	offset += bthrift.Binary.WriteStructBegin(buf[offset:], "TQueryCacheParam")
	if p != nil {
		offset += p.fastWriteField1(buf[offset:], binaryWriter)
		offset += p.fastWriteField5(buf[offset:], binaryWriter)
		offset += p.fastWriteField6(buf[offset:], binaryWriter)
		offset += p.fastWriteField7(buf[offset:], binaryWriter)
		offset += p.fastWriteField2(buf[offset:], binaryWriter)
		offset += p.fastWriteField3(buf[offset:], binaryWriter)
		offset += p.fastWriteField4(buf[offset:], binaryWriter)
	}
	offset += bthrift.Binary.WriteFieldStop(buf[offset:])
	offset += bthrift.Binary.WriteStructEnd(buf[offset:])
	return offset
}

func (p *TQueryCacheParam) BLength() int {
	l := 0
	l += bthrift.Binary.StructBeginLength("TQueryCacheParam")
	if p != nil {
		l += p.field1Length()
		l += p.field2Length()
		l += p.field3Length()
		l += p.field4Length()
		l += p.field5Length()
		l += p.field6Length()
		l += p.field7Length()
	}
	l += bthrift.Binary.FieldStopLength()
	l += bthrift.Binary.StructEndLength()
	return l
}

func (p *TQueryCacheParam) fastWriteField1(buf []byte, binaryWriter bthrift.BinaryWriter) int {
	offset := 0
	if p.IsSetNodeId() {
		offset += bthrift.Binary.WriteFieldBegin(buf[offset:], "node_id", thrift.I32, 1)
		offset += bthrift.Binary.WriteI32(buf[offset:], *p.NodeId)

		offset += bthrift.Binary.WriteFieldEnd(buf[offset:])
	}
	return offset
}

func (p *TQueryCacheParam) fastWriteField2(buf []byte, binaryWriter bthrift.BinaryWriter) int {
	offset := 0
	if p.IsSetDigest() {
		offset += bthrift.Binary.WriteFieldBegin(buf[offset:], "digest", thrift.STRING, 2)
		offset += bthrift.Binary.WriteBinaryNocopy(buf[offset:], binaryWriter, []byte(p.Digest))

		offset += bthrift.Binary.WriteFieldEnd(buf[offset:])
	}
	return offset
}

func (p *TQueryCacheParam) fastWriteField3(buf []byte, binaryWriter bthrift.BinaryWriter) int {
	offset := 0
	if p.IsSetOutputSlotMapping() {
		offset += bthrift.Binary.WriteFieldBegin(buf[offset:], "output_slot_mapping", thrift.MAP, 3)
		mapBeginOffset := offset
		offset += bthrift.Binary.MapBeginLength(thrift.I32, thrift.I32, 0)
		var length int
		for k, v := range p.OutputSlotMapping {
			length++

			offset += bthrift.Binary.WriteI32(buf[offset:], k)

			offset += bthrift.Binary.WriteI32(buf[offset:], v)

		}
		bthrift.Binary.WriteMapBegin(buf[mapBeginOffset:], thrift.I32, thrift.I32, length)
		offset += bthrift.Binary.WriteMapEnd(buf[offset:])
		offset += bthrift.Binary.WriteFieldEnd(buf[offset:])
	}
	return offset
}

func (p *TQueryCacheParam) fastWriteField4(buf []byte, binaryWriter bthrift.BinaryWriter) int {
	offset := 0
	if p.IsSetTabletToRange() {
		offset += bthrift.Binary.WriteFieldBegin(buf[offset:], "tablet_to_range", thrift.MAP, 4)
		mapBeginOffset := offset
		offset += bthrift.Binary.MapBeginLength(thrift.I64, thrift.STRING, 0)
		var length int
		for k, v := range p.TabletToRange {
			length++

			offset += bthrift.Binary.WriteI64(buf[offset:], k)

			offset += bthrift.Binary.WriteStringNocopy(buf[offset:], binaryWriter, v)

		}
		bthrift.Binary.WriteMapBegin(buf[mapBeginOffset:], thrift.I64, thrift.STRING, length)
		offset += bthrift.Binary.WriteMapEnd(buf[offset:])
		offset += bthrift.Binary.WriteFieldEnd(buf[offset:])
	}
	return offset
}

func (p *TQueryCacheParam) fastWriteField5(buf []byte, binaryWriter bthrift.BinaryWriter) int {
	offset := 0
	if p.IsSetForceRefreshQueryCache() {
		offset += bthrift.Binary.WriteFieldBegin(buf[offset:], "force_refresh_query_cache", thrift.BOOL, 5)
		offset += bthrift.Binary.WriteBool(buf[offset:], *p.ForceRefreshQueryCache)

		offset += bthrift.Binary.WriteFieldEnd(buf[offset:])
	}
	return offset
}

func (p *TQueryCacheParam) fastWriteField6(buf []byte, binaryWriter bthrift.BinaryWriter) int {
	offset := 0
	if p.IsSetEntryMaxBytes() {
		offset += bthrift.Binary.WriteFieldBegin(buf[offset:], "entry_max_bytes", thrift.I64, 6)
		offset += bthrift.Binary.WriteI64(buf[offset:], *p.EntryMaxBytes)

		offset += bthrift.Binary.WriteFieldEnd(buf[offset:])
	}
	return offset
}

func (p *TQueryCacheParam) fastWriteField7(buf []byte, binaryWriter bthrift.BinaryWriter) int {
	offset := 0
	if p.IsSetEntryMaxRows() {
		offset += bthrift.Binary.WriteFieldBegin(buf[offset:], "entry_max_rows", thrift.I64, 7)
		offset += bthrift.Binary.WriteI64(buf[offset:], *p.EntryMaxRows)

		offset += bthrift.Binary.WriteFieldEnd(buf[offset:])
	}
	return offset
}

func (p *TQueryCacheParam) field1Length() int {
	l := 0
	if p.IsSetNodeId() {
		l += bthrift.Binary.FieldBeginLength("node_id", thrift.I32, 1)
		l += bthrift.Binary.I32Length(*p.NodeId)

		l += bthrift.Binary.FieldEndLength()
	}
	return l
}

func (p *TQueryCacheParam) field2Length() int {
	l := 0
	if p.IsSetDigest() {
		l += bthrift.Binary.FieldBeginLength("digest", thrift.STRING, 2)
		l += bthrift.Binary.BinaryLengthNocopy([]byte(p.Digest))

		l += bthrift.Binary.FieldEndLength()
	}
	return l
}

func (p *TQueryCacheParam) field3Length() int {
	l := 0
	if p.IsSetOutputSlotMapping() {
		l += bthrift.Binary.FieldBeginLength("output_slot_mapping", thrift.MAP, 3)
		l += bthrift.Binary.MapBeginLength(thrift.I32, thrift.I32, len(p.OutputSlotMapping))
		var tmpK int32
		var tmpV int32
		l += (bthrift.Binary.I32Length(int32(tmpK)) + bthrift.Binary.I32Length(int32(tmpV))) * len(p.OutputSlotMapping)
		l += bthrift.Binary.MapEndLength()
		l += bthrift.Binary.FieldEndLength()
	}
	return l
}

func (p *TQueryCacheParam) field4Length() int {
	l := 0
	if p.IsSetTabletToRange() {
		l += bthrift.Binary.FieldBeginLength("tablet_to_range", thrift.MAP, 4)
		l += bthrift.Binary.MapBeginLength(thrift.I64, thrift.STRING, len(p.TabletToRange))
		for k, v := range p.TabletToRange {

			l += bthrift.Binary.I64Length(k)

			l += bthrift.Binary.StringLengthNocopy(v)

		}
		l += bthrift.Binary.MapEndLength()
		l += bthrift.Binary.FieldEndLength()
	}
	return l
}

func (p *TQueryCacheParam) field5Length() int {
	l := 0
	if p.IsSetForceRefreshQueryCache() {
		l += bthrift.Binary.FieldBeginLength("force_refresh_query_cache", thrift.BOOL, 5)
		l += bthrift.Binary.BoolLength(*p.ForceRefreshQueryCache)

		l += bthrift.Binary.FieldEndLength()
	}
	return l
}

func (p *TQueryCacheParam) field6Length() int {
	l := 0
	if p.IsSetEntryMaxBytes() {
		l += bthrift.Binary.FieldBeginLength("entry_max_bytes", thrift.I64, 6)
		l += bthrift.Binary.I64Length(*p.EntryMaxBytes)

		l += bthrift.Binary.FieldEndLength()
	}
	return l
}

func (p *TQueryCacheParam) field7Length() int {
	l := 0
	if p.IsSetEntryMaxRows() {
		l += bthrift.Binary.FieldBeginLength("entry_max_rows", thrift.I64, 7)
		l += bthrift.Binary.I64Length(*p.EntryMaxRows)

		l += bthrift.Binary.FieldEndLength()
	}
	return l
}
