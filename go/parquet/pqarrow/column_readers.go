// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pqarrow

import (
	"encoding/binary"
	"reflect"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/apache/arrow/go/v7/arrow"
	"github.com/apache/arrow/go/v7/arrow/array"
	"github.com/apache/arrow/go/v7/arrow/bitutil"
	"github.com/apache/arrow/go/v7/arrow/decimal128"
	"github.com/apache/arrow/go/v7/arrow/memory"
	"github.com/apache/arrow/go/v7/parquet"
	"github.com/apache/arrow/go/v7/parquet/file"
	"github.com/apache/arrow/go/v7/parquet/internal/utils"
	"github.com/apache/arrow/go/v7/parquet/schema"
	"golang.org/x/xerrors"
)

type leafReader struct {
	out       *array.Chunked
	rctx      *readerCtx
	field     *arrow.Field
	input     *columnIterator
	descr     *schema.Column
	recordRdr file.RecordReader

	refCount int64
}

func newLeafReader(rctx *readerCtx, field *arrow.Field, input *columnIterator, leafInfo file.LevelInfo) (*ColumnReader, error) {
	ret := &leafReader{
		rctx:      rctx,
		field:     field,
		input:     input,
		descr:     input.Descr(),
		recordRdr: file.NewRecordReader(input.Descr(), leafInfo, field.Type.ID() == arrow.DICTIONARY, rctx.mem),
		refCount:  1,
	}
	err := ret.nextRowGroup()
	return &ColumnReader{ret}, err
}

func (lr *leafReader) Retain() {
	atomic.AddInt64(&lr.refCount, 1)
}

func (lr *leafReader) Release() {
	if atomic.AddInt64(&lr.refCount, -1) == 0 {
		if lr.out != nil {
			lr.out.Release()
			lr.out = nil
		}
		if lr.recordRdr != nil {
			lr.recordRdr.Release()
			lr.recordRdr = nil
		}
	}
}

func (lr *leafReader) GetDefLevels() ([]int16, error) {
	return lr.recordRdr.DefLevels()[:int(lr.recordRdr.LevelsPos())], nil
}

func (lr *leafReader) GetRepLevels() ([]int16, error) {
	return lr.recordRdr.RepLevels()[:int(lr.recordRdr.LevelsPos())], nil
}

func (lr *leafReader) IsOrHasRepeatedChild() bool { return false }

func (lr *leafReader) LoadBatch(nrecords int64) (err error) {
	if lr.out != nil {
		lr.out.Release()
		lr.out = nil
	}
	lr.recordRdr.Reset()

	if err := lr.recordRdr.Reserve(nrecords); err != nil {
		return err
	}
	for nrecords > 0 {
		if !lr.recordRdr.HasMore() {
			break
		}
		numRead, err := lr.recordRdr.ReadRecords(nrecords)
		if err != nil {
			return err
		}
		nrecords -= numRead
		if numRead == 0 {
			if err = lr.nextRowGroup(); err != nil {
				return err
			}
		}
	}
	lr.out, err = transferColumnData(lr.recordRdr, lr.field.Type, lr.descr, lr.rctx.mem)
	return
}

func (lr *leafReader) BuildArray(_ int64) (*array.Chunked, error) {
	return lr.out, nil
}

func (lr *leafReader) Field() *arrow.Field { return lr.field }

func (lr *leafReader) nextRowGroup() error {
	pr, err := lr.input.NextChunk()
	if err != nil {
		return err
	}
	lr.recordRdr.SetPageReader(pr)
	return nil
}

type structReader struct {
	rctx             *readerCtx
	filtered         *arrow.Field
	levelInfo        file.LevelInfo
	children         []*ColumnReader
	defRepLevelChild *ColumnReader
	hasRepeatedChild bool

	refCount int64
}

func (sr *structReader) Retain() {
	atomic.AddInt64(&sr.refCount, 1)
}

func (sr *structReader) Release() {
	if atomic.AddInt64(&sr.refCount, -1) == 0 {
		if sr.defRepLevelChild != nil {
			sr.defRepLevelChild.Release()
			sr.defRepLevelChild = nil
		}
		for _, c := range sr.children {
			c.Release()
		}
		sr.children = nil
	}
}

func newStructReader(rctx *readerCtx, filtered *arrow.Field, levelInfo file.LevelInfo, children []*ColumnReader) *ColumnReader {
	// there could be a mix of children some might be repeated and some might not be
	// if possible use one that isn't since that will be guaranteed to have the least
	// number of levels to reconstruct a nullable bitmap
	var result *ColumnReader
	for _, child := range children {
		if !child.IsOrHasRepeatedChild() {
			result = child
		}
	}

	ret := &structReader{
		rctx:      rctx,
		filtered:  filtered,
		levelInfo: levelInfo,
		children:  children,
		refCount:  1,
	}
	if result != nil {
		ret.defRepLevelChild = result
		ret.hasRepeatedChild = false
	} else {
		ret.defRepLevelChild = children[0]
		ret.hasRepeatedChild = true
	}
	ret.defRepLevelChild.Retain()
	return &ColumnReader{ret}
}

func (sr *structReader) IsOrHasRepeatedChild() bool { return sr.hasRepeatedChild }

func (sr *structReader) GetDefLevels() ([]int16, error) {
	if len(sr.children) == 0 {
		return nil, xerrors.New("struct raeder has no children")
	}

	// this method should only be called when this struct or one of its parents
	// are optional/repeated or has a repeated child
	// meaning all children must have rep/def levels associated with them
	return sr.defRepLevelChild.GetDefLevels()
}

func (sr *structReader) GetRepLevels() ([]int16, error) {
	if len(sr.children) == 0 {
		return nil, xerrors.New("struct raeder has no children")
	}

	// this method should only be called when this struct or one of its parents
	// are optional/repeated or has a repeated child
	// meaning all children must have rep/def levels associated with them
	return sr.defRepLevelChild.GetRepLevels()
}

func (sr *structReader) LoadBatch(nrecords int64) error {
	for _, rdr := range sr.children {
		if err := rdr.LoadBatch(nrecords); err != nil {
			return err
		}
	}
	return nil
}

func (sr *structReader) Field() *arrow.Field { return sr.filtered }

func (sr *structReader) BuildArray(lenBound int64) (*array.Chunked, error) {
	validityIO := file.ValidityBitmapInputOutput{
		ReadUpperBound: lenBound,
		Read:           lenBound,
	}

	var nullBitmap *memory.Buffer

	if sr.hasRepeatedChild {
		nullBitmap = memory.NewResizableBuffer(sr.rctx.mem)
		nullBitmap.Resize(int(bitutil.BytesForBits(lenBound)))
		validityIO.ValidBits = nullBitmap.Bytes()
		defLevels, err := sr.GetDefLevels()
		if err != nil {
			return nil, err
		}
		repLevels, err := sr.GetRepLevels()
		if err != nil {
			return nil, err
		}

		if err := file.DefRepLevelsToBitmap(defLevels, repLevels, sr.levelInfo, &validityIO); err != nil {
			return nil, err
		}

	} else if sr.filtered.Nullable {
		nullBitmap = memory.NewResizableBuffer(sr.rctx.mem)
		nullBitmap.Resize(int(bitutil.BytesForBits(lenBound)))
		validityIO.ValidBits = nullBitmap.Bytes()
		defLevels, err := sr.GetDefLevels()
		if err != nil {
			return nil, err
		}

		file.DefLevelsToBitmap(defLevels, sr.levelInfo, &validityIO)
	}

	if nullBitmap != nil {
		nullBitmap.Resize(int(bitutil.BytesForBits(validityIO.Read)))
	}

	childArrData := make([]*array.Data, 0)
	// gather children arrays and def levels
	for _, child := range sr.children {
		field, err := child.BuildArray(validityIO.Read)
		if err != nil {
			return nil, err
		}
		arrdata, err := chunksToSingle(field)
		if err != nil {
			return nil, err
		}
		childArrData = append(childArrData, arrdata)
	}

	if !sr.filtered.Nullable && !sr.hasRepeatedChild {
		validityIO.Read = int64(childArrData[0].Len())
	}

	buffers := make([]*memory.Buffer, 1)
	if validityIO.NullCount > 0 {
		buffers[0] = nullBitmap
	}

	data := array.NewData(sr.filtered.Type, int(validityIO.Read), buffers, childArrData, int(validityIO.NullCount), 0)
	defer data.Release()
	arr := array.MakeFromData(data)
	defer arr.Release()
	return array.NewChunked(sr.filtered.Type, []array.Interface{arr}), nil
}

type listReader struct {
	rctx    *readerCtx
	field   *arrow.Field
	info    file.LevelInfo
	itemRdr *ColumnReader

	refCount int64
}

func newListReader(rctx *readerCtx, field *arrow.Field, info file.LevelInfo, childRdr *ColumnReader) *ColumnReader {
	childRdr.Retain()
	return &ColumnReader{&listReader{rctx, field, info, childRdr, 1}}
}

func (lr *listReader) Retain() {
	atomic.AddInt64(&lr.refCount, 1)
}

func (lr *listReader) Release() {
	if atomic.AddInt64(&lr.refCount, -1) == 0 {
		if lr.itemRdr != nil {
			lr.itemRdr.Release()
			lr.itemRdr = nil
		}
	}
}

func (lr *listReader) GetDefLevels() ([]int16, error) {
	return lr.itemRdr.GetDefLevels()
}

func (lr *listReader) GetRepLevels() ([]int16, error) {
	return lr.itemRdr.GetRepLevels()
}

func (lr *listReader) Field() *arrow.Field { return lr.field }

func (lr *listReader) IsOrHasRepeatedChild() bool { return true }

func (lr *listReader) LoadBatch(nrecords int64) error {
	return lr.itemRdr.LoadBatch(nrecords)
}

func (lr *listReader) BuildArray(lenBound int64) (*array.Chunked, error) {
	var (
		defLevels      []int16
		repLevels      []int16
		err            error
		validityBuffer *memory.Buffer
	)

	if defLevels, err = lr.itemRdr.GetDefLevels(); err != nil {
		return nil, err
	}
	if repLevels, err = lr.itemRdr.GetRepLevels(); err != nil {
		return nil, err
	}

	validityIO := file.ValidityBitmapInputOutput{ReadUpperBound: lenBound}
	if lr.field.Nullable {
		validityBuffer = memory.NewResizableBuffer(lr.rctx.mem)
		validityBuffer.Resize(int(bitutil.BytesForBits(lenBound)))
		defer validityBuffer.Release()
		validityIO.ValidBits = validityBuffer.Bytes()
	}
	offsetsBuffer := memory.NewResizableBuffer(lr.rctx.mem)
	offsetsBuffer.Resize(arrow.Int32Traits.BytesRequired(int(lenBound) + 1))
	defer offsetsBuffer.Release()

	offsetData := arrow.Int32Traits.CastFromBytes(offsetsBuffer.Bytes())
	if err = file.DefRepLevelsToListInfo(defLevels, repLevels, lr.info, &validityIO, offsetData); err != nil {
		return nil, err
	}

	arr, err := lr.itemRdr.BuildArray(int64(offsetData[int(validityIO.Read)]))
	if err != nil {
		return nil, err
	}

	// resize to actual number of elems returned
	offsetsBuffer.Resize(arrow.Int32Traits.BytesRequired(int(validityIO.Read) + 1))
	if validityBuffer != nil {
		validityBuffer.Resize(int(bitutil.BytesForBits(validityIO.Read)))
	}

	item, err := chunksToSingle(arr)
	if err != nil {
		return nil, err
	}
	defer item.Release()

	buffers := []*memory.Buffer{nil, offsetsBuffer}
	if validityIO.NullCount > 0 {
		buffers[0] = validityBuffer
	}

	data := array.NewData(lr.field.Type, int(validityIO.Read), buffers, []*array.Data{item}, int(validityIO.NullCount), 0)
	defer data.Release()
	if lr.field.Type.ID() == arrow.FIXED_SIZE_LIST {
		defer data.Buffers()[1].Release()
		listSize := lr.field.Type.(*arrow.FixedSizeListType).Len()
		for x := 1; x < data.Len(); x++ {
			size := offsetData[x] - offsetData[x-1]
			if size != listSize {
				return nil, xerrors.Errorf("expected all lists to be of size=%d, but index %d had size=%d", listSize, x, size)
			}
		}
		data.Buffers()[1] = nil
	}
	return array.NewChunked(lr.field.Type, []array.Interface{array.MakeFromData(data)}), nil
}

type fixedSizeListReader struct {
	listReader
}

func newFixedSizeListReader(rctx *readerCtx, field *arrow.Field, info file.LevelInfo, childRdr *ColumnReader) *ColumnReader {
	childRdr.Retain()
	return &ColumnReader{&fixedSizeListReader{listReader{rctx, field, info, childRdr, 1}}}
}

func chunksToSingle(chunked *array.Chunked) (*array.Data, error) {
	switch len(chunked.Chunks()) {
	case 0:
		return array.NewData(chunked.DataType(), 0, []*memory.Buffer{nil, nil}, nil, 0, 0), nil
	case 1:
		return chunked.Chunk(0).Data(), nil
	default:
		return nil, xerrors.New("not implemented")
	}
}

func transferColumnData(rdr file.RecordReader, valueType arrow.DataType, descr *schema.Column, mem memory.Allocator) (*array.Chunked, error) {
	var data array.Interface
	switch valueType.ID() {
	// case arrow.DICTIONARY:
	case arrow.NULL:
		return array.NewChunked(arrow.Null, []array.Interface{array.NewNull(rdr.ValuesWritten())}), nil
	case arrow.INT32, arrow.INT64, arrow.FLOAT32, arrow.FLOAT64:
		data = transferZeroCopy(rdr, valueType)
	case arrow.BOOL:
		data = transferBool(rdr)
	case arrow.UINT8,
		arrow.UINT16,
		arrow.UINT32,
		arrow.UINT64,
		arrow.INT8,
		arrow.INT16,
		arrow.DATE32,
		arrow.TIME32,
		arrow.TIME64:
		data = transferInt(rdr, valueType)
	case arrow.DATE64:
		data = transferDate64(rdr, valueType)
	case arrow.FIXED_SIZE_BINARY, arrow.BINARY, arrow.STRING:
		return transferBinary(rdr, valueType), nil
	case arrow.DECIMAL:
		switch descr.PhysicalType() {
		case parquet.Types.Int32, parquet.Types.Int64:
			data = transferDecimalInteger(rdr, valueType)
		case parquet.Types.ByteArray, parquet.Types.FixedLenByteArray:
			return transferDecimalBytes(rdr.(file.BinaryRecordReader), valueType)
		default:
			return nil, xerrors.New("physical type for decimal128 must be int32, int64, bytearray or fixed len byte array")
		}
	case arrow.TIMESTAMP:
		tstype := valueType.(*arrow.TimestampType)
		switch tstype.Unit {
		case arrow.Millisecond, arrow.Microsecond:
			data = transferZeroCopy(rdr, valueType)
		case arrow.Nanosecond:
			if descr.PhysicalType() == parquet.Types.Int96 {
				data = transferInt96(rdr, valueType)
			} else {
				data = transferZeroCopy(rdr, valueType)
			}
		default:
			return nil, xerrors.New("time unit not supported")
		}
	default:
		return nil, xerrors.Errorf("no support for reading columns of type: %s", valueType.Name())
	}

	defer data.Release()
	return array.NewChunked(valueType, []array.Interface{data}), nil
}

func transferZeroCopy(rdr file.RecordReader, dt arrow.DataType) array.Interface {
	return array.MakeFromData(array.NewData(dt, rdr.ValuesWritten(), []*memory.Buffer{
		rdr.ReleaseValidBits(), rdr.ReleaseValues(),
	}, nil, int(rdr.NullCount()), 0))
}

func transferBinary(rdr file.RecordReader, dt arrow.DataType) *array.Chunked {
	brdr := rdr.(file.BinaryRecordReader)
	chunks := brdr.GetBuilderChunks()
	if dt == arrow.BinaryTypes.String {
		// convert chunks from binary to string without copying data
		for idx := range chunks {
			chunks[idx] = array.MakeFromData(chunks[idx].Data())
		}
	}
	return array.NewChunked(dt, chunks)
}

func transferInt(rdr file.RecordReader, dt arrow.DataType) array.Interface {
	var (
		output reflect.Value
	)

	signed := true
	data := make([]byte, rdr.ValuesWritten()*int(bitutil.BytesForBits(int64(dt.(arrow.FixedWidthDataType).BitWidth()))))
	switch dt.ID() {
	case arrow.INT8:
		output = reflect.ValueOf(arrow.Int8Traits.CastFromBytes(data))
	case arrow.UINT8:
		signed = false
		output = reflect.ValueOf(arrow.Uint8Traits.CastFromBytes(data))
	case arrow.INT16:
		output = reflect.ValueOf(arrow.Int16Traits.CastFromBytes(data))
	case arrow.UINT16:
		signed = false
		output = reflect.ValueOf(arrow.Uint16Traits.CastFromBytes(data))
	case arrow.UINT32:
		signed = false
		output = reflect.ValueOf(arrow.Uint32Traits.CastFromBytes(data))
	case arrow.UINT64:
		signed = false
		output = reflect.ValueOf(arrow.Uint64Traits.CastFromBytes(data))
	case arrow.DATE32:
		output = reflect.ValueOf(arrow.Date32Traits.CastFromBytes(data))
	case arrow.TIME32:
		output = reflect.ValueOf(arrow.Time32Traits.CastFromBytes(data))
	case arrow.TIME64:
		output = reflect.ValueOf(arrow.Time64Traits.CastFromBytes(data))
	}

	length := rdr.ValuesWritten()
	switch rdr.Type() {
	case parquet.Types.Int32:
		values := arrow.Int32Traits.CastFromBytes(rdr.Values())
		if signed {
			for idx, v := range values[:length] {
				output.Index(idx).SetInt(int64(v))
			}
		} else {
			for idx, v := range values[:length] {
				output.Index(idx).SetUint(uint64(v))
			}
		}
	case parquet.Types.Int64:
		values := arrow.Int64Traits.CastFromBytes(rdr.Values())
		if signed {
			for idx, v := range values[:length] {
				output.Index(idx).SetInt(v)
			}
		} else {
			for idx, v := range values[:length] {
				output.Index(idx).SetUint(uint64(v))
			}
		}
	}

	return array.MakeFromData(array.NewData(dt, rdr.ValuesWritten(), []*memory.Buffer{
		rdr.ReleaseValidBits(), memory.NewBufferBytes(data),
	}, nil, int(rdr.NullCount()), 0))
}

func transferBool(rdr file.RecordReader) array.Interface {
	// TODO(mtopol): optimize this so we don't convert bitmap to []bool back to bitmap
	length := rdr.ValuesWritten()
	data := make([]byte, int(bitutil.BytesForBits(int64(length))))
	bytedata := rdr.Values()
	values := *(*[]bool)(unsafe.Pointer(&bytedata))

	for idx, v := range values[:length] {
		if v {
			bitutil.SetBit(data, idx)
		}
	}

	return array.MakeFromData(array.NewData(&arrow.BooleanType{}, length, []*memory.Buffer{
		rdr.ReleaseValidBits(), memory.NewBufferBytes(data),
	}, nil, int(rdr.NullCount()), 0))
}

var milliPerDay = time.Duration(24 * time.Hour).Milliseconds()

func transferDate64(rdr file.RecordReader, dt arrow.DataType) array.Interface {
	length := rdr.ValuesWritten()
	values := arrow.Int32Traits.CastFromBytes(rdr.Values())

	data := make([]byte, arrow.Int64Traits.BytesRequired(length))
	out := arrow.Int64Traits.CastFromBytes(data)
	for idx, val := range values[:length] {
		out[idx] = int64(val) * milliPerDay
	}

	return array.MakeFromData(array.NewData(dt, length, []*memory.Buffer{
		rdr.ReleaseValidBits(), memory.NewBufferBytes(data),
	}, nil, int(rdr.NullCount()), 0))
}

func transferInt96(rdr file.RecordReader, dt arrow.DataType) array.Interface {
	length := rdr.ValuesWritten()
	values := parquet.Int96Traits.CastFromBytes(rdr.Values())

	data := make([]byte, arrow.Int64SizeBytes*length)
	out := arrow.Int64Traits.CastFromBytes(data)

	for idx, val := range values[:length] {
		if binary.LittleEndian.Uint32(val[8:]) == 0 {
			out[idx] = 0
		} else {
			out[idx] = val.ToTime().UnixNano()
		}
	}

	return array.MakeFromData(array.NewData(dt, length, []*memory.Buffer{
		rdr.ReleaseValidBits(), memory.NewBufferBytes(data),
	}, nil, int(rdr.NullCount()), 0))
}

func transferDecimalInteger(rdr file.RecordReader, dt arrow.DataType) array.Interface {
	length := rdr.ValuesWritten()

	var values reflect.Value
	switch rdr.Type() {
	case parquet.Types.Int32:
		values = reflect.ValueOf(arrow.Int32Traits.CastFromBytes(rdr.Values())[:length])
	case parquet.Types.Int64:
		values = reflect.ValueOf(arrow.Int64Traits.CastFromBytes(rdr.Values())[:length])
	}

	data := make([]byte, arrow.Decimal128Traits.BytesRequired(length))
	out := arrow.Decimal128Traits.CastFromBytes(data)
	for i := 0; i < values.Len(); i++ {
		out[i] = decimal128.FromI64(values.Index(i).Int())
	}

	var nullmap *memory.Buffer
	if rdr.NullCount() > 0 {
		nullmap = rdr.ReleaseValidBits()
	}
	return array.MakeFromData(array.NewData(dt, length, []*memory.Buffer{
		nullmap, memory.NewBufferBytes(data),
	}, nil, int(rdr.NullCount()), 0))
}

func uint64FromBigEndianShifted(buf []byte) uint64 {
	var (
		bytes [8]byte
	)
	copy(bytes[8-len(buf):], buf)
	return binary.BigEndian.Uint64(bytes[:])
}

func bigEndianToDecimal128(buf []byte) (decimal128.Num, error) {
	const (
		minDecimalBytes = 1
		maxDecimalBytes = 16
	)

	if len(buf) < minDecimalBytes || len(buf) > maxDecimalBytes {
		return decimal128.Num{}, xerrors.Errorf("length of byte array passed to bigEndianToDecimal128 was %d but must be between %d and %d",
			len(buf), minDecimalBytes, maxDecimalBytes)
	}

	// bytes are big endian so first byte is MSB and holds the sign bit
	isNeg := int8(buf[0]) < 0

	// 1. extract high bits
	highBitsOffset := utils.MaxInt(0, len(buf)-8)
	var (
		highBits uint64
		lowBits  uint64
		hi       int64
		lo       int64
	)
	highBits = uint64FromBigEndianShifted(buf[:highBitsOffset])

	if highBitsOffset == 8 {
		hi = int64(highBits)
	} else {
		if isNeg && len(buf) < maxDecimalBytes {
			hi = -1
		}

		hi = int64(uint64(hi) << (uint64(highBitsOffset) * 8))
		hi |= int64(highBits)
	}

	// 2. extract lower bits
	lowBitsOffset := utils.MinInt(len(buf), 8)
	lowBits = uint64FromBigEndianShifted(buf[highBitsOffset:])

	if lowBitsOffset == 8 {
		lo = int64(lowBits)
	} else {
		if isNeg && len(buf) < 8 {
			lo = -1
		}

		lo = int64(uint64(lo) << (uint64(lowBitsOffset) * 8))
		lo |= int64(lowBits)
	}

	return decimal128.New(hi, uint64(lo)), nil
}

type varOrFixedBin interface {
	array.Interface
	Value(i int) []byte
}

func transferDecimalBytes(rdr file.BinaryRecordReader, dt arrow.DataType) (*array.Chunked, error) {
	convert := func(arr array.Interface) (array.Interface, error) {
		length := arr.Len()
		data := make([]byte, arrow.Decimal128Traits.BytesRequired(length))
		out := arrow.Decimal128Traits.CastFromBytes(data)

		input := arr.(varOrFixedBin)
		nullCount := input.NullN()

		var err error
		for i := 0; i < length; i++ {
			if nullCount > 0 && input.IsNull(i) {
				continue
			}

			rec := input.Value(i)
			if len(rec) <= 0 {
				return nil, xerrors.Errorf("invalud BYTEARRAY length for type: %s", dt)
			}
			out[i], err = bigEndianToDecimal128(rec)
			if err != nil {
				return nil, err
			}
		}

		return array.MakeFromData(array.NewData(dt, length, []*memory.Buffer{
			input.Data().Buffers()[0], memory.NewBufferBytes(data),
		}, nil, nullCount, 0)), nil
	}

	chunks := rdr.GetBuilderChunks()
	var err error
	for idx, chunk := range chunks {
		defer chunk.Release()
		if chunks[idx], err = convert(chunk); err != nil {
			return nil, err
		}
	}
	return array.NewChunked(dt, chunks), nil
}
