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

package array

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"math"
	"reflect"
	"sync/atomic"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/internal/debug"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/goccy/go-json"
)

// A BinaryBuilder is used to build a Binary array using the Append methods.
type BinaryBuilder struct {
	builder

	dtype   arrow.BinaryDataType
	offsets bufBuilder
	values  *byteBufferBuilder

	appendOffsetVal func(int)
	getOffsetVal    func(int) int
	maxCapacity     uint64
	offsetByteWidth int
}

// NewBinaryBuilder can be used for any of the variable length binary types,
// Binary, LargeBinary, String, LargeString by passing the appropriate data type
func NewBinaryBuilder(mem memory.Allocator, dtype arrow.BinaryDataType) *BinaryBuilder {
	var (
		offsets         bufBuilder
		offsetValFn     func(int)
		maxCapacity     uint64
		offsetByteWidth int
		getOffsetVal    func(int) int
	)
	switch dtype.Layout().Buffers[1].ByteWidth {
	case 4:
		b := newInt32BufferBuilder(mem)
		offsetValFn = func(v int) { b.AppendValue(int32(v)) }
		getOffsetVal = func(i int) int { return int(b.Value(i)) }
		offsets = b
		maxCapacity = math.MaxInt32
		offsetByteWidth = arrow.Int32SizeBytes
	case 8:
		b := newInt64BufferBuilder(mem)
		offsetValFn = func(v int) { b.AppendValue(int64(v)) }
		getOffsetVal = func(i int) int { return int(b.Value(i)) }
		offsets = b
		maxCapacity = math.MaxInt64
		offsetByteWidth = arrow.Int64SizeBytes
	}

	b := &BinaryBuilder{
		builder:         builder{refCount: 1, mem: mem},
		dtype:           dtype,
		offsets:         offsets,
		values:          newByteBufferBuilder(mem),
		appendOffsetVal: offsetValFn,
		maxCapacity:     maxCapacity,
		offsetByteWidth: offsetByteWidth,
		getOffsetVal:    getOffsetVal,
	}
	return b
}

func (b *BinaryBuilder) Type() arrow.DataType { return b.dtype }

// Release decreases the reference count by 1.
// When the reference count goes to zero, the memory is freed.
// Release may be called simultaneously from multiple goroutines.
func (b *BinaryBuilder) Release() {
	debug.Assert(atomic.LoadInt64(&b.refCount) > 0, "too many releases")

	if atomic.AddInt64(&b.refCount, -1) == 0 {
		if b.nullBitmap != nil {
			b.nullBitmap.Release()
			b.nullBitmap = nil
		}
		if b.offsets != nil {
			b.offsets.Release()
			b.offsets = nil
		}
		if b.values != nil {
			b.values.Release()
			b.values = nil
		}
	}
}

func (b *BinaryBuilder) Append(v []byte) {
	b.Reserve(1)
	b.appendNextOffset()
	b.values.Append(v)
	b.UnsafeAppendBoolToBitmap(true)
}

func (b *BinaryBuilder) AppendString(v string) {
	b.Append([]byte(v))
}

func (b *BinaryBuilder) AppendNull() {
	b.Reserve(1)
	b.appendNextOffset()
	b.UnsafeAppendBoolToBitmap(false)
}

func (b *BinaryBuilder) AppendEmptyValue() {
	b.Reserve(1)
	b.appendNextOffset()
	b.UnsafeAppendBoolToBitmap(true)
}

// AppendValues will append the values in the v slice. The valid slice determines which values
// in v are valid (not null). The valid slice must either be empty or be equal in length to v. If empty,
// all values in v are appended and considered valid.
func (b *BinaryBuilder) AppendValues(v [][]byte, valid []bool) {
	if len(v) != len(valid) && len(valid) != 0 {
		panic("len(v) != len(valid) && len(valid) != 0")
	}

	if len(v) == 0 {
		return
	}

	b.Reserve(len(v))
	for _, vv := range v {
		b.appendNextOffset()
		b.values.Append(vv)
	}

	b.builder.unsafeAppendBoolsToBitmap(valid, len(v))
}

// AppendStringValues will append the values in the v slice. The valid slice determines which values
// in v are valid (not null). The valid slice must either be empty or be equal in length to v. If empty,
// all values in v are appended and considered valid.
func (b *BinaryBuilder) AppendStringValues(v []string, valid []bool) {
	if len(v) != len(valid) && len(valid) != 0 {
		panic("len(v) != len(valid) && len(valid) != 0")
	}

	if len(v) == 0 {
		return
	}

	b.Reserve(len(v))
	for _, vv := range v {
		b.appendNextOffset()
		b.values.Append([]byte(vv))
	}

	b.builder.unsafeAppendBoolsToBitmap(valid, len(v))
}

func (b *BinaryBuilder) UnsafeAppend(v []byte) {
	b.appendNextOffset()
	b.values.unsafeAppend(v)
	b.UnsafeAppendBoolToBitmap(true)
}

func (b *BinaryBuilder) Value(i int) []byte {
	start := b.getOffsetVal(i)
	var end int
	if i == (b.length - 1) {
		end = b.values.Len()
	} else {
		end = b.getOffsetVal(i + 1)
	}
	return b.values.Bytes()[start:end]
}

func (b *BinaryBuilder) init(capacity int) {
	b.builder.init(capacity)
	b.offsets.resize((capacity + 1) * b.offsetByteWidth)
}

// DataLen returns the number of bytes in the data array.
func (b *BinaryBuilder) DataLen() int { return b.values.length }

// DataCap returns the total number of bytes that can be stored
// without allocating additional memory.
func (b *BinaryBuilder) DataCap() int { return b.values.capacity }

// Reserve ensures there is enough space for appending n elements
// by checking the capacity and calling Resize if necessary.
func (b *BinaryBuilder) Reserve(n int) {
	b.builder.reserve(n, b.Resize)
}

// ReserveData ensures there is enough space for appending n bytes
// by checking the capacity and resizing the data buffer if necessary.
func (b *BinaryBuilder) ReserveData(n int) {
	if b.values.capacity < b.values.length+n {
		b.values.resize(b.values.Len() + n)
	}
}

// Resize adjusts the space allocated by b to n elements. If n is greater than b.Cap(),
// additional memory will be allocated. If n is smaller, the allocated memory may be reduced.
func (b *BinaryBuilder) Resize(n int) {
	b.offsets.resize((n + 1) * b.offsetByteWidth)
	if (n * b.offsetByteWidth) < b.offsets.Len() {
		b.offsets.SetLength(n * b.offsetByteWidth)
	}
	b.builder.resize(n, b.init)
}

func (b *BinaryBuilder) ResizeData(n int) {
	b.values.length = n
}

// NewArray creates a Binary array from the memory buffers used by the builder and resets the BinaryBuilder
// so it can be used to build a new array.
//
// Builds the appropriate Binary or LargeBinary array based on the datatype
// it was initialized with.
func (b *BinaryBuilder) NewArray() arrow.Array {
	if b.offsetByteWidth == arrow.Int32SizeBytes {
		return b.NewBinaryArray()
	}
	return b.NewLargeBinaryArray()
}

// NewBinaryArray creates a Binary array from the memory buffers used by the builder and resets the BinaryBuilder
// so it can be used to build a new array.
func (b *BinaryBuilder) NewBinaryArray() (a *Binary) {
	if b.offsetByteWidth != arrow.Int32SizeBytes {
		panic("arrow/array: invalid call to NewBinaryArray when building a LargeBinary array")
	}

	data := b.newData()
	a = NewBinaryData(data)
	data.Release()
	return
}

func (b *BinaryBuilder) NewLargeBinaryArray() (a *LargeBinary) {
	if b.offsetByteWidth != arrow.Int64SizeBytes {
		panic("arrow/array: invalid call to NewLargeBinaryArray when building a Binary array")
	}

	data := b.newData()
	a = NewLargeBinaryData(data)
	data.Release()
	return
}

func (b *BinaryBuilder) newData() (data *Data) {
	b.appendNextOffset()
	offsets, values := b.offsets.Finish(), b.values.Finish()
	data = NewData(b.dtype, b.length, []*memory.Buffer{b.nullBitmap, offsets, values}, nil, b.nulls, 0)
	if offsets != nil {
		offsets.Release()
	}

	if values != nil {
		values.Release()
	}

	b.builder.reset()

	return
}

func (b *BinaryBuilder) appendNextOffset() {
	numBytes := b.values.Len()
	debug.Assert(uint64(numBytes) <= b.maxCapacity, "exceeded maximum capacity of binary array")
	b.appendOffsetVal(numBytes)
}

func (b *BinaryBuilder) unmarshalOne(dec *json.Decoder) error {
	t, err := dec.Token()
	if err != nil {
		return err
	}

	switch v := t.(type) {
	case string:
		data, err := base64.StdEncoding.DecodeString(v)
		if err != nil {
			return err
		}
		b.Append(data)
	case []byte:
		b.Append(v)
	case nil:
		b.AppendNull()
	default:
		return &json.UnmarshalTypeError{
			Value:  fmt.Sprint(t),
			Type:   reflect.TypeOf([]byte{}),
			Offset: dec.InputOffset(),
		}
	}
	return nil
}

func (b *BinaryBuilder) unmarshal(dec *json.Decoder) error {
	for dec.More() {
		if err := b.unmarshalOne(dec); err != nil {
			return err
		}
	}
	return nil
}

func (b *BinaryBuilder) UnmarshalJSON(data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	t, err := dec.Token()
	if err != nil {
		return err
	}

	if delim, ok := t.(json.Delim); !ok || delim != '[' {
		return fmt.Errorf("binary builder must unpack from json array, found %s", delim)
	}

	return b.unmarshal(dec)
}

var (
	_ Builder = (*BinaryBuilder)(nil)
)
