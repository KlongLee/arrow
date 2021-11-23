// Code generated by tensor/numeric.gen_test.go.tmpl. DO NOT EDIT.

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

package tensor_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/apache/arrow/go/v7/arrow"
	"github.com/apache/arrow/go/v7/arrow/array"
	"github.com/apache/arrow/go/v7/arrow/memory"
	"github.com/apache/arrow/go/v7/arrow/tensor"
)

func TestTensorInt8(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	bld := array.NewInt8Builder(mem)
	defer bld.Release()

	raw := []int8{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	bld.AppendValues(raw, nil)

	arr := bld.NewInt8Array()
	defer arr.Release()

	var (
		shape = []int64{2, 5}
		names = []string{"x", "y"}
		bw    = int64(arrow.PrimitiveTypes.Int8.(arrow.FixedWidthDataType).BitWidth()) / 8
	)

	tsr := tensor.New(arr.Data(), shape, nil, names).(*tensor.Int8)
	defer tsr.Release()

	tsr.Retain()
	tsr.Release()

	if got, want := tsr.Len(), 10; got != want {
		t.Fatalf("invalid length: got=%d, want=%d", got, want)
	}

	if got, want := tsr.Shape(), shape; !reflect.DeepEqual(got, want) {
		t.Fatalf("invalid shape: got=%v, want=%v", got, want)
	}

	if got, want := tsr.Strides(), []int64{5 * bw, 1 * bw}; !reflect.DeepEqual(got, want) {
		t.Fatalf("invalid strides: got=%v, want=%v", got, want)
	}

	if got, want := tsr.NumDims(), 2; got != want {
		t.Fatalf("invalid dims: got=%d, want=%d", got, want)
	}

	for i, name := range names {
		if got, want := tsr.DimName(i), name; got != want {
			t.Fatalf("invalid dim-name[%d]: got=%q, want=%q", i, got, want)
		}
	}

	if got, want := tsr.DataType(), arr.DataType(); got != want {
		t.Fatalf("invalid data-type: got=%q, want=%q", got.Name(), want.Name())
	}

	if got, want := tsr.Data(), arr.Data(); got != want {
		t.Fatalf("invalid data: got=%v, want=%v", got, want)
	}

	if tsr.IsMutable() {
		t.Fatalf("should not be mutable")
	}

	if !tsr.IsContiguous() {
		t.Fatalf("should be contiguous")
	}

	if !tsr.IsRowMajor() || tsr.IsColMajor() {
		t.Fatalf("should be row-major")
	}

	if got, want := tsr.Int8Values(), raw; !reflect.DeepEqual(got, want) {
		t.Fatalf("invalid backing array: got=%v, want=%v", got, want)
	}

	for _, tc := range []struct {
		i []int64
		v int8
	}{
		{i: []int64{0, 0}, v: 1},
		{i: []int64{0, 1}, v: 2},
		{i: []int64{0, 2}, v: 3},
		{i: []int64{0, 3}, v: 4},
		{i: []int64{0, 4}, v: 5},
		{i: []int64{1, 0}, v: 6},
		{i: []int64{1, 1}, v: 7},
		{i: []int64{1, 2}, v: 8},
		{i: []int64{1, 3}, v: 9},
		{i: []int64{1, 4}, v: 10},
	} {
		t.Run(fmt.Sprintf("%v", tc.i), func(t *testing.T) {
			got := tsr.Value(tc.i)
			if got != tc.v {
				t.Fatalf("arr[%v]: got=%v, want=%v", tc.i, got, tc.v)
			}
		})
	}
}

func TestTensorInt16(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	bld := array.NewInt16Builder(mem)
	defer bld.Release()

	raw := []int16{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	bld.AppendValues(raw, nil)

	arr := bld.NewInt16Array()
	defer arr.Release()

	var (
		shape = []int64{2, 5}
		names = []string{"x", "y"}
		bw    = int64(arrow.PrimitiveTypes.Int16.(arrow.FixedWidthDataType).BitWidth()) / 8
	)

	tsr := tensor.New(arr.Data(), shape, nil, names).(*tensor.Int16)
	defer tsr.Release()

	tsr.Retain()
	tsr.Release()

	if got, want := tsr.Len(), 10; got != want {
		t.Fatalf("invalid length: got=%d, want=%d", got, want)
	}

	if got, want := tsr.Shape(), shape; !reflect.DeepEqual(got, want) {
		t.Fatalf("invalid shape: got=%v, want=%v", got, want)
	}

	if got, want := tsr.Strides(), []int64{5 * bw, 1 * bw}; !reflect.DeepEqual(got, want) {
		t.Fatalf("invalid strides: got=%v, want=%v", got, want)
	}

	if got, want := tsr.NumDims(), 2; got != want {
		t.Fatalf("invalid dims: got=%d, want=%d", got, want)
	}

	for i, name := range names {
		if got, want := tsr.DimName(i), name; got != want {
			t.Fatalf("invalid dim-name[%d]: got=%q, want=%q", i, got, want)
		}
	}

	if got, want := tsr.DataType(), arr.DataType(); got != want {
		t.Fatalf("invalid data-type: got=%q, want=%q", got.Name(), want.Name())
	}

	if got, want := tsr.Data(), arr.Data(); got != want {
		t.Fatalf("invalid data: got=%v, want=%v", got, want)
	}

	if tsr.IsMutable() {
		t.Fatalf("should not be mutable")
	}

	if !tsr.IsContiguous() {
		t.Fatalf("should be contiguous")
	}

	if !tsr.IsRowMajor() || tsr.IsColMajor() {
		t.Fatalf("should be row-major")
	}

	if got, want := tsr.Int16Values(), raw; !reflect.DeepEqual(got, want) {
		t.Fatalf("invalid backing array: got=%v, want=%v", got, want)
	}

	for _, tc := range []struct {
		i []int64
		v int16
	}{
		{i: []int64{0, 0}, v: 1},
		{i: []int64{0, 1}, v: 2},
		{i: []int64{0, 2}, v: 3},
		{i: []int64{0, 3}, v: 4},
		{i: []int64{0, 4}, v: 5},
		{i: []int64{1, 0}, v: 6},
		{i: []int64{1, 1}, v: 7},
		{i: []int64{1, 2}, v: 8},
		{i: []int64{1, 3}, v: 9},
		{i: []int64{1, 4}, v: 10},
	} {
		t.Run(fmt.Sprintf("%v", tc.i), func(t *testing.T) {
			got := tsr.Value(tc.i)
			if got != tc.v {
				t.Fatalf("arr[%v]: got=%v, want=%v", tc.i, got, tc.v)
			}
		})
	}
}

func TestTensorInt32(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	bld := array.NewInt32Builder(mem)
	defer bld.Release()

	raw := []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	bld.AppendValues(raw, nil)

	arr := bld.NewInt32Array()
	defer arr.Release()

	var (
		shape = []int64{2, 5}
		names = []string{"x", "y"}
		bw    = int64(arrow.PrimitiveTypes.Int32.(arrow.FixedWidthDataType).BitWidth()) / 8
	)

	tsr := tensor.New(arr.Data(), shape, nil, names).(*tensor.Int32)
	defer tsr.Release()

	tsr.Retain()
	tsr.Release()

	if got, want := tsr.Len(), 10; got != want {
		t.Fatalf("invalid length: got=%d, want=%d", got, want)
	}

	if got, want := tsr.Shape(), shape; !reflect.DeepEqual(got, want) {
		t.Fatalf("invalid shape: got=%v, want=%v", got, want)
	}

	if got, want := tsr.Strides(), []int64{5 * bw, 1 * bw}; !reflect.DeepEqual(got, want) {
		t.Fatalf("invalid strides: got=%v, want=%v", got, want)
	}

	if got, want := tsr.NumDims(), 2; got != want {
		t.Fatalf("invalid dims: got=%d, want=%d", got, want)
	}

	for i, name := range names {
		if got, want := tsr.DimName(i), name; got != want {
			t.Fatalf("invalid dim-name[%d]: got=%q, want=%q", i, got, want)
		}
	}

	if got, want := tsr.DataType(), arr.DataType(); got != want {
		t.Fatalf("invalid data-type: got=%q, want=%q", got.Name(), want.Name())
	}

	if got, want := tsr.Data(), arr.Data(); got != want {
		t.Fatalf("invalid data: got=%v, want=%v", got, want)
	}

	if tsr.IsMutable() {
		t.Fatalf("should not be mutable")
	}

	if !tsr.IsContiguous() {
		t.Fatalf("should be contiguous")
	}

	if !tsr.IsRowMajor() || tsr.IsColMajor() {
		t.Fatalf("should be row-major")
	}

	if got, want := tsr.Int32Values(), raw; !reflect.DeepEqual(got, want) {
		t.Fatalf("invalid backing array: got=%v, want=%v", got, want)
	}

	for _, tc := range []struct {
		i []int64
		v int32
	}{
		{i: []int64{0, 0}, v: 1},
		{i: []int64{0, 1}, v: 2},
		{i: []int64{0, 2}, v: 3},
		{i: []int64{0, 3}, v: 4},
		{i: []int64{0, 4}, v: 5},
		{i: []int64{1, 0}, v: 6},
		{i: []int64{1, 1}, v: 7},
		{i: []int64{1, 2}, v: 8},
		{i: []int64{1, 3}, v: 9},
		{i: []int64{1, 4}, v: 10},
	} {
		t.Run(fmt.Sprintf("%v", tc.i), func(t *testing.T) {
			got := tsr.Value(tc.i)
			if got != tc.v {
				t.Fatalf("arr[%v]: got=%v, want=%v", tc.i, got, tc.v)
			}
		})
	}
}

func TestTensorInt64(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	bld := array.NewInt64Builder(mem)
	defer bld.Release()

	raw := []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	bld.AppendValues(raw, nil)

	arr := bld.NewInt64Array()
	defer arr.Release()

	var (
		shape = []int64{2, 5}
		names = []string{"x", "y"}
		bw    = int64(arrow.PrimitiveTypes.Int64.(arrow.FixedWidthDataType).BitWidth()) / 8
	)

	tsr := tensor.New(arr.Data(), shape, nil, names).(*tensor.Int64)
	defer tsr.Release()

	tsr.Retain()
	tsr.Release()

	if got, want := tsr.Len(), 10; got != want {
		t.Fatalf("invalid length: got=%d, want=%d", got, want)
	}

	if got, want := tsr.Shape(), shape; !reflect.DeepEqual(got, want) {
		t.Fatalf("invalid shape: got=%v, want=%v", got, want)
	}

	if got, want := tsr.Strides(), []int64{5 * bw, 1 * bw}; !reflect.DeepEqual(got, want) {
		t.Fatalf("invalid strides: got=%v, want=%v", got, want)
	}

	if got, want := tsr.NumDims(), 2; got != want {
		t.Fatalf("invalid dims: got=%d, want=%d", got, want)
	}

	for i, name := range names {
		if got, want := tsr.DimName(i), name; got != want {
			t.Fatalf("invalid dim-name[%d]: got=%q, want=%q", i, got, want)
		}
	}

	if got, want := tsr.DataType(), arr.DataType(); got != want {
		t.Fatalf("invalid data-type: got=%q, want=%q", got.Name(), want.Name())
	}

	if got, want := tsr.Data(), arr.Data(); got != want {
		t.Fatalf("invalid data: got=%v, want=%v", got, want)
	}

	if tsr.IsMutable() {
		t.Fatalf("should not be mutable")
	}

	if !tsr.IsContiguous() {
		t.Fatalf("should be contiguous")
	}

	if !tsr.IsRowMajor() || tsr.IsColMajor() {
		t.Fatalf("should be row-major")
	}

	if got, want := tsr.Int64Values(), raw; !reflect.DeepEqual(got, want) {
		t.Fatalf("invalid backing array: got=%v, want=%v", got, want)
	}

	for _, tc := range []struct {
		i []int64
		v int64
	}{
		{i: []int64{0, 0}, v: 1},
		{i: []int64{0, 1}, v: 2},
		{i: []int64{0, 2}, v: 3},
		{i: []int64{0, 3}, v: 4},
		{i: []int64{0, 4}, v: 5},
		{i: []int64{1, 0}, v: 6},
		{i: []int64{1, 1}, v: 7},
		{i: []int64{1, 2}, v: 8},
		{i: []int64{1, 3}, v: 9},
		{i: []int64{1, 4}, v: 10},
	} {
		t.Run(fmt.Sprintf("%v", tc.i), func(t *testing.T) {
			got := tsr.Value(tc.i)
			if got != tc.v {
				t.Fatalf("arr[%v]: got=%v, want=%v", tc.i, got, tc.v)
			}
		})
	}
}

func TestTensorUint8(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	bld := array.NewUint8Builder(mem)
	defer bld.Release()

	raw := []uint8{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	bld.AppendValues(raw, nil)

	arr := bld.NewUint8Array()
	defer arr.Release()

	var (
		shape = []int64{2, 5}
		names = []string{"x", "y"}
		bw    = int64(arrow.PrimitiveTypes.Uint8.(arrow.FixedWidthDataType).BitWidth()) / 8
	)

	tsr := tensor.New(arr.Data(), shape, nil, names).(*tensor.Uint8)
	defer tsr.Release()

	tsr.Retain()
	tsr.Release()

	if got, want := tsr.Len(), 10; got != want {
		t.Fatalf("invalid length: got=%d, want=%d", got, want)
	}

	if got, want := tsr.Shape(), shape; !reflect.DeepEqual(got, want) {
		t.Fatalf("invalid shape: got=%v, want=%v", got, want)
	}

	if got, want := tsr.Strides(), []int64{5 * bw, 1 * bw}; !reflect.DeepEqual(got, want) {
		t.Fatalf("invalid strides: got=%v, want=%v", got, want)
	}

	if got, want := tsr.NumDims(), 2; got != want {
		t.Fatalf("invalid dims: got=%d, want=%d", got, want)
	}

	for i, name := range names {
		if got, want := tsr.DimName(i), name; got != want {
			t.Fatalf("invalid dim-name[%d]: got=%q, want=%q", i, got, want)
		}
	}

	if got, want := tsr.DataType(), arr.DataType(); got != want {
		t.Fatalf("invalid data-type: got=%q, want=%q", got.Name(), want.Name())
	}

	if got, want := tsr.Data(), arr.Data(); got != want {
		t.Fatalf("invalid data: got=%v, want=%v", got, want)
	}

	if tsr.IsMutable() {
		t.Fatalf("should not be mutable")
	}

	if !tsr.IsContiguous() {
		t.Fatalf("should be contiguous")
	}

	if !tsr.IsRowMajor() || tsr.IsColMajor() {
		t.Fatalf("should be row-major")
	}

	if got, want := tsr.Uint8Values(), raw; !reflect.DeepEqual(got, want) {
		t.Fatalf("invalid backing array: got=%v, want=%v", got, want)
	}

	for _, tc := range []struct {
		i []int64
		v uint8
	}{
		{i: []int64{0, 0}, v: 1},
		{i: []int64{0, 1}, v: 2},
		{i: []int64{0, 2}, v: 3},
		{i: []int64{0, 3}, v: 4},
		{i: []int64{0, 4}, v: 5},
		{i: []int64{1, 0}, v: 6},
		{i: []int64{1, 1}, v: 7},
		{i: []int64{1, 2}, v: 8},
		{i: []int64{1, 3}, v: 9},
		{i: []int64{1, 4}, v: 10},
	} {
		t.Run(fmt.Sprintf("%v", tc.i), func(t *testing.T) {
			got := tsr.Value(tc.i)
			if got != tc.v {
				t.Fatalf("arr[%v]: got=%v, want=%v", tc.i, got, tc.v)
			}
		})
	}
}

func TestTensorUint16(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	bld := array.NewUint16Builder(mem)
	defer bld.Release()

	raw := []uint16{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	bld.AppendValues(raw, nil)

	arr := bld.NewUint16Array()
	defer arr.Release()

	var (
		shape = []int64{2, 5}
		names = []string{"x", "y"}
		bw    = int64(arrow.PrimitiveTypes.Uint16.(arrow.FixedWidthDataType).BitWidth()) / 8
	)

	tsr := tensor.New(arr.Data(), shape, nil, names).(*tensor.Uint16)
	defer tsr.Release()

	tsr.Retain()
	tsr.Release()

	if got, want := tsr.Len(), 10; got != want {
		t.Fatalf("invalid length: got=%d, want=%d", got, want)
	}

	if got, want := tsr.Shape(), shape; !reflect.DeepEqual(got, want) {
		t.Fatalf("invalid shape: got=%v, want=%v", got, want)
	}

	if got, want := tsr.Strides(), []int64{5 * bw, 1 * bw}; !reflect.DeepEqual(got, want) {
		t.Fatalf("invalid strides: got=%v, want=%v", got, want)
	}

	if got, want := tsr.NumDims(), 2; got != want {
		t.Fatalf("invalid dims: got=%d, want=%d", got, want)
	}

	for i, name := range names {
		if got, want := tsr.DimName(i), name; got != want {
			t.Fatalf("invalid dim-name[%d]: got=%q, want=%q", i, got, want)
		}
	}

	if got, want := tsr.DataType(), arr.DataType(); got != want {
		t.Fatalf("invalid data-type: got=%q, want=%q", got.Name(), want.Name())
	}

	if got, want := tsr.Data(), arr.Data(); got != want {
		t.Fatalf("invalid data: got=%v, want=%v", got, want)
	}

	if tsr.IsMutable() {
		t.Fatalf("should not be mutable")
	}

	if !tsr.IsContiguous() {
		t.Fatalf("should be contiguous")
	}

	if !tsr.IsRowMajor() || tsr.IsColMajor() {
		t.Fatalf("should be row-major")
	}

	if got, want := tsr.Uint16Values(), raw; !reflect.DeepEqual(got, want) {
		t.Fatalf("invalid backing array: got=%v, want=%v", got, want)
	}

	for _, tc := range []struct {
		i []int64
		v uint16
	}{
		{i: []int64{0, 0}, v: 1},
		{i: []int64{0, 1}, v: 2},
		{i: []int64{0, 2}, v: 3},
		{i: []int64{0, 3}, v: 4},
		{i: []int64{0, 4}, v: 5},
		{i: []int64{1, 0}, v: 6},
		{i: []int64{1, 1}, v: 7},
		{i: []int64{1, 2}, v: 8},
		{i: []int64{1, 3}, v: 9},
		{i: []int64{1, 4}, v: 10},
	} {
		t.Run(fmt.Sprintf("%v", tc.i), func(t *testing.T) {
			got := tsr.Value(tc.i)
			if got != tc.v {
				t.Fatalf("arr[%v]: got=%v, want=%v", tc.i, got, tc.v)
			}
		})
	}
}

func TestTensorUint32(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	bld := array.NewUint32Builder(mem)
	defer bld.Release()

	raw := []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	bld.AppendValues(raw, nil)

	arr := bld.NewUint32Array()
	defer arr.Release()

	var (
		shape = []int64{2, 5}
		names = []string{"x", "y"}
		bw    = int64(arrow.PrimitiveTypes.Uint32.(arrow.FixedWidthDataType).BitWidth()) / 8
	)

	tsr := tensor.New(arr.Data(), shape, nil, names).(*tensor.Uint32)
	defer tsr.Release()

	tsr.Retain()
	tsr.Release()

	if got, want := tsr.Len(), 10; got != want {
		t.Fatalf("invalid length: got=%d, want=%d", got, want)
	}

	if got, want := tsr.Shape(), shape; !reflect.DeepEqual(got, want) {
		t.Fatalf("invalid shape: got=%v, want=%v", got, want)
	}

	if got, want := tsr.Strides(), []int64{5 * bw, 1 * bw}; !reflect.DeepEqual(got, want) {
		t.Fatalf("invalid strides: got=%v, want=%v", got, want)
	}

	if got, want := tsr.NumDims(), 2; got != want {
		t.Fatalf("invalid dims: got=%d, want=%d", got, want)
	}

	for i, name := range names {
		if got, want := tsr.DimName(i), name; got != want {
			t.Fatalf("invalid dim-name[%d]: got=%q, want=%q", i, got, want)
		}
	}

	if got, want := tsr.DataType(), arr.DataType(); got != want {
		t.Fatalf("invalid data-type: got=%q, want=%q", got.Name(), want.Name())
	}

	if got, want := tsr.Data(), arr.Data(); got != want {
		t.Fatalf("invalid data: got=%v, want=%v", got, want)
	}

	if tsr.IsMutable() {
		t.Fatalf("should not be mutable")
	}

	if !tsr.IsContiguous() {
		t.Fatalf("should be contiguous")
	}

	if !tsr.IsRowMajor() || tsr.IsColMajor() {
		t.Fatalf("should be row-major")
	}

	if got, want := tsr.Uint32Values(), raw; !reflect.DeepEqual(got, want) {
		t.Fatalf("invalid backing array: got=%v, want=%v", got, want)
	}

	for _, tc := range []struct {
		i []int64
		v uint32
	}{
		{i: []int64{0, 0}, v: 1},
		{i: []int64{0, 1}, v: 2},
		{i: []int64{0, 2}, v: 3},
		{i: []int64{0, 3}, v: 4},
		{i: []int64{0, 4}, v: 5},
		{i: []int64{1, 0}, v: 6},
		{i: []int64{1, 1}, v: 7},
		{i: []int64{1, 2}, v: 8},
		{i: []int64{1, 3}, v: 9},
		{i: []int64{1, 4}, v: 10},
	} {
		t.Run(fmt.Sprintf("%v", tc.i), func(t *testing.T) {
			got := tsr.Value(tc.i)
			if got != tc.v {
				t.Fatalf("arr[%v]: got=%v, want=%v", tc.i, got, tc.v)
			}
		})
	}
}

func TestTensorUint64(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	bld := array.NewUint64Builder(mem)
	defer bld.Release()

	raw := []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	bld.AppendValues(raw, nil)

	arr := bld.NewUint64Array()
	defer arr.Release()

	var (
		shape = []int64{2, 5}
		names = []string{"x", "y"}
		bw    = int64(arrow.PrimitiveTypes.Uint64.(arrow.FixedWidthDataType).BitWidth()) / 8
	)

	tsr := tensor.New(arr.Data(), shape, nil, names).(*tensor.Uint64)
	defer tsr.Release()

	tsr.Retain()
	tsr.Release()

	if got, want := tsr.Len(), 10; got != want {
		t.Fatalf("invalid length: got=%d, want=%d", got, want)
	}

	if got, want := tsr.Shape(), shape; !reflect.DeepEqual(got, want) {
		t.Fatalf("invalid shape: got=%v, want=%v", got, want)
	}

	if got, want := tsr.Strides(), []int64{5 * bw, 1 * bw}; !reflect.DeepEqual(got, want) {
		t.Fatalf("invalid strides: got=%v, want=%v", got, want)
	}

	if got, want := tsr.NumDims(), 2; got != want {
		t.Fatalf("invalid dims: got=%d, want=%d", got, want)
	}

	for i, name := range names {
		if got, want := tsr.DimName(i), name; got != want {
			t.Fatalf("invalid dim-name[%d]: got=%q, want=%q", i, got, want)
		}
	}

	if got, want := tsr.DataType(), arr.DataType(); got != want {
		t.Fatalf("invalid data-type: got=%q, want=%q", got.Name(), want.Name())
	}

	if got, want := tsr.Data(), arr.Data(); got != want {
		t.Fatalf("invalid data: got=%v, want=%v", got, want)
	}

	if tsr.IsMutable() {
		t.Fatalf("should not be mutable")
	}

	if !tsr.IsContiguous() {
		t.Fatalf("should be contiguous")
	}

	if !tsr.IsRowMajor() || tsr.IsColMajor() {
		t.Fatalf("should be row-major")
	}

	if got, want := tsr.Uint64Values(), raw; !reflect.DeepEqual(got, want) {
		t.Fatalf("invalid backing array: got=%v, want=%v", got, want)
	}

	for _, tc := range []struct {
		i []int64
		v uint64
	}{
		{i: []int64{0, 0}, v: 1},
		{i: []int64{0, 1}, v: 2},
		{i: []int64{0, 2}, v: 3},
		{i: []int64{0, 3}, v: 4},
		{i: []int64{0, 4}, v: 5},
		{i: []int64{1, 0}, v: 6},
		{i: []int64{1, 1}, v: 7},
		{i: []int64{1, 2}, v: 8},
		{i: []int64{1, 3}, v: 9},
		{i: []int64{1, 4}, v: 10},
	} {
		t.Run(fmt.Sprintf("%v", tc.i), func(t *testing.T) {
			got := tsr.Value(tc.i)
			if got != tc.v {
				t.Fatalf("arr[%v]: got=%v, want=%v", tc.i, got, tc.v)
			}
		})
	}
}

func TestTensorFloat32(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	bld := array.NewFloat32Builder(mem)
	defer bld.Release()

	raw := []float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	bld.AppendValues(raw, nil)

	arr := bld.NewFloat32Array()
	defer arr.Release()

	var (
		shape = []int64{2, 5}
		names = []string{"x", "y"}
		bw    = int64(arrow.PrimitiveTypes.Float32.(arrow.FixedWidthDataType).BitWidth()) / 8
	)

	tsr := tensor.New(arr.Data(), shape, nil, names).(*tensor.Float32)
	defer tsr.Release()

	tsr.Retain()
	tsr.Release()

	if got, want := tsr.Len(), 10; got != want {
		t.Fatalf("invalid length: got=%d, want=%d", got, want)
	}

	if got, want := tsr.Shape(), shape; !reflect.DeepEqual(got, want) {
		t.Fatalf("invalid shape: got=%v, want=%v", got, want)
	}

	if got, want := tsr.Strides(), []int64{5 * bw, 1 * bw}; !reflect.DeepEqual(got, want) {
		t.Fatalf("invalid strides: got=%v, want=%v", got, want)
	}

	if got, want := tsr.NumDims(), 2; got != want {
		t.Fatalf("invalid dims: got=%d, want=%d", got, want)
	}

	for i, name := range names {
		if got, want := tsr.DimName(i), name; got != want {
			t.Fatalf("invalid dim-name[%d]: got=%q, want=%q", i, got, want)
		}
	}

	if got, want := tsr.DataType(), arr.DataType(); got != want {
		t.Fatalf("invalid data-type: got=%q, want=%q", got.Name(), want.Name())
	}

	if got, want := tsr.Data(), arr.Data(); got != want {
		t.Fatalf("invalid data: got=%v, want=%v", got, want)
	}

	if tsr.IsMutable() {
		t.Fatalf("should not be mutable")
	}

	if !tsr.IsContiguous() {
		t.Fatalf("should be contiguous")
	}

	if !tsr.IsRowMajor() || tsr.IsColMajor() {
		t.Fatalf("should be row-major")
	}

	if got, want := tsr.Float32Values(), raw; !reflect.DeepEqual(got, want) {
		t.Fatalf("invalid backing array: got=%v, want=%v", got, want)
	}

	for _, tc := range []struct {
		i []int64
		v float32
	}{
		{i: []int64{0, 0}, v: 1},
		{i: []int64{0, 1}, v: 2},
		{i: []int64{0, 2}, v: 3},
		{i: []int64{0, 3}, v: 4},
		{i: []int64{0, 4}, v: 5},
		{i: []int64{1, 0}, v: 6},
		{i: []int64{1, 1}, v: 7},
		{i: []int64{1, 2}, v: 8},
		{i: []int64{1, 3}, v: 9},
		{i: []int64{1, 4}, v: 10},
	} {
		t.Run(fmt.Sprintf("%v", tc.i), func(t *testing.T) {
			got := tsr.Value(tc.i)
			if got != tc.v {
				t.Fatalf("arr[%v]: got=%v, want=%v", tc.i, got, tc.v)
			}
		})
	}
}

func TestTensorFloat64(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	bld := array.NewFloat64Builder(mem)
	defer bld.Release()

	raw := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	bld.AppendValues(raw, nil)

	arr := bld.NewFloat64Array()
	defer arr.Release()

	var (
		shape = []int64{2, 5}
		names = []string{"x", "y"}
		bw    = int64(arrow.PrimitiveTypes.Float64.(arrow.FixedWidthDataType).BitWidth()) / 8
	)

	tsr := tensor.New(arr.Data(), shape, nil, names).(*tensor.Float64)
	defer tsr.Release()

	tsr.Retain()
	tsr.Release()

	if got, want := tsr.Len(), 10; got != want {
		t.Fatalf("invalid length: got=%d, want=%d", got, want)
	}

	if got, want := tsr.Shape(), shape; !reflect.DeepEqual(got, want) {
		t.Fatalf("invalid shape: got=%v, want=%v", got, want)
	}

	if got, want := tsr.Strides(), []int64{5 * bw, 1 * bw}; !reflect.DeepEqual(got, want) {
		t.Fatalf("invalid strides: got=%v, want=%v", got, want)
	}

	if got, want := tsr.NumDims(), 2; got != want {
		t.Fatalf("invalid dims: got=%d, want=%d", got, want)
	}

	for i, name := range names {
		if got, want := tsr.DimName(i), name; got != want {
			t.Fatalf("invalid dim-name[%d]: got=%q, want=%q", i, got, want)
		}
	}

	if got, want := tsr.DataType(), arr.DataType(); got != want {
		t.Fatalf("invalid data-type: got=%q, want=%q", got.Name(), want.Name())
	}

	if got, want := tsr.Data(), arr.Data(); got != want {
		t.Fatalf("invalid data: got=%v, want=%v", got, want)
	}

	if tsr.IsMutable() {
		t.Fatalf("should not be mutable")
	}

	if !tsr.IsContiguous() {
		t.Fatalf("should be contiguous")
	}

	if !tsr.IsRowMajor() || tsr.IsColMajor() {
		t.Fatalf("should be row-major")
	}

	if got, want := tsr.Float64Values(), raw; !reflect.DeepEqual(got, want) {
		t.Fatalf("invalid backing array: got=%v, want=%v", got, want)
	}

	for _, tc := range []struct {
		i []int64
		v float64
	}{
		{i: []int64{0, 0}, v: 1},
		{i: []int64{0, 1}, v: 2},
		{i: []int64{0, 2}, v: 3},
		{i: []int64{0, 3}, v: 4},
		{i: []int64{0, 4}, v: 5},
		{i: []int64{1, 0}, v: 6},
		{i: []int64{1, 1}, v: 7},
		{i: []int64{1, 2}, v: 8},
		{i: []int64{1, 3}, v: 9},
		{i: []int64{1, 4}, v: 10},
	} {
		t.Run(fmt.Sprintf("%v", tc.i), func(t *testing.T) {
			got := tsr.Value(tc.i)
			if got != tc.v {
				t.Fatalf("arr[%v]: got=%v, want=%v", tc.i, got, tc.v)
			}
		})
	}
}

func TestTensorDate32(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	bld := array.NewDate32Builder(mem)
	defer bld.Release()

	raw := []arrow.Date32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	bld.AppendValues(raw, nil)

	arr := bld.NewDate32Array()
	defer arr.Release()

	var (
		shape = []int64{2, 5}
		names = []string{"x", "y"}
		bw    = int64(arrow.PrimitiveTypes.Date32.(arrow.FixedWidthDataType).BitWidth()) / 8
	)

	tsr := tensor.New(arr.Data(), shape, nil, names).(*tensor.Date32)
	defer tsr.Release()

	tsr.Retain()
	tsr.Release()

	if got, want := tsr.Len(), 10; got != want {
		t.Fatalf("invalid length: got=%d, want=%d", got, want)
	}

	if got, want := tsr.Shape(), shape; !reflect.DeepEqual(got, want) {
		t.Fatalf("invalid shape: got=%v, want=%v", got, want)
	}

	if got, want := tsr.Strides(), []int64{5 * bw, 1 * bw}; !reflect.DeepEqual(got, want) {
		t.Fatalf("invalid strides: got=%v, want=%v", got, want)
	}

	if got, want := tsr.NumDims(), 2; got != want {
		t.Fatalf("invalid dims: got=%d, want=%d", got, want)
	}

	for i, name := range names {
		if got, want := tsr.DimName(i), name; got != want {
			t.Fatalf("invalid dim-name[%d]: got=%q, want=%q", i, got, want)
		}
	}

	if got, want := tsr.DataType(), arr.DataType(); got != want {
		t.Fatalf("invalid data-type: got=%q, want=%q", got.Name(), want.Name())
	}

	if got, want := tsr.Data(), arr.Data(); got != want {
		t.Fatalf("invalid data: got=%v, want=%v", got, want)
	}

	if tsr.IsMutable() {
		t.Fatalf("should not be mutable")
	}

	if !tsr.IsContiguous() {
		t.Fatalf("should be contiguous")
	}

	if !tsr.IsRowMajor() || tsr.IsColMajor() {
		t.Fatalf("should be row-major")
	}

	if got, want := tsr.Date32Values(), raw; !reflect.DeepEqual(got, want) {
		t.Fatalf("invalid backing array: got=%v, want=%v", got, want)
	}

	for _, tc := range []struct {
		i []int64
		v arrow.Date32
	}{
		{i: []int64{0, 0}, v: 1},
		{i: []int64{0, 1}, v: 2},
		{i: []int64{0, 2}, v: 3},
		{i: []int64{0, 3}, v: 4},
		{i: []int64{0, 4}, v: 5},
		{i: []int64{1, 0}, v: 6},
		{i: []int64{1, 1}, v: 7},
		{i: []int64{1, 2}, v: 8},
		{i: []int64{1, 3}, v: 9},
		{i: []int64{1, 4}, v: 10},
	} {
		t.Run(fmt.Sprintf("%v", tc.i), func(t *testing.T) {
			got := tsr.Value(tc.i)
			if got != tc.v {
				t.Fatalf("arr[%v]: got=%v, want=%v", tc.i, got, tc.v)
			}
		})
	}
}

func TestTensorDate64(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	bld := array.NewDate64Builder(mem)
	defer bld.Release()

	raw := []arrow.Date64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	bld.AppendValues(raw, nil)

	arr := bld.NewDate64Array()
	defer arr.Release()

	var (
		shape = []int64{2, 5}
		names = []string{"x", "y"}
		bw    = int64(arrow.PrimitiveTypes.Date64.(arrow.FixedWidthDataType).BitWidth()) / 8
	)

	tsr := tensor.New(arr.Data(), shape, nil, names).(*tensor.Date64)
	defer tsr.Release()

	tsr.Retain()
	tsr.Release()

	if got, want := tsr.Len(), 10; got != want {
		t.Fatalf("invalid length: got=%d, want=%d", got, want)
	}

	if got, want := tsr.Shape(), shape; !reflect.DeepEqual(got, want) {
		t.Fatalf("invalid shape: got=%v, want=%v", got, want)
	}

	if got, want := tsr.Strides(), []int64{5 * bw, 1 * bw}; !reflect.DeepEqual(got, want) {
		t.Fatalf("invalid strides: got=%v, want=%v", got, want)
	}

	if got, want := tsr.NumDims(), 2; got != want {
		t.Fatalf("invalid dims: got=%d, want=%d", got, want)
	}

	for i, name := range names {
		if got, want := tsr.DimName(i), name; got != want {
			t.Fatalf("invalid dim-name[%d]: got=%q, want=%q", i, got, want)
		}
	}

	if got, want := tsr.DataType(), arr.DataType(); got != want {
		t.Fatalf("invalid data-type: got=%q, want=%q", got.Name(), want.Name())
	}

	if got, want := tsr.Data(), arr.Data(); got != want {
		t.Fatalf("invalid data: got=%v, want=%v", got, want)
	}

	if tsr.IsMutable() {
		t.Fatalf("should not be mutable")
	}

	if !tsr.IsContiguous() {
		t.Fatalf("should be contiguous")
	}

	if !tsr.IsRowMajor() || tsr.IsColMajor() {
		t.Fatalf("should be row-major")
	}

	if got, want := tsr.Date64Values(), raw; !reflect.DeepEqual(got, want) {
		t.Fatalf("invalid backing array: got=%v, want=%v", got, want)
	}

	for _, tc := range []struct {
		i []int64
		v arrow.Date64
	}{
		{i: []int64{0, 0}, v: 1},
		{i: []int64{0, 1}, v: 2},
		{i: []int64{0, 2}, v: 3},
		{i: []int64{0, 3}, v: 4},
		{i: []int64{0, 4}, v: 5},
		{i: []int64{1, 0}, v: 6},
		{i: []int64{1, 1}, v: 7},
		{i: []int64{1, 2}, v: 8},
		{i: []int64{1, 3}, v: 9},
		{i: []int64{1, 4}, v: 10},
	} {
		t.Run(fmt.Sprintf("%v", tc.i), func(t *testing.T) {
			got := tsr.Value(tc.i)
			if got != tc.v {
				t.Fatalf("arr[%v]: got=%v, want=%v", tc.i, got, tc.v)
			}
		})
	}
}
