// Code generated by array/numeric.gen.go.tmpl. DO NOT EDIT.

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
	"fmt"
	"strconv"
	"strings"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/internal/json"
)

// A type which represents an immutable sequence of int64 values.
type Int64 struct {
	array
	values []int64
}

// NewInt64Data creates a new Int64.
func NewInt64Data(data arrow.ArrayData) *Int64 {
	a := &Int64{}
	a.refCount = 1
	a.setData(data.(*Data))
	return a
}

// Reset resets the array for re-use.
func (a *Int64) Reset(data *Data) {
	a.setData(data)
}

// Value returns the value at the specified index.
func (a *Int64) Value(i int) int64 { return a.values[i] }

// Values returns the values.
func (a *Int64) Int64Values() []int64 { return a.values }

// String returns a string representation of the array.
func (a *Int64) String() string {
	o := new(strings.Builder)
	o.WriteString("[")
	for i, v := range a.values {
		if i > 0 {
			fmt.Fprintf(o, " ")
		}
		switch {
		case a.IsNull(i):
			o.WriteString(NullValueStr)
		default:
			fmt.Fprintf(o, "%v", v)
		}
	}
	o.WriteString("]")
	return o.String()
}

func (a *Int64) setData(data *Data) {
	a.array.setData(data)
	vals := data.buffers[1]
	if vals != nil {
		a.values = arrow.Int64Traits.CastFromBytes(vals.Bytes())
		beg := a.array.data.offset
		end := beg + a.array.data.length
		a.values = a.values[beg:end]
	}
}

func (a *Int64) ValueStr(i int) string {
	if a.IsNull(i) {
		return NullValueStr
	}
	return strconv.FormatInt(int64(a.Value(i)), 10)
}

func (a *Int64) GetOneForMarshal(i int) interface{} {
	if a.IsNull(i) {
		return nil
	}

	return a.values[i]
}

func (a *Int64) MarshalJSON() ([]byte, error) {
	vals := make([]interface{}, a.Len())
	for i := 0; i < a.Len(); i++ {
		if a.IsValid(i) {
			vals[i] = float64(a.values[i]) // prevent uint8 from being seen as binary data
		} else {
			vals[i] = nil
		}
	}

	return json.Marshal(vals)
}

func arrayEqualInt64(left, right *Int64) bool {
	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) {
			continue
		}
		if left.Value(i) != right.Value(i) {
			return false
		}
	}
	return true
}

// A type which represents an immutable sequence of uint64 values.
type Uint64 struct {
	array
	values []uint64
}

// NewUint64Data creates a new Uint64.
func NewUint64Data(data arrow.ArrayData) *Uint64 {
	a := &Uint64{}
	a.refCount = 1
	a.setData(data.(*Data))
	return a
}

// Reset resets the array for re-use.
func (a *Uint64) Reset(data *Data) {
	a.setData(data)
}

// Value returns the value at the specified index.
func (a *Uint64) Value(i int) uint64 { return a.values[i] }

// Values returns the values.
func (a *Uint64) Uint64Values() []uint64 { return a.values }

// String returns a string representation of the array.
func (a *Uint64) String() string {
	o := new(strings.Builder)
	o.WriteString("[")
	for i, v := range a.values {
		if i > 0 {
			fmt.Fprintf(o, " ")
		}
		switch {
		case a.IsNull(i):
			o.WriteString(NullValueStr)
		default:
			fmt.Fprintf(o, "%v", v)
		}
	}
	o.WriteString("]")
	return o.String()
}

func (a *Uint64) setData(data *Data) {
	a.array.setData(data)
	vals := data.buffers[1]
	if vals != nil {
		a.values = arrow.Uint64Traits.CastFromBytes(vals.Bytes())
		beg := a.array.data.offset
		end := beg + a.array.data.length
		a.values = a.values[beg:end]
	}
}

func (a *Uint64) ValueStr(i int) string {
	if a.IsNull(i) {
		return NullValueStr
	}
	return strconv.FormatUint(uint64(a.Value(i)), 10)
}

func (a *Uint64) GetOneForMarshal(i int) interface{} {
	if a.IsNull(i) {
		return nil
	}

	return a.values[i]
}

func (a *Uint64) MarshalJSON() ([]byte, error) {
	vals := make([]interface{}, a.Len())
	for i := 0; i < a.Len(); i++ {
		if a.IsValid(i) {
			vals[i] = float64(a.values[i]) // prevent uint8 from being seen as binary data
		} else {
			vals[i] = nil
		}
	}

	return json.Marshal(vals)
}

func arrayEqualUint64(left, right *Uint64) bool {
	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) {
			continue
		}
		if left.Value(i) != right.Value(i) {
			return false
		}
	}
	return true
}

// A type which represents an immutable sequence of float64 values.
type Float64 struct {
	array
	values []float64
}

// NewFloat64Data creates a new Float64.
func NewFloat64Data(data arrow.ArrayData) *Float64 {
	a := &Float64{}
	a.refCount = 1
	a.setData(data.(*Data))
	return a
}

// Reset resets the array for re-use.
func (a *Float64) Reset(data *Data) {
	a.setData(data)
}

// Value returns the value at the specified index.
func (a *Float64) Value(i int) float64 { return a.values[i] }

// Values returns the values.
func (a *Float64) Float64Values() []float64 { return a.values }

// String returns a string representation of the array.
func (a *Float64) String() string {
	o := new(strings.Builder)
	o.WriteString("[")
	for i, v := range a.values {
		if i > 0 {
			fmt.Fprintf(o, " ")
		}
		switch {
		case a.IsNull(i):
			o.WriteString(NullValueStr)
		default:
			fmt.Fprintf(o, "%v", v)
		}
	}
	o.WriteString("]")
	return o.String()
}

func (a *Float64) setData(data *Data) {
	a.array.setData(data)
	vals := data.buffers[1]
	if vals != nil {
		a.values = arrow.Float64Traits.CastFromBytes(vals.Bytes())
		beg := a.array.data.offset
		end := beg + a.array.data.length
		a.values = a.values[beg:end]
	}
}

func (a *Float64) ValueStr(i int) string {
	if a.IsNull(i) {
		return NullValueStr
	}
	return strconv.FormatFloat(float64(a.Value(i)), 'g', -1, 64)
}

func (a *Float64) GetOneForMarshal(i int) interface{} {
	if a.IsNull(i) {
		return nil
	}

	return a.values[i]
}

func (a *Float64) MarshalJSON() ([]byte, error) {
	vals := make([]interface{}, a.Len())
	for i := 0; i < a.Len(); i++ {
		if a.IsValid(i) {
			vals[i] = float64(a.values[i]) // prevent uint8 from being seen as binary data
		} else {
			vals[i] = nil
		}
	}

	return json.Marshal(vals)
}

func arrayEqualFloat64(left, right *Float64) bool {
	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) {
			continue
		}
		if left.Value(i) != right.Value(i) {
			return false
		}
	}
	return true
}

// A type which represents an immutable sequence of int32 values.
type Int32 struct {
	array
	values []int32
}

// NewInt32Data creates a new Int32.
func NewInt32Data(data arrow.ArrayData) *Int32 {
	a := &Int32{}
	a.refCount = 1
	a.setData(data.(*Data))
	return a
}

// Reset resets the array for re-use.
func (a *Int32) Reset(data *Data) {
	a.setData(data)
}

// Value returns the value at the specified index.
func (a *Int32) Value(i int) int32 { return a.values[i] }

// Values returns the values.
func (a *Int32) Int32Values() []int32 { return a.values }

// String returns a string representation of the array.
func (a *Int32) String() string {
	o := new(strings.Builder)
	o.WriteString("[")
	for i, v := range a.values {
		if i > 0 {
			fmt.Fprintf(o, " ")
		}
		switch {
		case a.IsNull(i):
			o.WriteString(NullValueStr)
		default:
			fmt.Fprintf(o, "%v", v)
		}
	}
	o.WriteString("]")
	return o.String()
}

func (a *Int32) setData(data *Data) {
	a.array.setData(data)
	vals := data.buffers[1]
	if vals != nil {
		a.values = arrow.Int32Traits.CastFromBytes(vals.Bytes())
		beg := a.array.data.offset
		end := beg + a.array.data.length
		a.values = a.values[beg:end]
	}
}

func (a *Int32) ValueStr(i int) string {
	if a.IsNull(i) {
		return NullValueStr
	}
	return strconv.FormatInt(int64(a.Value(i)), 10)
}

func (a *Int32) GetOneForMarshal(i int) interface{} {
	if a.IsNull(i) {
		return nil
	}

	return a.values[i]
}

func (a *Int32) MarshalJSON() ([]byte, error) {
	vals := make([]interface{}, a.Len())
	for i := 0; i < a.Len(); i++ {
		if a.IsValid(i) {
			vals[i] = float64(a.values[i]) // prevent uint8 from being seen as binary data
		} else {
			vals[i] = nil
		}
	}

	return json.Marshal(vals)
}

func arrayEqualInt32(left, right *Int32) bool {
	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) {
			continue
		}
		if left.Value(i) != right.Value(i) {
			return false
		}
	}
	return true
}

// A type which represents an immutable sequence of uint32 values.
type Uint32 struct {
	array
	values []uint32
}

// NewUint32Data creates a new Uint32.
func NewUint32Data(data arrow.ArrayData) *Uint32 {
	a := &Uint32{}
	a.refCount = 1
	a.setData(data.(*Data))
	return a
}

// Reset resets the array for re-use.
func (a *Uint32) Reset(data *Data) {
	a.setData(data)
}

// Value returns the value at the specified index.
func (a *Uint32) Value(i int) uint32 { return a.values[i] }

// Values returns the values.
func (a *Uint32) Uint32Values() []uint32 { return a.values }

// String returns a string representation of the array.
func (a *Uint32) String() string {
	o := new(strings.Builder)
	o.WriteString("[")
	for i, v := range a.values {
		if i > 0 {
			fmt.Fprintf(o, " ")
		}
		switch {
		case a.IsNull(i):
			o.WriteString(NullValueStr)
		default:
			fmt.Fprintf(o, "%v", v)
		}
	}
	o.WriteString("]")
	return o.String()
}

func (a *Uint32) setData(data *Data) {
	a.array.setData(data)
	vals := data.buffers[1]
	if vals != nil {
		a.values = arrow.Uint32Traits.CastFromBytes(vals.Bytes())
		beg := a.array.data.offset
		end := beg + a.array.data.length
		a.values = a.values[beg:end]
	}
}

func (a *Uint32) ValueStr(i int) string {
	if a.IsNull(i) {
		return NullValueStr
	}
	return strconv.FormatUint(uint64(a.Value(i)), 10)
}

func (a *Uint32) GetOneForMarshal(i int) interface{} {
	if a.IsNull(i) {
		return nil
	}

	return a.values[i]
}

func (a *Uint32) MarshalJSON() ([]byte, error) {
	vals := make([]interface{}, a.Len())
	for i := 0; i < a.Len(); i++ {
		if a.IsValid(i) {
			vals[i] = float64(a.values[i]) // prevent uint8 from being seen as binary data
		} else {
			vals[i] = nil
		}
	}

	return json.Marshal(vals)
}

func arrayEqualUint32(left, right *Uint32) bool {
	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) {
			continue
		}
		if left.Value(i) != right.Value(i) {
			return false
		}
	}
	return true
}

// A type which represents an immutable sequence of float32 values.
type Float32 struct {
	array
	values []float32
}

// NewFloat32Data creates a new Float32.
func NewFloat32Data(data arrow.ArrayData) *Float32 {
	a := &Float32{}
	a.refCount = 1
	a.setData(data.(*Data))
	return a
}

// Reset resets the array for re-use.
func (a *Float32) Reset(data *Data) {
	a.setData(data)
}

// Value returns the value at the specified index.
func (a *Float32) Value(i int) float32 { return a.values[i] }

// Values returns the values.
func (a *Float32) Float32Values() []float32 { return a.values }

// String returns a string representation of the array.
func (a *Float32) String() string {
	o := new(strings.Builder)
	o.WriteString("[")
	for i, v := range a.values {
		if i > 0 {
			fmt.Fprintf(o, " ")
		}
		switch {
		case a.IsNull(i):
			o.WriteString(NullValueStr)
		default:
			fmt.Fprintf(o, "%v", v)
		}
	}
	o.WriteString("]")
	return o.String()
}

func (a *Float32) setData(data *Data) {
	a.array.setData(data)
	vals := data.buffers[1]
	if vals != nil {
		a.values = arrow.Float32Traits.CastFromBytes(vals.Bytes())
		beg := a.array.data.offset
		end := beg + a.array.data.length
		a.values = a.values[beg:end]
	}
}

func (a *Float32) ValueStr(i int) string {
	if a.IsNull(i) {
		return NullValueStr
	}
	return strconv.FormatFloat(float64(a.Value(i)), 'g', -1, 32)
}

func (a *Float32) GetOneForMarshal(i int) interface{} {
	if a.IsNull(i) {
		return nil
	}

	return a.values[i]
}

func (a *Float32) MarshalJSON() ([]byte, error) {
	vals := make([]interface{}, a.Len())
	for i := 0; i < a.Len(); i++ {
		if a.IsValid(i) {
			vals[i] = float64(a.values[i]) // prevent uint8 from being seen as binary data
		} else {
			vals[i] = nil
		}
	}

	return json.Marshal(vals)
}

func arrayEqualFloat32(left, right *Float32) bool {
	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) {
			continue
		}
		if left.Value(i) != right.Value(i) {
			return false
		}
	}
	return true
}

// A type which represents an immutable sequence of int16 values.
type Int16 struct {
	array
	values []int16
}

// NewInt16Data creates a new Int16.
func NewInt16Data(data arrow.ArrayData) *Int16 {
	a := &Int16{}
	a.refCount = 1
	a.setData(data.(*Data))
	return a
}

// Reset resets the array for re-use.
func (a *Int16) Reset(data *Data) {
	a.setData(data)
}

// Value returns the value at the specified index.
func (a *Int16) Value(i int) int16 { return a.values[i] }

// Values returns the values.
func (a *Int16) Int16Values() []int16 { return a.values }

// String returns a string representation of the array.
func (a *Int16) String() string {
	o := new(strings.Builder)
	o.WriteString("[")
	for i, v := range a.values {
		if i > 0 {
			fmt.Fprintf(o, " ")
		}
		switch {
		case a.IsNull(i):
			o.WriteString(NullValueStr)
		default:
			fmt.Fprintf(o, "%v", v)
		}
	}
	o.WriteString("]")
	return o.String()
}

func (a *Int16) setData(data *Data) {
	a.array.setData(data)
	vals := data.buffers[1]
	if vals != nil {
		a.values = arrow.Int16Traits.CastFromBytes(vals.Bytes())
		beg := a.array.data.offset
		end := beg + a.array.data.length
		a.values = a.values[beg:end]
	}
}

func (a *Int16) ValueStr(i int) string {
	if a.IsNull(i) {
		return NullValueStr
	}
	return strconv.FormatInt(int64(a.Value(i)), 10)
}

func (a *Int16) GetOneForMarshal(i int) interface{} {
	if a.IsNull(i) {
		return nil
	}

	return a.values[i]
}

func (a *Int16) MarshalJSON() ([]byte, error) {
	vals := make([]interface{}, a.Len())
	for i := 0; i < a.Len(); i++ {
		if a.IsValid(i) {
			vals[i] = float64(a.values[i]) // prevent uint8 from being seen as binary data
		} else {
			vals[i] = nil
		}
	}

	return json.Marshal(vals)
}

func arrayEqualInt16(left, right *Int16) bool {
	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) {
			continue
		}
		if left.Value(i) != right.Value(i) {
			return false
		}
	}
	return true
}

// A type which represents an immutable sequence of uint16 values.
type Uint16 struct {
	array
	values []uint16
}

// NewUint16Data creates a new Uint16.
func NewUint16Data(data arrow.ArrayData) *Uint16 {
	a := &Uint16{}
	a.refCount = 1
	a.setData(data.(*Data))
	return a
}

// Reset resets the array for re-use.
func (a *Uint16) Reset(data *Data) {
	a.setData(data)
}

// Value returns the value at the specified index.
func (a *Uint16) Value(i int) uint16 { return a.values[i] }

// Values returns the values.
func (a *Uint16) Uint16Values() []uint16 { return a.values }

// String returns a string representation of the array.
func (a *Uint16) String() string {
	o := new(strings.Builder)
	o.WriteString("[")
	for i, v := range a.values {
		if i > 0 {
			fmt.Fprintf(o, " ")
		}
		switch {
		case a.IsNull(i):
			o.WriteString(NullValueStr)
		default:
			fmt.Fprintf(o, "%v", v)
		}
	}
	o.WriteString("]")
	return o.String()
}

func (a *Uint16) setData(data *Data) {
	a.array.setData(data)
	vals := data.buffers[1]
	if vals != nil {
		a.values = arrow.Uint16Traits.CastFromBytes(vals.Bytes())
		beg := a.array.data.offset
		end := beg + a.array.data.length
		a.values = a.values[beg:end]
	}
}

func (a *Uint16) ValueStr(i int) string {
	if a.IsNull(i) {
		return NullValueStr
	}
	return strconv.FormatUint(uint64(a.Value(i)), 10)
}

func (a *Uint16) GetOneForMarshal(i int) interface{} {
	if a.IsNull(i) {
		return nil
	}

	return a.values[i]
}

func (a *Uint16) MarshalJSON() ([]byte, error) {
	vals := make([]interface{}, a.Len())
	for i := 0; i < a.Len(); i++ {
		if a.IsValid(i) {
			vals[i] = float64(a.values[i]) // prevent uint8 from being seen as binary data
		} else {
			vals[i] = nil
		}
	}

	return json.Marshal(vals)
}

func arrayEqualUint16(left, right *Uint16) bool {
	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) {
			continue
		}
		if left.Value(i) != right.Value(i) {
			return false
		}
	}
	return true
}

// A type which represents an immutable sequence of int8 values.
type Int8 struct {
	array
	values []int8
}

// NewInt8Data creates a new Int8.
func NewInt8Data(data arrow.ArrayData) *Int8 {
	a := &Int8{}
	a.refCount = 1
	a.setData(data.(*Data))
	return a
}

// Reset resets the array for re-use.
func (a *Int8) Reset(data *Data) {
	a.setData(data)
}

// Value returns the value at the specified index.
func (a *Int8) Value(i int) int8 { return a.values[i] }

// Values returns the values.
func (a *Int8) Int8Values() []int8 { return a.values }

// String returns a string representation of the array.
func (a *Int8) String() string {
	o := new(strings.Builder)
	o.WriteString("[")
	for i, v := range a.values {
		if i > 0 {
			fmt.Fprintf(o, " ")
		}
		switch {
		case a.IsNull(i):
			o.WriteString(NullValueStr)
		default:
			fmt.Fprintf(o, "%v", v)
		}
	}
	o.WriteString("]")
	return o.String()
}

func (a *Int8) setData(data *Data) {
	a.array.setData(data)
	vals := data.buffers[1]
	if vals != nil {
		a.values = arrow.Int8Traits.CastFromBytes(vals.Bytes())
		beg := a.array.data.offset
		end := beg + a.array.data.length
		a.values = a.values[beg:end]
	}
}

func (a *Int8) ValueStr(i int) string {
	if a.IsNull(i) {
		return NullValueStr
	}
	return strconv.FormatInt(int64(a.Value(i)), 10)
}

func (a *Int8) GetOneForMarshal(i int) interface{} {
	if a.IsNull(i) {
		return nil
	}

	return float64(a.values[i]) // prevent uint8 from being seen as binary data
}

func (a *Int8) MarshalJSON() ([]byte, error) {
	vals := make([]interface{}, a.Len())
	for i := 0; i < a.Len(); i++ {
		if a.IsValid(i) {
			vals[i] = float64(a.values[i]) // prevent uint8 from being seen as binary data
		} else {
			vals[i] = nil
		}
	}

	return json.Marshal(vals)
}

func arrayEqualInt8(left, right *Int8) bool {
	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) {
			continue
		}
		if left.Value(i) != right.Value(i) {
			return false
		}
	}
	return true
}

// A type which represents an immutable sequence of uint8 values.
type Uint8 struct {
	array
	values []uint8
}

// NewUint8Data creates a new Uint8.
func NewUint8Data(data arrow.ArrayData) *Uint8 {
	a := &Uint8{}
	a.refCount = 1
	a.setData(data.(*Data))
	return a
}

// Reset resets the array for re-use.
func (a *Uint8) Reset(data *Data) {
	a.setData(data)
}

// Value returns the value at the specified index.
func (a *Uint8) Value(i int) uint8 { return a.values[i] }

// Values returns the values.
func (a *Uint8) Uint8Values() []uint8 { return a.values }

// String returns a string representation of the array.
func (a *Uint8) String() string {
	o := new(strings.Builder)
	o.WriteString("[")
	for i, v := range a.values {
		if i > 0 {
			fmt.Fprintf(o, " ")
		}
		switch {
		case a.IsNull(i):
			o.WriteString(NullValueStr)
		default:
			fmt.Fprintf(o, "%v", v)
		}
	}
	o.WriteString("]")
	return o.String()
}

func (a *Uint8) setData(data *Data) {
	a.array.setData(data)
	vals := data.buffers[1]
	if vals != nil {
		a.values = arrow.Uint8Traits.CastFromBytes(vals.Bytes())
		beg := a.array.data.offset
		end := beg + a.array.data.length
		a.values = a.values[beg:end]
	}
}

func (a *Uint8) ValueStr(i int) string {
	if a.IsNull(i) {
		return NullValueStr
	}
	return strconv.FormatUint(uint64(a.Value(i)), 10)
}

func (a *Uint8) GetOneForMarshal(i int) interface{} {
	if a.IsNull(i) {
		return nil
	}

	return float64(a.values[i]) // prevent uint8 from being seen as binary data
}

func (a *Uint8) MarshalJSON() ([]byte, error) {
	vals := make([]interface{}, a.Len())
	for i := 0; i < a.Len(); i++ {
		if a.IsValid(i) {
			vals[i] = float64(a.values[i]) // prevent uint8 from being seen as binary data
		} else {
			vals[i] = nil
		}
	}

	return json.Marshal(vals)
}

func arrayEqualUint8(left, right *Uint8) bool {
	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) {
			continue
		}
		if left.Value(i) != right.Value(i) {
			return false
		}
	}
	return true
}

// A type which represents an immutable sequence of arrow.Time32 values.
type Time32 struct {
	array
	values []arrow.Time32
}

// NewTime32Data creates a new Time32.
func NewTime32Data(data arrow.ArrayData) *Time32 {
	a := &Time32{}
	a.refCount = 1
	a.setData(data.(*Data))
	return a
}

// Reset resets the array for re-use.
func (a *Time32) Reset(data *Data) {
	a.setData(data)
}

// Value returns the value at the specified index.
func (a *Time32) Value(i int) arrow.Time32 { return a.values[i] }

// Values returns the values.
func (a *Time32) Time32Values() []arrow.Time32 { return a.values }

// String returns a string representation of the array.
func (a *Time32) String() string {
	o := new(strings.Builder)
	o.WriteString("[")
	for i, v := range a.values {
		if i > 0 {
			fmt.Fprintf(o, " ")
		}
		switch {
		case a.IsNull(i):
			o.WriteString(NullValueStr)
		default:
			fmt.Fprintf(o, "%v", v)
		}
	}
	o.WriteString("]")
	return o.String()
}

func (a *Time32) setData(data *Data) {
	a.array.setData(data)
	vals := data.buffers[1]
	if vals != nil {
		a.values = arrow.Time32Traits.CastFromBytes(vals.Bytes())
		beg := a.array.data.offset
		end := beg + a.array.data.length
		a.values = a.values[beg:end]
	}
}

func (a *Time32) ValueStr(i int) string {
	if a.IsNull(i) {
		return NullValueStr
	}
	return a.values[i].FormattedString(a.DataType().(*arrow.Time32Type).Unit)
}

func (a *Time32) GetOneForMarshal(i int) interface{} {
	if a.IsNull(i) {
		return nil
	}
	return a.values[i].ToTime(a.DataType().(*arrow.Time32Type).Unit).Format("15:04:05.999999999")
}

func (a *Time32) MarshalJSON() ([]byte, error) {
	vals := make([]interface{}, a.Len())
	for i := range a.values {
		vals[i] = a.GetOneForMarshal(i)
	}

	return json.Marshal(vals)
}

func arrayEqualTime32(left, right *Time32) bool {
	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) {
			continue
		}
		if left.Value(i) != right.Value(i) {
			return false
		}
	}
	return true
}

// A type which represents an immutable sequence of arrow.Time64 values.
type Time64 struct {
	array
	values []arrow.Time64
}

// NewTime64Data creates a new Time64.
func NewTime64Data(data arrow.ArrayData) *Time64 {
	a := &Time64{}
	a.refCount = 1
	a.setData(data.(*Data))
	return a
}

// Reset resets the array for re-use.
func (a *Time64) Reset(data *Data) {
	a.setData(data)
}

// Value returns the value at the specified index.
func (a *Time64) Value(i int) arrow.Time64 { return a.values[i] }

// Values returns the values.
func (a *Time64) Time64Values() []arrow.Time64 { return a.values }

// String returns a string representation of the array.
func (a *Time64) String() string {
	o := new(strings.Builder)
	o.WriteString("[")
	for i, v := range a.values {
		if i > 0 {
			fmt.Fprintf(o, " ")
		}
		switch {
		case a.IsNull(i):
			o.WriteString(NullValueStr)
		default:
			fmt.Fprintf(o, "%v", v)
		}
	}
	o.WriteString("]")
	return o.String()
}

func (a *Time64) setData(data *Data) {
	a.array.setData(data)
	vals := data.buffers[1]
	if vals != nil {
		a.values = arrow.Time64Traits.CastFromBytes(vals.Bytes())
		beg := a.array.data.offset
		end := beg + a.array.data.length
		a.values = a.values[beg:end]
	}
}

func (a *Time64) ValueStr(i int) string {
	if a.IsNull(i) {
		return NullValueStr
	}
	return a.values[i].FormattedString(a.DataType().(*arrow.Time64Type).Unit)
}

func (a *Time64) GetOneForMarshal(i int) interface{} {
	if a.IsNull(i) {
		return nil
	}
	return a.values[i].ToTime(a.DataType().(*arrow.Time64Type).Unit).Format("15:04:05.999999999")
}

func (a *Time64) MarshalJSON() ([]byte, error) {
	vals := make([]interface{}, a.Len())
	for i := range a.values {
		vals[i] = a.GetOneForMarshal(i)
	}

	return json.Marshal(vals)
}

func arrayEqualTime64(left, right *Time64) bool {
	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) {
			continue
		}
		if left.Value(i) != right.Value(i) {
			return false
		}
	}
	return true
}

// A type which represents an immutable sequence of arrow.Date32 values.
type Date32 struct {
	array
	values []arrow.Date32
}

// NewDate32Data creates a new Date32.
func NewDate32Data(data arrow.ArrayData) *Date32 {
	a := &Date32{}
	a.refCount = 1
	a.setData(data.(*Data))
	return a
}

// Reset resets the array for re-use.
func (a *Date32) Reset(data *Data) {
	a.setData(data)
}

// Value returns the value at the specified index.
func (a *Date32) Value(i int) arrow.Date32 { return a.values[i] }

// Values returns the values.
func (a *Date32) Date32Values() []arrow.Date32 { return a.values }

// String returns a string representation of the array.
func (a *Date32) String() string {
	o := new(strings.Builder)
	o.WriteString("[")
	for i, v := range a.values {
		if i > 0 {
			fmt.Fprintf(o, " ")
		}
		switch {
		case a.IsNull(i):
			o.WriteString(NullValueStr)
		default:
			fmt.Fprintf(o, "%v", v)
		}
	}
	o.WriteString("]")
	return o.String()
}

func (a *Date32) setData(data *Data) {
	a.array.setData(data)
	vals := data.buffers[1]
	if vals != nil {
		a.values = arrow.Date32Traits.CastFromBytes(vals.Bytes())
		beg := a.array.data.offset
		end := beg + a.array.data.length
		a.values = a.values[beg:end]
	}
}

func (a *Date32) ValueStr(i int) string {
	if a.IsNull(i) {
		return NullValueStr
	}
	return a.values[i].FormattedString()
}

func (a *Date32) GetOneForMarshal(i int) interface{} {
	if a.IsNull(i) {
		return nil
	}
	return a.values[i].ToTime().Format("2006-01-02")
}

func (a *Date32) MarshalJSON() ([]byte, error) {
	vals := make([]interface{}, a.Len())
	for i := range a.values {
		vals[i] = a.GetOneForMarshal(i)
	}

	return json.Marshal(vals)
}

func arrayEqualDate32(left, right *Date32) bool {
	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) {
			continue
		}
		if left.Value(i) != right.Value(i) {
			return false
		}
	}
	return true
}

// A type which represents an immutable sequence of arrow.Date64 values.
type Date64 struct {
	array
	values []arrow.Date64
}

// NewDate64Data creates a new Date64.
func NewDate64Data(data arrow.ArrayData) *Date64 {
	a := &Date64{}
	a.refCount = 1
	a.setData(data.(*Data))
	return a
}

// Reset resets the array for re-use.
func (a *Date64) Reset(data *Data) {
	a.setData(data)
}

// Value returns the value at the specified index.
func (a *Date64) Value(i int) arrow.Date64 { return a.values[i] }

// Values returns the values.
func (a *Date64) Date64Values() []arrow.Date64 { return a.values }

// String returns a string representation of the array.
func (a *Date64) String() string {
	o := new(strings.Builder)
	o.WriteString("[")
	for i, v := range a.values {
		if i > 0 {
			fmt.Fprintf(o, " ")
		}
		switch {
		case a.IsNull(i):
			o.WriteString(NullValueStr)
		default:
			fmt.Fprintf(o, "%v", v)
		}
	}
	o.WriteString("]")
	return o.String()
}

func (a *Date64) setData(data *Data) {
	a.array.setData(data)
	vals := data.buffers[1]
	if vals != nil {
		a.values = arrow.Date64Traits.CastFromBytes(vals.Bytes())
		beg := a.array.data.offset
		end := beg + a.array.data.length
		a.values = a.values[beg:end]
	}
}

func (a *Date64) ValueStr(i int) string {
	if a.IsNull(i) {
		return NullValueStr
	}
	return a.values[i].FormattedString()
}

func (a *Date64) GetOneForMarshal(i int) interface{} {
	if a.IsNull(i) {
		return nil
	}
	return a.values[i].ToTime().Format("2006-01-02")
}

func (a *Date64) MarshalJSON() ([]byte, error) {
	vals := make([]interface{}, a.Len())
	for i := range a.values {
		vals[i] = a.GetOneForMarshal(i)
	}

	return json.Marshal(vals)
}

func arrayEqualDate64(left, right *Date64) bool {
	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) {
			continue
		}
		if left.Value(i) != right.Value(i) {
			return false
		}
	}
	return true
}

// A type which represents an immutable sequence of arrow.Duration values.
type Duration struct {
	array
	values []arrow.Duration
}

// NewDurationData creates a new Duration.
func NewDurationData(data arrow.ArrayData) *Duration {
	a := &Duration{}
	a.refCount = 1
	a.setData(data.(*Data))
	return a
}

// Reset resets the array for re-use.
func (a *Duration) Reset(data *Data) {
	a.setData(data)
}

// Value returns the value at the specified index.
func (a *Duration) Value(i int) arrow.Duration { return a.values[i] }

// Values returns the values.
func (a *Duration) DurationValues() []arrow.Duration { return a.values }

// String returns a string representation of the array.
func (a *Duration) String() string {
	o := new(strings.Builder)
	o.WriteString("[")
	for i, v := range a.values {
		if i > 0 {
			fmt.Fprintf(o, " ")
		}
		switch {
		case a.IsNull(i):
			o.WriteString(NullValueStr)
		default:
			fmt.Fprintf(o, "%v", v)
		}
	}
	o.WriteString("]")
	return o.String()
}

func (a *Duration) setData(data *Data) {
	a.array.setData(data)
	vals := data.buffers[1]
	if vals != nil {
		a.values = arrow.DurationTraits.CastFromBytes(vals.Bytes())
		beg := a.array.data.offset
		end := beg + a.array.data.length
		a.values = a.values[beg:end]
	}
}

func (a *Duration) ValueStr(i int) string {
	if a.IsNull(i) {
		return NullValueStr
	}
	// return value and suffix as a string such as "12345ms"
	return fmt.Sprintf("%d%s", a.values[i], a.DataType().(*arrow.DurationType).Unit)
}

func (a *Duration) GetOneForMarshal(i int) interface{} {
	if a.IsNull(i) {
		return nil
	}
	// return value and suffix as a string such as "12345ms"
	return fmt.Sprintf("%d%s", a.values[i], a.DataType().(*arrow.DurationType).Unit.String())
}

func (a *Duration) MarshalJSON() ([]byte, error) {
	vals := make([]interface{}, a.Len())
	for i := range a.values {
		vals[i] = a.GetOneForMarshal(i)
	}

	return json.Marshal(vals)
}

func arrayEqualDuration(left, right *Duration) bool {
	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) {
			continue
		}
		if left.Value(i) != right.Value(i) {
			return false
		}
	}
	return true
}
