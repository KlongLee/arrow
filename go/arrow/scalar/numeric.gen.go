// Code generated by scalar/numeric.gen.go.tmpl. DO NOT EDIT.

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

package scalar

import (
	"fmt"
	"reflect"
	"unsafe"

	"github.com/apache/arrow/go/v6/arrow"
	"golang.org/x/xerrors"
)

type Int8 struct {
	scalar
	Value int8
}

func (s *Int8) Data() []byte {
	return (*[arrow.Int8SizeBytes]byte)(unsafe.Pointer(&s.Value))[:]
}

func (s *Int8) equals(rhs Scalar) bool {
	return s.Value == rhs.(*Int8).Value
}

func (s *Int8) value() interface{} {
	return s.Value
}

func (s *Int8) String() string {
	if !s.Valid {
		return "null"
	}
	val, err := s.CastTo(arrow.BinaryTypes.String)
	if err != nil {
		return "..."
	}
	return string(val.(*String).Value.Bytes())
}

func (s *Int8) CastTo(dt arrow.DataType) (Scalar, error) {
	if !s.Valid {
		return MakeNullScalar(dt), nil
	}

	r, ok := numericMap[dt.ID()]
	if ok {
		return convertToNumeric(reflect.ValueOf(s.Value), r.valueType, r.scalarFunc), nil
	}

	switch dt := dt.(type) {
	case *arrow.BooleanType:
		return NewBooleanScalar(s.Value != 0), nil
	case *arrow.Date32Type:
		return NewDate32Scalar(arrow.Date32(s.Value)), nil
	case *arrow.Date64Type:
		return NewDate64Scalar(arrow.Date64(s.Value)), nil
	case *arrow.Time32Type:
		return NewTime32Scalar(arrow.Time32(s.Value), dt), nil
	case *arrow.Time64Type:
		return NewTime64Scalar(arrow.Time64(s.Value), dt), nil
	case *arrow.TimestampType:
		return NewTimestampScalar(arrow.Timestamp(s.Value), dt), nil
	case *arrow.MonthIntervalType:
		return NewMonthIntervalScalar(arrow.MonthInterval(s.Value)), nil
	case *arrow.StringType:
		return NewStringScalar(fmt.Sprintf("%v", s.Value)), nil
	}

	return nil, xerrors.Errorf("invalid scalar cast from type int8 to type %s", dt)
}

func NewInt8Scalar(val int8) *Int8 {
	return &Int8{scalar{Type: arrow.PrimitiveTypes.Int8, Valid: true}, val}
}

type Int16 struct {
	scalar
	Value int16
}

func (s *Int16) Data() []byte {
	return (*[arrow.Int16SizeBytes]byte)(unsafe.Pointer(&s.Value))[:]
}

func (s *Int16) equals(rhs Scalar) bool {
	return s.Value == rhs.(*Int16).Value
}

func (s *Int16) value() interface{} {
	return s.Value
}

func (s *Int16) String() string {
	if !s.Valid {
		return "null"
	}
	val, err := s.CastTo(arrow.BinaryTypes.String)
	if err != nil {
		return "..."
	}
	return string(val.(*String).Value.Bytes())
}

func (s *Int16) CastTo(dt arrow.DataType) (Scalar, error) {
	if !s.Valid {
		return MakeNullScalar(dt), nil
	}

	r, ok := numericMap[dt.ID()]
	if ok {
		return convertToNumeric(reflect.ValueOf(s.Value), r.valueType, r.scalarFunc), nil
	}

	switch dt := dt.(type) {
	case *arrow.BooleanType:
		return NewBooleanScalar(s.Value != 0), nil
	case *arrow.Date32Type:
		return NewDate32Scalar(arrow.Date32(s.Value)), nil
	case *arrow.Date64Type:
		return NewDate64Scalar(arrow.Date64(s.Value)), nil
	case *arrow.Time32Type:
		return NewTime32Scalar(arrow.Time32(s.Value), dt), nil
	case *arrow.Time64Type:
		return NewTime64Scalar(arrow.Time64(s.Value), dt), nil
	case *arrow.TimestampType:
		return NewTimestampScalar(arrow.Timestamp(s.Value), dt), nil
	case *arrow.MonthIntervalType:
		return NewMonthIntervalScalar(arrow.MonthInterval(s.Value)), nil
	case *arrow.StringType:
		return NewStringScalar(fmt.Sprintf("%v", s.Value)), nil
	}

	return nil, xerrors.Errorf("invalid scalar cast from type int16 to type %s", dt)
}

func NewInt16Scalar(val int16) *Int16 {
	return &Int16{scalar{Type: arrow.PrimitiveTypes.Int16, Valid: true}, val}
}

type Int32 struct {
	scalar
	Value int32
}

func (s *Int32) Data() []byte {
	return (*[arrow.Int32SizeBytes]byte)(unsafe.Pointer(&s.Value))[:]
}

func (s *Int32) equals(rhs Scalar) bool {
	return s.Value == rhs.(*Int32).Value
}

func (s *Int32) value() interface{} {
	return s.Value
}

func (s *Int32) String() string {
	if !s.Valid {
		return "null"
	}
	val, err := s.CastTo(arrow.BinaryTypes.String)
	if err != nil {
		return "..."
	}
	return string(val.(*String).Value.Bytes())
}

func (s *Int32) CastTo(dt arrow.DataType) (Scalar, error) {
	if !s.Valid {
		return MakeNullScalar(dt), nil
	}

	r, ok := numericMap[dt.ID()]
	if ok {
		return convertToNumeric(reflect.ValueOf(s.Value), r.valueType, r.scalarFunc), nil
	}

	switch dt := dt.(type) {
	case *arrow.BooleanType:
		return NewBooleanScalar(s.Value != 0), nil
	case *arrow.Date32Type:
		return NewDate32Scalar(arrow.Date32(s.Value)), nil
	case *arrow.Date64Type:
		return NewDate64Scalar(arrow.Date64(s.Value)), nil
	case *arrow.Time32Type:
		return NewTime32Scalar(arrow.Time32(s.Value), dt), nil
	case *arrow.Time64Type:
		return NewTime64Scalar(arrow.Time64(s.Value), dt), nil
	case *arrow.TimestampType:
		return NewTimestampScalar(arrow.Timestamp(s.Value), dt), nil
	case *arrow.MonthIntervalType:
		return NewMonthIntervalScalar(arrow.MonthInterval(s.Value)), nil
	case *arrow.StringType:
		return NewStringScalar(fmt.Sprintf("%v", s.Value)), nil
	}

	return nil, xerrors.Errorf("invalid scalar cast from type int32 to type %s", dt)
}

func NewInt32Scalar(val int32) *Int32 {
	return &Int32{scalar{Type: arrow.PrimitiveTypes.Int32, Valid: true}, val}
}

type Int64 struct {
	scalar
	Value int64
}

func (s *Int64) Data() []byte {
	return (*[arrow.Int64SizeBytes]byte)(unsafe.Pointer(&s.Value))[:]
}

func (s *Int64) equals(rhs Scalar) bool {
	return s.Value == rhs.(*Int64).Value
}

func (s *Int64) value() interface{} {
	return s.Value
}

func (s *Int64) String() string {
	if !s.Valid {
		return "null"
	}
	val, err := s.CastTo(arrow.BinaryTypes.String)
	if err != nil {
		return "..."
	}
	return string(val.(*String).Value.Bytes())
}

func (s *Int64) CastTo(dt arrow.DataType) (Scalar, error) {
	if !s.Valid {
		return MakeNullScalar(dt), nil
	}

	r, ok := numericMap[dt.ID()]
	if ok {
		return convertToNumeric(reflect.ValueOf(s.Value), r.valueType, r.scalarFunc), nil
	}

	switch dt := dt.(type) {
	case *arrow.BooleanType:
		return NewBooleanScalar(s.Value != 0), nil
	case *arrow.Date32Type:
		return NewDate32Scalar(arrow.Date32(s.Value)), nil
	case *arrow.Date64Type:
		return NewDate64Scalar(arrow.Date64(s.Value)), nil
	case *arrow.Time32Type:
		return NewTime32Scalar(arrow.Time32(s.Value), dt), nil
	case *arrow.Time64Type:
		return NewTime64Scalar(arrow.Time64(s.Value), dt), nil
	case *arrow.TimestampType:
		return NewTimestampScalar(arrow.Timestamp(s.Value), dt), nil
	case *arrow.MonthIntervalType:
		return NewMonthIntervalScalar(arrow.MonthInterval(s.Value)), nil
	case *arrow.StringType:
		return NewStringScalar(fmt.Sprintf("%v", s.Value)), nil
	}

	return nil, xerrors.Errorf("invalid scalar cast from type int64 to type %s", dt)
}

func NewInt64Scalar(val int64) *Int64 {
	return &Int64{scalar{Type: arrow.PrimitiveTypes.Int64, Valid: true}, val}
}

type Uint8 struct {
	scalar
	Value uint8
}

func (s *Uint8) Data() []byte {
	return (*[arrow.Uint8SizeBytes]byte)(unsafe.Pointer(&s.Value))[:]
}

func (s *Uint8) equals(rhs Scalar) bool {
	return s.Value == rhs.(*Uint8).Value
}

func (s *Uint8) value() interface{} {
	return s.Value
}

func (s *Uint8) String() string {
	if !s.Valid {
		return "null"
	}
	val, err := s.CastTo(arrow.BinaryTypes.String)
	if err != nil {
		return "..."
	}
	return string(val.(*String).Value.Bytes())
}

func (s *Uint8) CastTo(dt arrow.DataType) (Scalar, error) {
	if !s.Valid {
		return MakeNullScalar(dt), nil
	}

	r, ok := numericMap[dt.ID()]
	if ok {
		return convertToNumeric(reflect.ValueOf(s.Value), r.valueType, r.scalarFunc), nil
	}

	switch dt := dt.(type) {
	case *arrow.BooleanType:
		return NewBooleanScalar(s.Value != 0), nil
	case *arrow.Date32Type:
		return NewDate32Scalar(arrow.Date32(s.Value)), nil
	case *arrow.Date64Type:
		return NewDate64Scalar(arrow.Date64(s.Value)), nil
	case *arrow.Time32Type:
		return NewTime32Scalar(arrow.Time32(s.Value), dt), nil
	case *arrow.Time64Type:
		return NewTime64Scalar(arrow.Time64(s.Value), dt), nil
	case *arrow.TimestampType:
		return NewTimestampScalar(arrow.Timestamp(s.Value), dt), nil
	case *arrow.MonthIntervalType:
		return NewMonthIntervalScalar(arrow.MonthInterval(s.Value)), nil
	case *arrow.StringType:
		return NewStringScalar(fmt.Sprintf("%v", s.Value)), nil
	}

	return nil, xerrors.Errorf("invalid scalar cast from type uint8 to type %s", dt)
}

func NewUint8Scalar(val uint8) *Uint8 {
	return &Uint8{scalar{Type: arrow.PrimitiveTypes.Uint8, Valid: true}, val}
}

type Uint16 struct {
	scalar
	Value uint16
}

func (s *Uint16) Data() []byte {
	return (*[arrow.Uint16SizeBytes]byte)(unsafe.Pointer(&s.Value))[:]
}

func (s *Uint16) equals(rhs Scalar) bool {
	return s.Value == rhs.(*Uint16).Value
}

func (s *Uint16) value() interface{} {
	return s.Value
}

func (s *Uint16) String() string {
	if !s.Valid {
		return "null"
	}
	val, err := s.CastTo(arrow.BinaryTypes.String)
	if err != nil {
		return "..."
	}
	return string(val.(*String).Value.Bytes())
}

func (s *Uint16) CastTo(dt arrow.DataType) (Scalar, error) {
	if !s.Valid {
		return MakeNullScalar(dt), nil
	}

	r, ok := numericMap[dt.ID()]
	if ok {
		return convertToNumeric(reflect.ValueOf(s.Value), r.valueType, r.scalarFunc), nil
	}

	switch dt := dt.(type) {
	case *arrow.BooleanType:
		return NewBooleanScalar(s.Value != 0), nil
	case *arrow.Date32Type:
		return NewDate32Scalar(arrow.Date32(s.Value)), nil
	case *arrow.Date64Type:
		return NewDate64Scalar(arrow.Date64(s.Value)), nil
	case *arrow.Time32Type:
		return NewTime32Scalar(arrow.Time32(s.Value), dt), nil
	case *arrow.Time64Type:
		return NewTime64Scalar(arrow.Time64(s.Value), dt), nil
	case *arrow.TimestampType:
		return NewTimestampScalar(arrow.Timestamp(s.Value), dt), nil
	case *arrow.MonthIntervalType:
		return NewMonthIntervalScalar(arrow.MonthInterval(s.Value)), nil
	case *arrow.StringType:
		return NewStringScalar(fmt.Sprintf("%v", s.Value)), nil
	}

	return nil, xerrors.Errorf("invalid scalar cast from type uint16 to type %s", dt)
}

func NewUint16Scalar(val uint16) *Uint16 {
	return &Uint16{scalar{Type: arrow.PrimitiveTypes.Uint16, Valid: true}, val}
}

type Uint32 struct {
	scalar
	Value uint32
}

func (s *Uint32) Data() []byte {
	return (*[arrow.Uint32SizeBytes]byte)(unsafe.Pointer(&s.Value))[:]
}

func (s *Uint32) equals(rhs Scalar) bool {
	return s.Value == rhs.(*Uint32).Value
}

func (s *Uint32) value() interface{} {
	return s.Value
}

func (s *Uint32) String() string {
	if !s.Valid {
		return "null"
	}
	val, err := s.CastTo(arrow.BinaryTypes.String)
	if err != nil {
		return "..."
	}
	return string(val.(*String).Value.Bytes())
}

func (s *Uint32) CastTo(dt arrow.DataType) (Scalar, error) {
	if !s.Valid {
		return MakeNullScalar(dt), nil
	}

	r, ok := numericMap[dt.ID()]
	if ok {
		return convertToNumeric(reflect.ValueOf(s.Value), r.valueType, r.scalarFunc), nil
	}

	switch dt := dt.(type) {
	case *arrow.BooleanType:
		return NewBooleanScalar(s.Value != 0), nil
	case *arrow.Date32Type:
		return NewDate32Scalar(arrow.Date32(s.Value)), nil
	case *arrow.Date64Type:
		return NewDate64Scalar(arrow.Date64(s.Value)), nil
	case *arrow.Time32Type:
		return NewTime32Scalar(arrow.Time32(s.Value), dt), nil
	case *arrow.Time64Type:
		return NewTime64Scalar(arrow.Time64(s.Value), dt), nil
	case *arrow.TimestampType:
		return NewTimestampScalar(arrow.Timestamp(s.Value), dt), nil
	case *arrow.MonthIntervalType:
		return NewMonthIntervalScalar(arrow.MonthInterval(s.Value)), nil
	case *arrow.StringType:
		return NewStringScalar(fmt.Sprintf("%v", s.Value)), nil
	}

	return nil, xerrors.Errorf("invalid scalar cast from type uint32 to type %s", dt)
}

func NewUint32Scalar(val uint32) *Uint32 {
	return &Uint32{scalar{Type: arrow.PrimitiveTypes.Uint32, Valid: true}, val}
}

type Uint64 struct {
	scalar
	Value uint64
}

func (s *Uint64) Data() []byte {
	return (*[arrow.Uint64SizeBytes]byte)(unsafe.Pointer(&s.Value))[:]
}

func (s *Uint64) equals(rhs Scalar) bool {
	return s.Value == rhs.(*Uint64).Value
}

func (s *Uint64) value() interface{} {
	return s.Value
}

func (s *Uint64) String() string {
	if !s.Valid {
		return "null"
	}
	val, err := s.CastTo(arrow.BinaryTypes.String)
	if err != nil {
		return "..."
	}
	return string(val.(*String).Value.Bytes())
}

func (s *Uint64) CastTo(dt arrow.DataType) (Scalar, error) {
	if !s.Valid {
		return MakeNullScalar(dt), nil
	}

	r, ok := numericMap[dt.ID()]
	if ok {
		return convertToNumeric(reflect.ValueOf(s.Value), r.valueType, r.scalarFunc), nil
	}

	switch dt := dt.(type) {
	case *arrow.BooleanType:
		return NewBooleanScalar(s.Value != 0), nil
	case *arrow.Date32Type:
		return NewDate32Scalar(arrow.Date32(s.Value)), nil
	case *arrow.Date64Type:
		return NewDate64Scalar(arrow.Date64(s.Value)), nil
	case *arrow.Time32Type:
		return NewTime32Scalar(arrow.Time32(s.Value), dt), nil
	case *arrow.Time64Type:
		return NewTime64Scalar(arrow.Time64(s.Value), dt), nil
	case *arrow.TimestampType:
		return NewTimestampScalar(arrow.Timestamp(s.Value), dt), nil
	case *arrow.MonthIntervalType:
		return NewMonthIntervalScalar(arrow.MonthInterval(s.Value)), nil
	case *arrow.StringType:
		return NewStringScalar(fmt.Sprintf("%v", s.Value)), nil
	}

	return nil, xerrors.Errorf("invalid scalar cast from type uint64 to type %s", dt)
}

func NewUint64Scalar(val uint64) *Uint64 {
	return &Uint64{scalar{Type: arrow.PrimitiveTypes.Uint64, Valid: true}, val}
}

type Float32 struct {
	scalar
	Value float32
}

func (s *Float32) Data() []byte {
	return (*[arrow.Float32SizeBytes]byte)(unsafe.Pointer(&s.Value))[:]
}

func (s *Float32) equals(rhs Scalar) bool {
	return s.Value == rhs.(*Float32).Value
}

func (s *Float32) value() interface{} {
	return s.Value
}

func (s *Float32) String() string {
	if !s.Valid {
		return "null"
	}
	val, err := s.CastTo(arrow.BinaryTypes.String)
	if err != nil {
		return "..."
	}
	return string(val.(*String).Value.Bytes())
}

func (s *Float32) CastTo(dt arrow.DataType) (Scalar, error) {
	if !s.Valid {
		return MakeNullScalar(dt), nil
	}

	r, ok := numericMap[dt.ID()]
	if ok {
		return convertToNumeric(reflect.ValueOf(s.Value), r.valueType, r.scalarFunc), nil
	}

	switch dt := dt.(type) {
	case *arrow.BooleanType:
		return NewBooleanScalar(s.Value != 0), nil
	case *arrow.Date32Type:
		return NewDate32Scalar(arrow.Date32(s.Value)), nil
	case *arrow.Date64Type:
		return NewDate64Scalar(arrow.Date64(s.Value)), nil
	case *arrow.Time32Type:
		return NewTime32Scalar(arrow.Time32(s.Value), dt), nil
	case *arrow.Time64Type:
		return NewTime64Scalar(arrow.Time64(s.Value), dt), nil
	case *arrow.TimestampType:
		return NewTimestampScalar(arrow.Timestamp(s.Value), dt), nil
	case *arrow.MonthIntervalType:
		return NewMonthIntervalScalar(arrow.MonthInterval(s.Value)), nil
	case *arrow.StringType:
		return NewStringScalar(fmt.Sprintf("%v", s.Value)), nil
	}

	return nil, xerrors.Errorf("invalid scalar cast from type float32 to type %s", dt)
}

func NewFloat32Scalar(val float32) *Float32 {
	return &Float32{scalar{Type: arrow.PrimitiveTypes.Float32, Valid: true}, val}
}

type Float64 struct {
	scalar
	Value float64
}

func (s *Float64) Data() []byte {
	return (*[arrow.Float64SizeBytes]byte)(unsafe.Pointer(&s.Value))[:]
}

func (s *Float64) equals(rhs Scalar) bool {
	return s.Value == rhs.(*Float64).Value
}

func (s *Float64) value() interface{} {
	return s.Value
}

func (s *Float64) String() string {
	if !s.Valid {
		return "null"
	}
	val, err := s.CastTo(arrow.BinaryTypes.String)
	if err != nil {
		return "..."
	}
	return string(val.(*String).Value.Bytes())
}

func (s *Float64) CastTo(dt arrow.DataType) (Scalar, error) {
	if !s.Valid {
		return MakeNullScalar(dt), nil
	}

	r, ok := numericMap[dt.ID()]
	if ok {
		return convertToNumeric(reflect.ValueOf(s.Value), r.valueType, r.scalarFunc), nil
	}

	switch dt := dt.(type) {
	case *arrow.BooleanType:
		return NewBooleanScalar(s.Value != 0), nil
	case *arrow.Date32Type:
		return NewDate32Scalar(arrow.Date32(s.Value)), nil
	case *arrow.Date64Type:
		return NewDate64Scalar(arrow.Date64(s.Value)), nil
	case *arrow.Time32Type:
		return NewTime32Scalar(arrow.Time32(s.Value), dt), nil
	case *arrow.Time64Type:
		return NewTime64Scalar(arrow.Time64(s.Value), dt), nil
	case *arrow.TimestampType:
		return NewTimestampScalar(arrow.Timestamp(s.Value), dt), nil
	case *arrow.MonthIntervalType:
		return NewMonthIntervalScalar(arrow.MonthInterval(s.Value)), nil
	case *arrow.StringType:
		return NewStringScalar(fmt.Sprintf("%v", s.Value)), nil
	}

	return nil, xerrors.Errorf("invalid scalar cast from type float64 to type %s", dt)
}

func NewFloat64Scalar(val float64) *Float64 {
	return &Float64{scalar{Type: arrow.PrimitiveTypes.Float64, Valid: true}, val}
}

var numericMap = map[arrow.Type]struct {
	scalarFunc reflect.Value
	valueType  reflect.Type
}{
	arrow.INT8:    {scalarFunc: reflect.ValueOf(NewInt8Scalar), valueType: reflect.TypeOf(int8(0))},
	arrow.INT16:   {scalarFunc: reflect.ValueOf(NewInt16Scalar), valueType: reflect.TypeOf(int16(0))},
	arrow.INT32:   {scalarFunc: reflect.ValueOf(NewInt32Scalar), valueType: reflect.TypeOf(int32(0))},
	arrow.INT64:   {scalarFunc: reflect.ValueOf(NewInt64Scalar), valueType: reflect.TypeOf(int64(0))},
	arrow.UINT8:   {scalarFunc: reflect.ValueOf(NewUint8Scalar), valueType: reflect.TypeOf(uint8(0))},
	arrow.UINT16:  {scalarFunc: reflect.ValueOf(NewUint16Scalar), valueType: reflect.TypeOf(uint16(0))},
	arrow.UINT32:  {scalarFunc: reflect.ValueOf(NewUint32Scalar), valueType: reflect.TypeOf(uint32(0))},
	arrow.UINT64:  {scalarFunc: reflect.ValueOf(NewUint64Scalar), valueType: reflect.TypeOf(uint64(0))},
	arrow.FLOAT32: {scalarFunc: reflect.ValueOf(NewFloat32Scalar), valueType: reflect.TypeOf(float32(0))},
	arrow.FLOAT64: {scalarFunc: reflect.ValueOf(NewFloat64Scalar), valueType: reflect.TypeOf(float64(0))},
}

var (
	_ Scalar = (*Int8)(nil)
	_ Scalar = (*Int16)(nil)
	_ Scalar = (*Int32)(nil)
	_ Scalar = (*Int64)(nil)
	_ Scalar = (*Uint8)(nil)
	_ Scalar = (*Uint16)(nil)
	_ Scalar = (*Uint32)(nil)
	_ Scalar = (*Uint64)(nil)
	_ Scalar = (*Float32)(nil)
	_ Scalar = (*Float64)(nil)
)
