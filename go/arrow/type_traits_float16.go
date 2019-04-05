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

package arrow

import (
	"encoding/binary"
	"math"
	"reflect"
	"unsafe"
)

// Float16 traits

type Float16 uint16

// https://en.wikipedia.org/wiki/Half-precision_floating-point_format
func NewFloat16(f float32) Float16 {
	b := math.Float32bits(f)
	sn := uint16((b >> 31) & 0x1)
	exp := (b >> 23) & 0xff
	res := int16(exp) - 127 + 15
	fc := uint16(b >> 13) & 0x3ff
	if exp == 0 {
		res = 0
	} else if exp == 0xff {
		res = 0x1f
	} else if res > 0x1e {
		res = 0x1f
		fc = 0
	} else if res < 0x01 {
		res = 0
		fc = 0
	}
	return Float16((sn << 15) | uint16(res << 10) | fc)
}

func (f Float16) Float32() float32 {
	sn := uint32((f >> 15) & 0x1)
	exp := (f >> 10) & 0x1f
	res := uint32(exp) + 127 - 15
	fc := uint32(f & 0x3ff)
	if exp == 0 {
		res = 0
	} else if exp == 0x1f {
		res = 0xff
	}
	return math.Float32frombits((sn << 31) | (res << 23) | (fc << 13))
}

var Float16Traits   float16Traits

const (
	// Float16SizeBytes specifies the number of bytes required to store a single float16 in memory
	Float16SizeBytes = int(unsafe.Sizeof(uint16(0)))
)

type float16Traits struct{}

// BytesRequired returns the number of bytes required to store n elements in memory.
func (float16Traits) BytesRequired(n int) int { return Float16SizeBytes * n }

// PutValue
func (float16Traits) PutValue(b []byte, v Float16) {
	binary.LittleEndian.PutUint16(b, uint16(v))
}

// CastFromBytes reinterprets the slice b to a slice of type uint16.
//
// NOTE: len(b) must be a multiple of Uint16SizeBytes.
func (float16Traits) CastFromBytes(b []byte) []Float16 {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var res []Float16
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len / Float16SizeBytes
	s.Cap = h.Cap / Float16SizeBytes

	return res
}

// CastToBytes reinterprets the slice b to a slice of bytes.
func (float16Traits) CastToBytes(b []Float16) []byte {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var res []byte
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len * Float16SizeBytes
	s.Cap = h.Cap * Float16SizeBytes

	return res
}

// Copy copies src to dst.
func (float16Traits) Copy(dst, src []Float16) { copy(dst, src) }

