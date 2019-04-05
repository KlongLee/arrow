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

package array_test

import (
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewFloat16Builder(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	ab := array.NewHalfFloatBuilder(mem)

	ab.Append(1)
	ab.Append(2)
	ab.Append(3)
	ab.AppendNull()
	ab.Append(5)
	ab.Append(6)
	ab.AppendNull()
	ab.Append(8)
	ab.Append(9)
	ab.Append(10)

	// check state of builder before NewFloat16Array
	assert.Equal(t, 10, ab.Len(), "unexpected Len()")
	assert.Equal(t, 2, ab.NullN(), "unexpected NullN()")

	a := ab.NewFloat16Array()

	// check state of builder after NewFloat16Array
	assert.Zero(t, ab.Len(), "unexpected ArrayBuilder.Len(), NewFloat16Array did not reset state")
	assert.Zero(t, ab.Cap(), "unexpected ArrayBuilder.Cap(), NewFloat16Array did not reset state")
	assert.Zero(t, ab.NullN(), "unexpected ArrayBuilder.NullN(), NewFloat16Array did not reset state")

	// check state of array

	assert.Equal(t, 2, a.NullN(), "unexpected null count")
	assert.Equal(t, []float32{1, 2, 3, 0, 5, 6, 0, 8, 9, 10}, a.Values(), "unexpected Float16Values")
	assert.Equal(t, []byte{0xb7}, a.NullBitmapBytes()[:1]) // 4 bytes due to minBuilderCapacity
	assert.Len(t, a.Values(), 10, "unexpected length of Float16Values")

	a.Release()
	ab.Append(7)
	ab.Append(8)

	a = ab.NewFloat16Array()

	assert.Equal(t, 0, a.NullN())
	assert.Equal(t, []float32{7, 8}, a.Values())
	assert.Len(t, a.Values(), 2)

	a.Release()
}
