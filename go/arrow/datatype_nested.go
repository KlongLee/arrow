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
	"fmt"
	"strings"
)

// ListType describes a nested type in which each array slot contains
// a variable-size sequence of values, all having the same relative type.
type ListType struct {
	elem         DataType // DataType of the list's elements
	Meta         Metadata
	NullableElem bool
}

// ListOf returns the list type with element type t.
// For example, if t represents int32, ListOf(t) represents []int32.
//
// ListOf panics if t is nil or invalid. NullableElem defaults to true
func ListOf(t DataType) *ListType {
	if t == nil {
		panic("arrow: nil DataType")
	}
	return &ListType{elem: t, NullableElem: true}
}

// ListOfNonNullable is like ListOf but NullableElem defaults to false, indicating
// that the child type should be marked as non-nullable.
func ListOfNonNullable(t DataType) *ListType {
	if t == nil {
		panic("arrow: nil DataType")
	}
	return &ListType{elem: t, NullableElem: false}
}

func (*ListType) ID() Type     { return LIST }
func (*ListType) Name() string { return "list" }
func (t *ListType) String() string {
	if t.NullableElem {
		return fmt.Sprintf("list<item: %v, nullable>", t.elem)
	}
	return fmt.Sprintf("list<item: %v>", t.elem)
}

// Elem returns the ListType's element type.
func (t *ListType) Elem() DataType { return t.elem }

func (t *ListType) ElemField() Field {
	return Field{
		Name:     "item",
		Type:     t.elem,
		Metadata: t.Meta,
		Nullable: t.NullableElem,
	}
}

// FixedSizeListType describes a nested type in which each array slot contains
// a fixed-size sequence of values, all having the same relative type.
type FixedSizeListType struct {
	n            int32    // number of elements in the list
	elem         DataType // DataType of the list's elements
	Meta         Metadata
	NullableElem bool
}

// FixedSizeListOf returns the list type with element type t.
// For example, if t represents int32, FixedSizeListOf(10, t) represents [10]int32.
//
// FixedSizeListOf panics if t is nil or invalid.
// FixedSizeListOf panics if n is <= 0.
// NullableElem defaults to true
func FixedSizeListOf(n int32, t DataType) *FixedSizeListType {
	if t == nil {
		panic("arrow: nil DataType")
	}
	if n <= 0 {
		panic("arrow: invalid size")
	}
	return &FixedSizeListType{elem: t, n: n, NullableElem: true}
}

// FixedSizeListOfNonNullable is like FixedSizeListOf but NullableElem defaults to false
// indicating that the child type should be marked as non-nullable.
func FixedSizeListOfNonNullable(n int32, t DataType) *FixedSizeListType {
	if t == nil {
		panic("arrow: nil DataType")
	}
	if n <= 0 {
		panic("arrow: invalid size")
	}
	return &FixedSizeListType{elem: t, n: n, NullableElem: false}
}

func (*FixedSizeListType) ID() Type     { return FIXED_SIZE_LIST }
func (*FixedSizeListType) Name() string { return "fixed_size_list" }
func (t *FixedSizeListType) String() string {
	if t.NullableElem {
		return fmt.Sprintf("fixed_size_list<item: %v, nullable>[%d]", t.elem, t.n)
	}
	return fmt.Sprintf("fixed_size_list<item: %v>[%d]", t.elem, t.n)
}

// Elem returns the FixedSizeListType's element type.
func (t *FixedSizeListType) Elem() DataType { return t.elem }

// Len returns the FixedSizeListType's size.
func (t *FixedSizeListType) Len() int32 { return t.n }

func (t *FixedSizeListType) ElemField() Field {
	return Field{
		Name:     "item",
		Type:     t.elem,
		Metadata: t.Meta,
		Nullable: t.NullableElem,
	}
}

// StructType describes a nested type parameterized by an ordered sequence
// of relative types, called its fields.
type StructType struct {
	fields []Field
	index  map[string]int
	meta   Metadata
}

// StructOf returns the struct type with fields fs.
//
// StructOf panics if there are duplicated fields.
// StructOf panics if there is a field with an invalid DataType.
func StructOf(fs ...Field) *StructType {
	n := len(fs)
	if n == 0 {
		return &StructType{}
	}

	t := &StructType{
		fields: make([]Field, n),
		index:  make(map[string]int, n),
	}
	for i, f := range fs {
		if f.Type == nil {
			panic("arrow: field with nil DataType")
		}
		t.fields[i] = Field{
			Name:     f.Name,
			Type:     f.Type,
			Nullable: f.Nullable,
			Metadata: f.Metadata.clone(),
		}
		if _, dup := t.index[f.Name]; dup {
			panic(fmt.Errorf("arrow: duplicate field with name %q", f.Name))
		}
		t.index[f.Name] = i
	}

	return t
}

func (*StructType) ID() Type     { return STRUCT }
func (*StructType) Name() string { return "struct" }

func (t *StructType) String() string {
	o := new(strings.Builder)
	o.WriteString("struct<")
	for i, f := range t.fields {
		if i > 0 {
			o.WriteString(", ")
		}
		o.WriteString(fmt.Sprintf("%s: %v", f.Name, f.Type))
	}
	o.WriteString(">")
	return o.String()
}

func (t *StructType) Fields() []Field   { return t.fields }
func (t *StructType) Field(i int) Field { return t.fields[i] }

func (t *StructType) FieldByName(name string) (Field, bool) {
	i, ok := t.index[name]
	if !ok {
		return Field{}, false
	}
	return t.fields[i], true
}

func (t *StructType) FieldIdx(name string) (int, bool) {
	i, ok := t.index[name]
	return i, ok
}

type MapType struct {
	value      *ListType
	KeysSorted bool
}

func MapOf(key, item DataType) *MapType {
	if key == nil || item == nil {
		panic("arrow: nil key or item type for MapType")
	}

	return &MapType{value: ListOf(StructOf(Field{Name: "key", Type: key}, Field{Name: "value", Type: item, Nullable: true}))}
}

func (*MapType) ID() Type     { return MAP }
func (*MapType) Name() string { return "map" }

func (t *MapType) String() string {
	var o strings.Builder
	o.WriteString(fmt.Sprintf("map<%s, %s",
		t.value.Elem().(*StructType).Field(0).Type,
		t.value.Elem().(*StructType).Field(1).Type))
	if t.KeysSorted {
		o.WriteString(", keys_sorted")
	}
	o.WriteString(">")
	return o.String()
}

func (t *MapType) KeyField() Field        { return t.value.Elem().(*StructType).Field(0) }
func (t *MapType) KeyType() DataType      { return t.KeyField().Type }
func (t *MapType) ItemField() Field       { return t.value.Elem().(*StructType).Field(1) }
func (t *MapType) ItemType() DataType     { return t.ItemField().Type }
func (t *MapType) ValueType() *StructType { return t.value.Elem().(*StructType) }
func (t *MapType) ValueField() Field {
	return Field{
		Name: "entries",
		Type: t.ValueType(),
	}
}

func (t *MapType) SetItemNullable(nullable bool) {
	t.value.Elem().(*StructType).fields[1].Nullable = nullable
}

type Field struct {
	Name     string   // Field name
	Type     DataType // The field's data type
	Nullable bool     // Fields can be nullable
	Metadata Metadata // The field's metadata, if any
}

func (f Field) HasMetadata() bool { return f.Metadata.Len() != 0 }

func (f Field) Equal(o Field) bool {
	switch {
	case f.Name != o.Name:
		return false
	case f.Nullable != o.Nullable:
		return false
	case !TypeEqual(f.Type, o.Type, CheckMetadata()):
		return false
	case !f.Metadata.Equal(o.Metadata):
		return false
	}
	return true
}

func (f Field) String() string {
	o := new(strings.Builder)
	nullable := ""
	if f.Nullable {
		nullable = ", nullable"
	}
	fmt.Fprintf(o, "%s: type=%v%v", f.Name, f.Type, nullable)
	if f.HasMetadata() {
		fmt.Fprintf(o, "\n%*.smetadata: %v", len(f.Name)+2, "", f.Metadata)
	}
	return o.String()
}

var (
	_ DataType = (*ListType)(nil)
	_ DataType = (*StructType)(nil)
	_ DataType = (*MapType)(nil)
)
