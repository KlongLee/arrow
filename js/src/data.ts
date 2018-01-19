// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import { popcnt_bit_range } from './util/bit';
import { VectorLike, Vector } from './vector';
import { VectorType, TypedArray, TypedArrayConstructor, Dictionary } from './type';
import { Int, Bool, FlatListType, List, FixedSizeList, Struct, Map_ } from './type';
import { DataType, FlatType, ListType, NestedType, DenseUnion, SparseUnion } from './type';

export function toTypedArray<T extends TypedArray>(ArrayType: TypedArrayConstructor<T>, values?: T | ArrayLike<number> | Iterable<number> | null): T {
    if (!ArrayType && ArrayBuffer.isView(values)) { return values; }
    return values instanceof ArrayType ? values
         : !values || !ArrayBuffer.isView(values) ? ArrayType.from(values || [])
         : new ArrayType(values.buffer, values.byteOffset, values.byteLength / ArrayType.BYTES_PER_ELEMENT);
}

export type Data<T extends DataType> = DataTypes<T>[T['TType']] & BaseData<T>;
export interface DataTypes<T extends DataType> {
/*                [Type.NONE]*/  0: BaseData<T>;
/*                [Type.Null]*/  1: FlatData<T>;
/*                 [Type.Int]*/  2: FlatData<T>;
/*               [Type.Float]*/  3: FlatData<T>;
/*              [Type.Binary]*/  4: FlatListData<T>;
/*                [Type.Utf8]*/  5: FlatListData<T>;
/*                [Type.Bool]*/  6: BoolData;
/*             [Type.Decimal]*/  7: FlatData<T>;
/*                [Type.Date]*/  8: FlatData<T>;
/*                [Type.Time]*/  9: FlatData<T>;
/*           [Type.Timestamp]*/ 10: FlatData<T>;
/*            [Type.Interval]*/ 11: FlatData<T>;
/*                [Type.List]*/ 12: ListData<List<T>>;
/*              [Type.Struct]*/ 13: NestedData<Struct>;
/*               [Type.Union]*/ 14: UnionData;
/*     [Type.FixedSizeBinary]*/ 15: FlatData<T>;
/*       [Type.FixedSizeList]*/ 16: ListData<FixedSizeList<T>>;
/*                 [Type.Map]*/ 17: NestedData<Map_>;
/*  [Type.DenseUnion]*/ DenseUnion: DenseUnionData;
/*[Type.SparseUnion]*/ SparseUnion: SparseUnionData;
/*[  Type.Dictionary]*/ Dictionary: DictionaryData<any>;
}
// When slicing, we do not know the null count of the sliced range without
// doing some computation. To avoid doing this eagerly, we set the null count
// to -1 (any negative number will do). When Array::null_count is called the
// first time, the null count will be computed. See ARROW-33
export type kUnknownNullCount = -1;
export const kUnknownNullCount = -1;

export class BaseData<T extends DataType = DataType> implements VectorLike {
    protected _type: T;
    protected _length: number;
    protected _offset: number;
    // @ts-ignore
    protected _childData: Data<any>[];
    protected _nullCount: number | kUnknownNullCount;
    protected /*  [VectorType.OFFSET]:*/ 0?: Int32Array;
    protected /*    [VectorType.DATA]:*/ 1?: T['TArray'];
    protected /*[VectorType.VALIDITY]:*/ 2?: Uint8Array;
    protected /*    [VectorType.TYPE]:*/ 3?: Int8Array;
    constructor(type: T, length: number, offset?: number, nullCount?: number) {
        this._type = type;
        this._length = Math.floor(Math.max(length || 0, 0));
        this._offset = Math.floor(Math.max(offset || 0, 0));
        this._nullCount = Math.floor(Math.max(nullCount || 0, -1));
    }
    public get type() { return this._type; }
    public get length() { return this._length; }
    public get offset() { return this._offset; }
    public get typeId() { return this._type.TType; }
    public get childData() { return this._childData; }
    public get nullBitmap() { return this[VectorType.VALIDITY]; }
    public get nullCount() {
        let nullCount = this._nullCount;
        let nullBitmap: Uint8Array | undefined;
        if (nullCount === -1 && (nullBitmap = this[VectorType.VALIDITY])) {
            this._nullCount = nullCount = popcnt_bit_range(nullBitmap, this._offset, this._offset + this._length);
        }
        return nullCount;
    }
    public clone(length = this._length, offset = this._offset, nullCount = this._nullCount) {
        return new BaseData<T>(this._type, length, offset, nullCount) as this;
    }
    public slice(offset: number, length: number) {
        return length <= 0 ? this : this.sliceInternal(this.clone(
            length, this._offset + offset, +(this._nullCount === 0) - 1
        ), offset, length);
    }
    protected sliceInternal(clone: this, offset: number, length: number) {
        let arr: any;
        // If typeIds exist, slice the typeIds buffer
        (arr = this[VectorType.TYPE]) && (clone[VectorType.TYPE] = this.sliceData(arr, offset, length));
        // If offsets exist, only slice the offsets buffer
        (arr = this[VectorType.OFFSET]) && (clone[VectorType.OFFSET] = this.sliceOffsets(arr, offset, length)) ||
            // Otherwise if no offsets, slice the data buffer
            (arr = this[VectorType.DATA]) && (clone[VectorType.DATA] = this.sliceData(arr, offset, length));
        return clone;
    }
    protected sliceData(data: T['TArray'] & TypedArray, offset: number, length: number) {
        return data.subarray(offset, offset + length);
    }
    protected sliceOffsets(valueOffsets: Int32Array, offset: number, length: number) {
        return valueOffsets.subarray(offset, offset + length + 1);
    }
}

export class FlatData<T extends FlatType> extends BaseData<T> {
    public /*    [VectorType.DATA]:*/ 1: T['TArray'];
    public /*[VectorType.VALIDITY]:*/ 2: Uint8Array;
    public get values() { return this[VectorType.DATA]; }
    constructor(type: T, length: number, nullBitmap: Uint8Array | null | undefined, data: Iterable<number>, offset?: number, nullCount?: number) {
        super(type, length, offset, nullCount);
        this[VectorType.DATA] = toTypedArray(this.ArrayType, data);
        this[VectorType.VALIDITY] = toTypedArray(Uint8Array, nullBitmap);
    }
    public get ArrayType(): T['ArrayType'] { return this._type.ArrayType; }
    public clone(length = this._length, offset = this._offset, nullCount = this._nullCount) {
        return new FlatData<T>(this._type, length, this[VectorType.VALIDITY], this[VectorType.DATA], offset, nullCount) as this;
    }
}

export class BoolData extends FlatData<Bool> {
    protected sliceData(data: Uint8Array) { return data; }
    public clone(length = this._length, offset = this._offset, nullCount = this._nullCount) {
        return new BoolData(this._type, length, this[VectorType.VALIDITY], this[VectorType.DATA], offset, nullCount) as this;
    }
}

export class FlatListData<T extends FlatListType> extends FlatData<T> {
    public /*  [VectorType.OFFSET]:*/ 0: Int32Array;
    public /*    [VectorType.DATA]:*/ 1: T['TArray'];
    public /*[VectorType.VALIDITY]:*/ 2: Uint8Array;
    public get values() { return this[VectorType.DATA]; }
    public get valueOffsets() { return this[VectorType.OFFSET]; }
    constructor(type: T, length: number, nullBitmap: Uint8Array | null | undefined, valueOffsets: Iterable<number>, data: T['TArray'], offset?: number, nullCount?: number) {
        super(type, length, nullBitmap, data, offset, nullCount);
        this[VectorType.OFFSET] = toTypedArray(Int32Array, valueOffsets);
    }
    public clone(length = this._length, offset = this._offset, nullCount = this._nullCount) {
        return new FlatListData<T>(this._type, length, this[VectorType.VALIDITY], this[VectorType.OFFSET], this[VectorType.DATA], offset, nullCount) as this;
    }
}

export class DictionaryData<T extends DataType> extends BaseData<Dictionary<T>> {
    protected _dictionary: Vector<T>;
    protected _indicies: Data<Int<any>>;
    public get indicies() { return this._indicies; }
    public get dictionary() { return this._dictionary; }
    constructor(type: Dictionary<T>, dictionary: Vector<T>, indicies: Data<Int<any>>) {
        super(type, indicies.length, (indicies as any)._nullCount);
        this._indicies = indicies;
        this._dictionary = dictionary;
    }
    public get length() { return this._indicies.length; }
    public get nullCount() { return this._indicies.nullCount; }
    public clone(length = this._length, offset = this._offset) {
        return new DictionaryData<T>(this._type, this._dictionary, this._indicies.slice(offset - this._offset, length)) as this;
    }
    protected sliceInternal(clone: this, _offset: number, _length: number) {
        clone._length = clone._indicies.length;
        clone._nullCount = (clone._indicies as any)._nullCount;
        return clone;
    }
}

export class NestedData<T extends NestedType = NestedType> extends BaseData<T> {
    public /*[VectorType.VALIDITY]:*/ 2: Uint8Array;
    constructor(type: T, length: number, nullBitmap: Uint8Array | null | undefined, childData: Data<any>[], offset?: number, nullCount?: number) {
        super(type, length, offset, nullCount);
        this._childData = childData;
        this[VectorType.VALIDITY] = toTypedArray(Uint8Array, nullBitmap);
    }
    public clone(length = this._length, offset = this._offset, nullCount = this._nullCount) {
        return new NestedData<T>(this._type, length, this[VectorType.VALIDITY], this._childData, offset, nullCount) as this;
    }
    protected sliceInternal(clone: this, offset: number, length: number) {
        if (!this[VectorType.OFFSET]) {
            clone._childData = this._childData.map((child) => child.slice(offset, length));
        }
        return super.sliceInternal(clone, offset, length);
    }
}

export class ListData<T extends ListType> extends NestedData<T> {
    public /*  [VectorType.OFFSET]:*/ 0: Int32Array;
    public /*[VectorType.VALIDITY]:*/ 2: Uint8Array;
    protected _valuesData: Data<T>;
    public get values() { return this._valuesData; }
    public get valueOffsets() { return this[VectorType.OFFSET]; }
    constructor(type: T, length: number, nullBitmap: Uint8Array | null | undefined, valueOffsets: Iterable<number>, valueChildData: Data<T>, offset?: number, nullCount?: number) {
        super(type, length, nullBitmap, [valueChildData], offset, nullCount);
        this._valuesData = valueChildData;
        this[VectorType.OFFSET] = toTypedArray(Int32Array, valueOffsets);
    }
    public clone(length = this._length, offset = this._offset, nullCount = this._nullCount) {
        return new ListData<T>(this._type, length, this[VectorType.VALIDITY], this[VectorType.OFFSET], this._valuesData, offset, nullCount) as this;
    }
}

export class UnionData<T extends (DenseUnion | SparseUnion) = any> extends NestedData<T> {
    public /*    [VectorType.TYPE]:*/ 3: T['TArray'];
    public get typeIds() { return this[VectorType.TYPE]; }
    constructor(type: T, length: number, nullBitmap: Uint8Array | null | undefined, typeIds: Iterable<number>, childData: Data<any>[], offset?: number, nullCount?: number) {
        super(type, length, nullBitmap, childData, offset, nullCount);
        this[VectorType.TYPE] = toTypedArray(Int8Array, typeIds);
    }
    public clone(length = this._length, offset = this._offset, nullCount = this._nullCount) {
        return new UnionData<T>(this._type, length, this[VectorType.VALIDITY], this[VectorType.TYPE], this._childData, offset, nullCount) as this;
    }
}

export class SparseUnionData extends UnionData<SparseUnion> {
    constructor(type: SparseUnion, length: number, nullBitmap: Uint8Array | null | undefined, typeIds: Iterable<number>, childData: Data<any>[], offset?: number, nullCount?: number) {
        super(type, length, nullBitmap, typeIds, childData, offset, nullCount);
    }
    public clone(length = this._length, offset = this._offset, nullCount = this._nullCount) {
        return new SparseUnionData(this._type, length, this[VectorType.VALIDITY], this[VectorType.TYPE], this._childData, offset, nullCount) as this;
    }
}

export class DenseUnionData extends UnionData<DenseUnion> {
    public /*  [VectorType.OFFSET]:*/ 0: Int32Array;
    public get valueOffsets() { return this[VectorType.OFFSET]; }
    constructor(type: DenseUnion, length: number, nullBitmap: Uint8Array | null | undefined, typeIds: Iterable<number>, valueOffsets: Iterable<number>, childData: Data<any>[], offset?: number, nullCount?: number) {
        super(type, length, nullBitmap, typeIds, childData, offset, nullCount);
        this[VectorType.OFFSET] = toTypedArray(Int32Array, valueOffsets);
    }
    public clone(length = this._length, offset = this._offset, nullCount = this._nullCount) {
        return new DenseUnionData(this._type, length, this[VectorType.VALIDITY], this[VectorType.TYPE], this[VectorType.OFFSET], this._childData, offset, nullCount) as this;
    }
}

export class ChunkedData<T extends DataType> extends BaseData<T> {
    protected _childVectors: Vector<T>[];
    protected _childOffsets: Uint32Array;
    public get childVectors() { return this._childVectors; }
    public get childOffsets() { return this._childOffsets; }
    public get childData() {
        return this._childData || (
               this._childData = this._childVectors.map(({ data }) => data));
    }
    constructor(type: T, length: number, childVectors: Vector<T>[], offset?: number, nullCount?: number, childOffsets?: Uint32Array) {
        super(type, length, offset, nullCount);
        this._childVectors = childVectors;
        this._childOffsets = childOffsets || ChunkedData.computeOffsets(childVectors);
    }
    public get nullCount() {
        let nullCount = this._nullCount;
        if (nullCount === -1) {
            this._nullCount = nullCount = this._childVectors.reduce((x, c) => x + c.nullCount, 0);
        }
        return nullCount;
    }
    public clone(length = this._length, offset = this._offset, nullCount = this._nullCount) {
        return new ChunkedData<T>(this._type, length, this._childVectors, offset, nullCount, this._childOffsets) as this;
    }
    protected sliceInternal(clone: this, offset: number, length: number) {
        const chunks = this._childVectors;
        const offsets = this._childOffsets;
        const chunkSlices: Vector<T>[] = [];
        for (let childIndex = -1, numChildren = chunks.length; ++childIndex < numChildren;) {
            const child = chunks[childIndex];
            const childLength = child.length;
            const childOffset = offsets[childIndex];
            // If the child is to the right of the slice boundary, exclude
            if (childOffset >= offset + length) { continue; }
            // If the child is to the left of of the slice boundary, exclude
            if (offset >= childOffset + childLength) { continue; }
            // If the child is between both left and right boundaries, include w/o slicing
            if (childOffset >= offset && (childOffset + childLength) <= offset + length) {
                chunkSlices.push(child);
                continue;
            }
            // If the child overlaps one of the slice boundaries, include that slice
            const begin = Math.max(0, offset - childOffset);
            const end = begin + Math.min(childLength - begin, (offset + length) - childOffset);
            chunkSlices.push(child.slice(begin, end));
        }
        clone._childVectors = chunkSlices;
        clone._childOffsets = ChunkedData.computeOffsets(chunkSlices);
        return clone;
    }
    static computeOffsets<T extends DataType>(childVectors: Vector<T>[]) {
        const childOffsets = new Uint32Array(childVectors.length + 1);
        for (let index = 0, length = childOffsets.length, childOffset = childOffsets[0] = 0; ++index < length;) {
            childOffsets[index] = (childOffset += childVectors[index - 1].length);
        }
        return childOffsets;
    }
}
