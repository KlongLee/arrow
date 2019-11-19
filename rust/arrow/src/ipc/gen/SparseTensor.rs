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

// automatically generated by the FlatBuffers compiler, do not modify


#![allow(dead_code)]
#![allow(unused_imports)]

use crate::ipc::gen::Schema::*;
use crate::ipc::gen::Tensor::*;

use std::cmp::Ordering;
use std::mem;

use flatbuffers::EndianScalar;

#[allow(non_camel_case_types)]
#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum SparseTensorIndex {
    NONE = 0,
    SparseTensorIndexCOO = 1,
    SparseMatrixIndexCSR = 2,
}

const ENUM_MIN_SPARSE_TENSOR_INDEX: u8 = 0;
const ENUM_MAX_SPARSE_TENSOR_INDEX: u8 = 2;

impl<'a> flatbuffers::Follow<'a> for SparseTensorIndex {
    type Inner = Self;
    #[inline]
    fn follow(buf: &'a [u8], loc: usize) -> Self::Inner {
        flatbuffers::read_scalar_at::<Self>(buf, loc)
    }
}

impl flatbuffers::EndianScalar for SparseTensorIndex {
    #[inline]
    fn to_little_endian(self) -> Self {
        let n = u8::to_le(self as u8);
        let p = &n as *const u8 as *const SparseTensorIndex;
        unsafe { *p }
    }
    #[inline]
    fn from_little_endian(self) -> Self {
        let n = u8::from_le(self as u8);
        let p = &n as *const u8 as *const SparseTensorIndex;
        unsafe { *p }
    }
}

impl flatbuffers::Push for SparseTensorIndex {
    type Output = SparseTensorIndex;
    #[inline]
    fn push(&self, dst: &mut [u8], _rest: &[u8]) {
        flatbuffers::emplace_scalar::<SparseTensorIndex>(dst, *self);
    }
}

#[allow(non_camel_case_types)]
const ENUM_VALUES_SPARSE_TENSOR_INDEX: [SparseTensorIndex; 3] = [
    SparseTensorIndex::NONE,
    SparseTensorIndex::SparseTensorIndexCOO,
    SparseTensorIndex::SparseMatrixIndexCSR,
];

#[allow(non_camel_case_types)]
const ENUM_NAMES_SPARSE_TENSOR_INDEX: [&'static str; 3] =
    ["NONE", "SparseTensorIndexCOO", "SparseMatrixIndexCSR"];

pub fn enum_name_sparse_tensor_index(e: SparseTensorIndex) -> &'static str {
    let index = e as u8;
    ENUM_NAMES_SPARSE_TENSOR_INDEX[index as usize]
}

pub struct SparseTensorIndexUnionTableOffset {}
pub enum SparseTensorIndexCOOOffset {}
#[derive(Copy, Clone, Debug, PartialEq)]

/// ----------------------------------------------------------------------
/// EXPERIMENTAL: Data structures for sparse tensors
/// Coodinate (COO) format of sparse tensor index.
///
/// COO's index list are represented as a NxM matrix,
/// where N is the number of non-zero values,
/// and M is the number of dimensions of a sparse tensor.
///
/// indicesBuffer stores the location and size of the data of this indices
/// matrix.  The value type and the stride of the indices matrix is
/// specified in indicesType and indicesStrides fields.
///
/// For example, let X be a 2x3x4x5 tensor, and it has the following
/// 6 non-zero values:
///
///   X[0, 1, 2, 0] := 1
///   X[1, 1, 2, 3] := 2
///   X[0, 2, 1, 0] := 3
///   X[0, 1, 3, 0] := 4
///   X[0, 1, 2, 1] := 5
///   X[1, 2, 0, 4] := 6
///
/// In COO format, the index matrix of X is the following 4x6 matrix:
///
///   [[0, 0, 0, 0, 1, 1],
///    [1, 1, 1, 2, 1, 2],
///    [2, 2, 3, 1, 2, 0],
///    [0, 1, 0, 0, 3, 4]]
///
/// Note that the indices are sorted in lexicographical order.
pub struct SparseTensorIndexCOO<'a> {
    pub _tab: flatbuffers::Table<'a>,
}

impl<'a> flatbuffers::Follow<'a> for SparseTensorIndexCOO<'a> {
    type Inner = SparseTensorIndexCOO<'a>;
    #[inline]
    fn follow(buf: &'a [u8], loc: usize) -> Self::Inner {
        Self {
            _tab: flatbuffers::Table { buf: buf, loc: loc },
        }
    }
}

impl<'a> SparseTensorIndexCOO<'a> {
    #[inline]
    pub fn init_from_table(table: flatbuffers::Table<'a>) -> Self {
        SparseTensorIndexCOO { _tab: table }
    }
    #[allow(unused_mut)]
    pub fn create<'bldr: 'args, 'args: 'mut_bldr, 'mut_bldr>(
        _fbb: &'mut_bldr mut flatbuffers::FlatBufferBuilder<'bldr>,
        args: &'args SparseTensorIndexCOOArgs<'args>,
    ) -> flatbuffers::WIPOffset<SparseTensorIndexCOO<'bldr>> {
        let mut builder = SparseTensorIndexCOOBuilder::new(_fbb);
        if let Some(x) = args.indicesBuffer {
            builder.add_indicesBuffer(x);
        }
        if let Some(x) = args.indicesStrides {
            builder.add_indicesStrides(x);
        }
        if let Some(x) = args.indicesType {
            builder.add_indicesType(x);
        }
        builder.finish()
    }

    pub const VT_INDICESTYPE: flatbuffers::VOffsetT = 4;
    pub const VT_INDICESSTRIDES: flatbuffers::VOffsetT = 6;
    pub const VT_INDICESBUFFER: flatbuffers::VOffsetT = 8;

    /// The type of values in indicesBuffer
    #[inline]
    pub fn indicesType(&self) -> Option<Int<'a>> {
        self._tab.get::<flatbuffers::ForwardsUOffset<Int<'a>>>(
            SparseTensorIndexCOO::VT_INDICESTYPE,
            None,
        )
    }
    /// Non-negative byte offsets to advance one value cell along each dimension
    #[inline]
    pub fn indicesStrides(&self) -> Option<flatbuffers::Vector<'a, i64>> {
        self._tab
            .get::<flatbuffers::ForwardsUOffset<flatbuffers::Vector<'a, i64>>>(
                SparseTensorIndexCOO::VT_INDICESSTRIDES,
                None,
            )
    }
    /// The location and size of the indices matrix's data
    #[inline]
    pub fn indicesBuffer(&self) -> Option<&'a Buffer> {
        self._tab
            .get::<Buffer>(SparseTensorIndexCOO::VT_INDICESBUFFER, None)
    }
}

pub struct SparseTensorIndexCOOArgs<'a> {
    pub indicesType: Option<flatbuffers::WIPOffset<Int<'a>>>,
    pub indicesStrides: Option<flatbuffers::WIPOffset<flatbuffers::Vector<'a, i64>>>,
    pub indicesBuffer: Option<&'a Buffer>,
}
impl<'a> Default for SparseTensorIndexCOOArgs<'a> {
    #[inline]
    fn default() -> Self {
        SparseTensorIndexCOOArgs {
            indicesType: None,
            indicesStrides: None,
            indicesBuffer: None,
        }
    }
}
pub struct SparseTensorIndexCOOBuilder<'a: 'b, 'b> {
    fbb_: &'b mut flatbuffers::FlatBufferBuilder<'a>,
    start_: flatbuffers::WIPOffset<flatbuffers::TableUnfinishedWIPOffset>,
}
impl<'a: 'b, 'b> SparseTensorIndexCOOBuilder<'a, 'b> {
    #[inline]
    pub fn add_indicesType(&mut self, indicesType: flatbuffers::WIPOffset<Int<'b>>) {
        self.fbb_.push_slot_always::<flatbuffers::WIPOffset<Int>>(
            SparseTensorIndexCOO::VT_INDICESTYPE,
            indicesType,
        );
    }
    #[inline]
    pub fn add_indicesStrides(
        &mut self,
        indicesStrides: flatbuffers::WIPOffset<flatbuffers::Vector<'b, i64>>,
    ) {
        self.fbb_.push_slot_always::<flatbuffers::WIPOffset<_>>(
            SparseTensorIndexCOO::VT_INDICESSTRIDES,
            indicesStrides,
        );
    }
    #[inline]
    pub fn add_indicesBuffer(&mut self, indicesBuffer: &'b Buffer) {
        self.fbb_.push_slot_always::<&Buffer>(
            SparseTensorIndexCOO::VT_INDICESBUFFER,
            indicesBuffer,
        );
    }
    #[inline]
    pub fn new(
        _fbb: &'b mut flatbuffers::FlatBufferBuilder<'a>,
    ) -> SparseTensorIndexCOOBuilder<'a, 'b> {
        let start = _fbb.start_table();
        SparseTensorIndexCOOBuilder {
            fbb_: _fbb,
            start_: start,
        }
    }
    #[inline]
    pub fn finish(self) -> flatbuffers::WIPOffset<SparseTensorIndexCOO<'a>> {
        let o = self.fbb_.end_table(self.start_);
        flatbuffers::WIPOffset::new(o.value())
    }
}

pub enum SparseMatrixIndexCSROffset {}
#[derive(Copy, Clone, Debug, PartialEq)]

/// Compressed Sparse Row format, that is matrix-specific.
pub struct SparseMatrixIndexCSR<'a> {
    pub _tab: flatbuffers::Table<'a>,
}

impl<'a> flatbuffers::Follow<'a> for SparseMatrixIndexCSR<'a> {
    type Inner = SparseMatrixIndexCSR<'a>;
    #[inline]
    fn follow(buf: &'a [u8], loc: usize) -> Self::Inner {
        Self {
            _tab: flatbuffers::Table { buf: buf, loc: loc },
        }
    }
}

impl<'a> SparseMatrixIndexCSR<'a> {
    #[inline]
    pub fn init_from_table(table: flatbuffers::Table<'a>) -> Self {
        SparseMatrixIndexCSR { _tab: table }
    }
    #[allow(unused_mut)]
    pub fn create<'bldr: 'args, 'args: 'mut_bldr, 'mut_bldr>(
        _fbb: &'mut_bldr mut flatbuffers::FlatBufferBuilder<'bldr>,
        args: &'args SparseMatrixIndexCSRArgs<'args>,
    ) -> flatbuffers::WIPOffset<SparseMatrixIndexCSR<'bldr>> {
        let mut builder = SparseMatrixIndexCSRBuilder::new(_fbb);
        if let Some(x) = args.indicesBuffer {
            builder.add_indicesBuffer(x);
        }
        if let Some(x) = args.indicesType {
            builder.add_indicesType(x);
        }
        if let Some(x) = args.indptrBuffer {
            builder.add_indptrBuffer(x);
        }
        if let Some(x) = args.indptrType {
            builder.add_indptrType(x);
        }
        builder.finish()
    }

    pub const VT_INDPTRTYPE: flatbuffers::VOffsetT = 4;
    pub const VT_INDPTRBUFFER: flatbuffers::VOffsetT = 6;
    pub const VT_INDICESTYPE: flatbuffers::VOffsetT = 8;
    pub const VT_INDICESBUFFER: flatbuffers::VOffsetT = 10;

    /// The type of values in indptrBuffer
    #[inline]
    pub fn indptrType(&self) -> Option<Int<'a>> {
        self._tab.get::<flatbuffers::ForwardsUOffset<Int<'a>>>(
            SparseMatrixIndexCSR::VT_INDPTRTYPE,
            None,
        )
    }
    /// indptrBuffer stores the location and size of indptr array that
    /// represents the range of the rows.
    /// The i-th row spans from indptr[i] to indptr[i+1] in the data.
    /// The length of this array is 1 + (the number of rows), and the type
    /// of index value is long.
    ///
    /// For example, let X be the following 6x4 matrix:
    ///
    ///   X := [[0, 1, 2, 0],
    ///         [0, 0, 3, 0],
    ///         [0, 4, 0, 5],
    ///         [0, 0, 0, 0],
    ///         [6, 0, 7, 8],
    ///         [0, 9, 0, 0]].
    ///
    /// The array of non-zero values in X is:
    ///
    ///   values(X) = [1, 2, 3, 4, 5, 6, 7, 8, 9].
    ///
    /// And the indptr of X is:
    ///
    ///   indptr(X) = [0, 2, 3, 5, 5, 8, 10].
    #[inline]
    pub fn indptrBuffer(&self) -> Option<&'a Buffer> {
        self._tab
            .get::<Buffer>(SparseMatrixIndexCSR::VT_INDPTRBUFFER, None)
    }
    /// The type of values in indicesBuffer
    #[inline]
    pub fn indicesType(&self) -> Option<Int<'a>> {
        self._tab.get::<flatbuffers::ForwardsUOffset<Int<'a>>>(
            SparseMatrixIndexCSR::VT_INDICESTYPE,
            None,
        )
    }
    /// indicesBuffer stores the location and size of the array that
    /// contains the column indices of the corresponding non-zero values.
    /// The type of index value is long.
    ///
    /// For example, the indices of the above X is:
    ///
    ///   indices(X) = [1, 2, 2, 1, 3, 0, 2, 3, 1].
    ///
    /// Note that the indices are sorted in lexicographical order for each row.
    #[inline]
    pub fn indicesBuffer(&self) -> Option<&'a Buffer> {
        self._tab
            .get::<Buffer>(SparseMatrixIndexCSR::VT_INDICESBUFFER, None)
    }
}

pub struct SparseMatrixIndexCSRArgs<'a> {
    pub indptrType: Option<flatbuffers::WIPOffset<Int<'a>>>,
    pub indptrBuffer: Option<&'a Buffer>,
    pub indicesType: Option<flatbuffers::WIPOffset<Int<'a>>>,
    pub indicesBuffer: Option<&'a Buffer>,
}
impl<'a> Default for SparseMatrixIndexCSRArgs<'a> {
    #[inline]
    fn default() -> Self {
        SparseMatrixIndexCSRArgs {
            indptrType: None,
            indptrBuffer: None,
            indicesType: None,
            indicesBuffer: None,
        }
    }
}
pub struct SparseMatrixIndexCSRBuilder<'a: 'b, 'b> {
    fbb_: &'b mut flatbuffers::FlatBufferBuilder<'a>,
    start_: flatbuffers::WIPOffset<flatbuffers::TableUnfinishedWIPOffset>,
}
impl<'a: 'b, 'b> SparseMatrixIndexCSRBuilder<'a, 'b> {
    #[inline]
    pub fn add_indptrType(&mut self, indptrType: flatbuffers::WIPOffset<Int<'b>>) {
        self.fbb_.push_slot_always::<flatbuffers::WIPOffset<Int>>(
            SparseMatrixIndexCSR::VT_INDPTRTYPE,
            indptrType,
        );
    }
    #[inline]
    pub fn add_indptrBuffer(&mut self, indptrBuffer: &'b Buffer) {
        self.fbb_.push_slot_always::<&Buffer>(
            SparseMatrixIndexCSR::VT_INDPTRBUFFER,
            indptrBuffer,
        );
    }
    #[inline]
    pub fn add_indicesType(&mut self, indicesType: flatbuffers::WIPOffset<Int<'b>>) {
        self.fbb_.push_slot_always::<flatbuffers::WIPOffset<Int>>(
            SparseMatrixIndexCSR::VT_INDICESTYPE,
            indicesType,
        );
    }
    #[inline]
    pub fn add_indicesBuffer(&mut self, indicesBuffer: &'b Buffer) {
        self.fbb_.push_slot_always::<&Buffer>(
            SparseMatrixIndexCSR::VT_INDICESBUFFER,
            indicesBuffer,
        );
    }
    #[inline]
    pub fn new(
        _fbb: &'b mut flatbuffers::FlatBufferBuilder<'a>,
    ) -> SparseMatrixIndexCSRBuilder<'a, 'b> {
        let start = _fbb.start_table();
        SparseMatrixIndexCSRBuilder {
            fbb_: _fbb,
            start_: start,
        }
    }
    #[inline]
    pub fn finish(self) -> flatbuffers::WIPOffset<SparseMatrixIndexCSR<'a>> {
        let o = self.fbb_.end_table(self.start_);
        flatbuffers::WIPOffset::new(o.value())
    }
}

pub enum SparseTensorOffset {}
#[derive(Copy, Clone, Debug, PartialEq)]

pub struct SparseTensor<'a> {
    pub _tab: flatbuffers::Table<'a>,
}

impl<'a> flatbuffers::Follow<'a> for SparseTensor<'a> {
    type Inner = SparseTensor<'a>;
    #[inline]
    fn follow(buf: &'a [u8], loc: usize) -> Self::Inner {
        Self {
            _tab: flatbuffers::Table { buf: buf, loc: loc },
        }
    }
}

impl<'a> SparseTensor<'a> {
    #[inline]
    pub fn init_from_table(table: flatbuffers::Table<'a>) -> Self {
        SparseTensor { _tab: table }
    }
    #[allow(unused_mut)]
    pub fn create<'bldr: 'args, 'args: 'mut_bldr, 'mut_bldr>(
        _fbb: &'mut_bldr mut flatbuffers::FlatBufferBuilder<'bldr>,
        args: &'args SparseTensorArgs<'args>,
    ) -> flatbuffers::WIPOffset<SparseTensor<'bldr>> {
        let mut builder = SparseTensorBuilder::new(_fbb);
        builder.add_non_zero_length(args.non_zero_length);
        if let Some(x) = args.data {
            builder.add_data(x);
        }
        if let Some(x) = args.sparseIndex {
            builder.add_sparseIndex(x);
        }
        if let Some(x) = args.shape {
            builder.add_shape(x);
        }
        if let Some(x) = args.type_ {
            builder.add_type_(x);
        }
        builder.add_sparseIndex_type(args.sparseIndex_type);
        builder.add_type_type(args.type_type);
        builder.finish()
    }

    pub const VT_TYPE_TYPE: flatbuffers::VOffsetT = 4;
    pub const VT_TYPE_: flatbuffers::VOffsetT = 6;
    pub const VT_SHAPE: flatbuffers::VOffsetT = 8;
    pub const VT_NON_ZERO_LENGTH: flatbuffers::VOffsetT = 10;
    pub const VT_SPARSEINDEX_TYPE: flatbuffers::VOffsetT = 12;
    pub const VT_SPARSEINDEX: flatbuffers::VOffsetT = 14;
    pub const VT_DATA: flatbuffers::VOffsetT = 16;

    #[inline]
    pub fn type_type(&self) -> Type {
        self._tab
            .get::<Type>(SparseTensor::VT_TYPE_TYPE, Some(Type::NONE))
            .unwrap()
    }
    /// The type of data contained in a value cell.
    /// Currently only fixed-width value types are supported,
    /// no strings or nested types.
    #[inline]
    pub fn type_(&self) -> Option<flatbuffers::Table<'a>> {
        self._tab
            .get::<flatbuffers::ForwardsUOffset<flatbuffers::Table<'a>>>(
                SparseTensor::VT_TYPE_,
                None,
            )
    }
    /// The dimensions of the tensor, optionally named.
    #[inline]
    pub fn shape(
        &self,
    ) -> Option<flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<TensorDim<'a>>>>
    {
        self._tab.get::<flatbuffers::ForwardsUOffset<
            flatbuffers::Vector<flatbuffers::ForwardsUOffset<TensorDim<'a>>>,
        >>(SparseTensor::VT_SHAPE, None)
    }
    /// The number of non-zero values in a sparse tensor.
    #[inline]
    pub fn non_zero_length(&self) -> i64 {
        self._tab
            .get::<i64>(SparseTensor::VT_NON_ZERO_LENGTH, Some(0))
            .unwrap()
    }
    #[inline]
    pub fn sparseIndex_type(&self) -> SparseTensorIndex {
        self._tab
            .get::<SparseTensorIndex>(
                SparseTensor::VT_SPARSEINDEX_TYPE,
                Some(SparseTensorIndex::NONE),
            )
            .unwrap()
    }
    /// Sparse tensor index
    #[inline]
    pub fn sparseIndex(&self) -> Option<flatbuffers::Table<'a>> {
        self._tab
            .get::<flatbuffers::ForwardsUOffset<flatbuffers::Table<'a>>>(
                SparseTensor::VT_SPARSEINDEX,
                None,
            )
    }
    /// The location and size of the tensor's data
    #[inline]
    pub fn data(&self) -> Option<&'a Buffer> {
        self._tab.get::<Buffer>(SparseTensor::VT_DATA, None)
    }
    #[inline]
    #[allow(non_snake_case)]
    pub fn type_as_null(&self) -> Option<Null<'a>> {
        if self.type_type() == Type::Null {
            self.type_().map(|u| Null::init_from_table(u))
        } else {
            None
        }
    }

    #[inline]
    #[allow(non_snake_case)]
    pub fn type_as_int(&self) -> Option<Int<'a>> {
        if self.type_type() == Type::Int {
            self.type_().map(|u| Int::init_from_table(u))
        } else {
            None
        }
    }

    #[inline]
    #[allow(non_snake_case)]
    pub fn type_as_floating_point(&self) -> Option<FloatingPoint<'a>> {
        if self.type_type() == Type::FloatingPoint {
            self.type_().map(|u| FloatingPoint::init_from_table(u))
        } else {
            None
        }
    }

    #[inline]
    #[allow(non_snake_case)]
    pub fn type_as_binary(&self) -> Option<Binary<'a>> {
        if self.type_type() == Type::Binary {
            self.type_().map(|u| Binary::init_from_table(u))
        } else {
            None
        }
    }

    #[inline]
    #[allow(non_snake_case)]
    pub fn type_as_utf_8(&self) -> Option<Utf8<'a>> {
        if self.type_type() == Type::Utf8 {
            self.type_().map(|u| Utf8::init_from_table(u))
        } else {
            None
        }
    }

    #[inline]
    #[allow(non_snake_case)]
    pub fn type_as_bool(&self) -> Option<Bool<'a>> {
        if self.type_type() == Type::Bool {
            self.type_().map(|u| Bool::init_from_table(u))
        } else {
            None
        }
    }

    #[inline]
    #[allow(non_snake_case)]
    pub fn type_as_decimal(&self) -> Option<Decimal<'a>> {
        if self.type_type() == Type::Decimal {
            self.type_().map(|u| Decimal::init_from_table(u))
        } else {
            None
        }
    }

    #[inline]
    #[allow(non_snake_case)]
    pub fn type_as_date(&self) -> Option<Date<'a>> {
        if self.type_type() == Type::Date {
            self.type_().map(|u| Date::init_from_table(u))
        } else {
            None
        }
    }

    #[inline]
    #[allow(non_snake_case)]
    pub fn type_as_time(&self) -> Option<Time<'a>> {
        if self.type_type() == Type::Time {
            self.type_().map(|u| Time::init_from_table(u))
        } else {
            None
        }
    }

    #[inline]
    #[allow(non_snake_case)]
    pub fn type_as_timestamp(&self) -> Option<Timestamp<'a>> {
        if self.type_type() == Type::Timestamp {
            self.type_().map(|u| Timestamp::init_from_table(u))
        } else {
            None
        }
    }

    #[inline]
    #[allow(non_snake_case)]
    pub fn type_as_interval(&self) -> Option<Interval<'a>> {
        if self.type_type() == Type::Interval {
            self.type_().map(|u| Interval::init_from_table(u))
        } else {
            None
        }
    }

    #[inline]
    #[allow(non_snake_case)]
    pub fn type_as_list(&self) -> Option<List<'a>> {
        if self.type_type() == Type::List {
            self.type_().map(|u| List::init_from_table(u))
        } else {
            None
        }
    }

    #[inline]
    #[allow(non_snake_case)]
    pub fn type_as_struct_(&self) -> Option<Struct_<'a>> {
        if self.type_type() == Type::Struct_ {
            self.type_().map(|u| Struct_::init_from_table(u))
        } else {
            None
        }
    }

    #[inline]
    #[allow(non_snake_case)]
    pub fn type_as_union(&self) -> Option<Union<'a>> {
        if self.type_type() == Type::Union {
            self.type_().map(|u| Union::init_from_table(u))
        } else {
            None
        }
    }

    #[inline]
    #[allow(non_snake_case)]
    pub fn type_as_fixed_size_binary(&self) -> Option<FixedSizeBinary<'a>> {
        if self.type_type() == Type::FixedSizeBinary {
            self.type_().map(|u| FixedSizeBinary::init_from_table(u))
        } else {
            None
        }
    }

    #[inline]
    #[allow(non_snake_case)]
    pub fn type_as_fixed_size_list(&self) -> Option<FixedSizeList<'a>> {
        if self.type_type() == Type::FixedSizeList {
            self.type_().map(|u| FixedSizeList::init_from_table(u))
        } else {
            None
        }
    }

    #[inline]
    #[allow(non_snake_case)]
    pub fn type_as_map(&self) -> Option<Map<'a>> {
        if self.type_type() == Type::Map {
            self.type_().map(|u| Map::init_from_table(u))
        } else {
            None
        }
    }

    #[inline]
    #[allow(non_snake_case)]
    pub fn type_as_duration(&self) -> Option<Duration<'a>> {
        if self.type_type() == Type::Duration {
            self.type_().map(|u| Duration::init_from_table(u))
        } else {
            None
        }
    }

    #[inline]
    #[allow(non_snake_case)]
    pub fn type_as_large_binary(&self) -> Option<LargeBinary<'a>> {
        if self.type_type() == Type::LargeBinary {
            self.type_().map(|u| LargeBinary::init_from_table(u))
        } else {
            None
        }
    }

    #[inline]
    #[allow(non_snake_case)]
    pub fn type_as_large_utf_8(&self) -> Option<LargeUtf8<'a>> {
        if self.type_type() == Type::LargeUtf8 {
            self.type_().map(|u| LargeUtf8::init_from_table(u))
        } else {
            None
        }
    }

    #[inline]
    #[allow(non_snake_case)]
    pub fn type_as_large_list(&self) -> Option<LargeList<'a>> {
        if self.type_type() == Type::LargeList {
            self.type_().map(|u| LargeList::init_from_table(u))
        } else {
            None
        }
    }

    #[inline]
    #[allow(non_snake_case)]
    pub fn sparseIndex_as_sparse_tensor_index_coo(
        &self,
    ) -> Option<SparseTensorIndexCOO<'a>> {
        if self.sparseIndex_type() == SparseTensorIndex::SparseTensorIndexCOO {
            self.sparseIndex()
                .map(|u| SparseTensorIndexCOO::init_from_table(u))
        } else {
            None
        }
    }

    #[inline]
    #[allow(non_snake_case)]
    pub fn sparseIndex_as_sparse_matrix_index_csr(
        &self,
    ) -> Option<SparseMatrixIndexCSR<'a>> {
        if self.sparseIndex_type() == SparseTensorIndex::SparseMatrixIndexCSR {
            self.sparseIndex()
                .map(|u| SparseMatrixIndexCSR::init_from_table(u))
        } else {
            None
        }
    }
}

pub struct SparseTensorArgs<'a> {
    pub type_type: Type,
    pub type_: Option<flatbuffers::WIPOffset<flatbuffers::UnionWIPOffset>>,
    pub shape: Option<
        flatbuffers::WIPOffset<
            flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<TensorDim<'a>>>,
        >,
    >,
    pub non_zero_length: i64,
    pub sparseIndex_type: SparseTensorIndex,
    pub sparseIndex: Option<flatbuffers::WIPOffset<flatbuffers::UnionWIPOffset>>,
    pub data: Option<&'a Buffer>,
}
impl<'a> Default for SparseTensorArgs<'a> {
    #[inline]
    fn default() -> Self {
        SparseTensorArgs {
            type_type: Type::NONE,
            type_: None,
            shape: None,
            non_zero_length: 0,
            sparseIndex_type: SparseTensorIndex::NONE,
            sparseIndex: None,
            data: None,
        }
    }
}
pub struct SparseTensorBuilder<'a: 'b, 'b> {
    fbb_: &'b mut flatbuffers::FlatBufferBuilder<'a>,
    start_: flatbuffers::WIPOffset<flatbuffers::TableUnfinishedWIPOffset>,
}
impl<'a: 'b, 'b> SparseTensorBuilder<'a, 'b> {
    #[inline]
    pub fn add_type_type(&mut self, type_type: Type) {
        self.fbb_
            .push_slot::<Type>(SparseTensor::VT_TYPE_TYPE, type_type, Type::NONE);
    }
    #[inline]
    pub fn add_type_(
        &mut self,
        type_: flatbuffers::WIPOffset<flatbuffers::UnionWIPOffset>,
    ) {
        self.fbb_
            .push_slot_always::<flatbuffers::WIPOffset<_>>(SparseTensor::VT_TYPE_, type_);
    }
    #[inline]
    pub fn add_shape(
        &mut self,
        shape: flatbuffers::WIPOffset<
            flatbuffers::Vector<'b, flatbuffers::ForwardsUOffset<TensorDim<'b>>>,
        >,
    ) {
        self.fbb_
            .push_slot_always::<flatbuffers::WIPOffset<_>>(SparseTensor::VT_SHAPE, shape);
    }
    #[inline]
    pub fn add_non_zero_length(&mut self, non_zero_length: i64) {
        self.fbb_
            .push_slot::<i64>(SparseTensor::VT_NON_ZERO_LENGTH, non_zero_length, 0);
    }
    #[inline]
    pub fn add_sparseIndex_type(&mut self, sparseIndex_type: SparseTensorIndex) {
        self.fbb_.push_slot::<SparseTensorIndex>(
            SparseTensor::VT_SPARSEINDEX_TYPE,
            sparseIndex_type,
            SparseTensorIndex::NONE,
        );
    }
    #[inline]
    pub fn add_sparseIndex(
        &mut self,
        sparseIndex: flatbuffers::WIPOffset<flatbuffers::UnionWIPOffset>,
    ) {
        self.fbb_.push_slot_always::<flatbuffers::WIPOffset<_>>(
            SparseTensor::VT_SPARSEINDEX,
            sparseIndex,
        );
    }
    #[inline]
    pub fn add_data(&mut self, data: &'b Buffer) {
        self.fbb_
            .push_slot_always::<&Buffer>(SparseTensor::VT_DATA, data);
    }
    #[inline]
    pub fn new(
        _fbb: &'b mut flatbuffers::FlatBufferBuilder<'a>,
    ) -> SparseTensorBuilder<'a, 'b> {
        let start = _fbb.start_table();
        SparseTensorBuilder {
            fbb_: _fbb,
            start_: start,
        }
    }
    #[inline]
    pub fn finish(self) -> flatbuffers::WIPOffset<SparseTensor<'a>> {
        let o = self.fbb_.end_table(self.start_);
        flatbuffers::WIPOffset::new(o.value())
    }
}

#[inline]
pub fn get_root_as_sparse_tensor<'a>(buf: &'a [u8]) -> SparseTensor<'a> {
    flatbuffers::get_root::<SparseTensor<'a>>(buf)
}

#[inline]
pub fn get_size_prefixed_root_as_sparse_tensor<'a>(buf: &'a [u8]) -> SparseTensor<'a> {
    flatbuffers::get_size_prefixed_root::<SparseTensor<'a>>(buf)
}

#[inline]
pub fn finish_sparse_tensor_buffer<'a, 'b>(
    fbb: &'b mut flatbuffers::FlatBufferBuilder<'a>,
    root: flatbuffers::WIPOffset<SparseTensor<'a>>,
) {
    fbb.finish(root, None);
}

#[inline]
pub fn finish_size_prefixed_sparse_tensor_buffer<'a, 'b>(
    fbb: &'b mut flatbuffers::FlatBufferBuilder<'a>,
    root: flatbuffers::WIPOffset<SparseTensor<'a>>,
) {
    fbb.finish_size_prefixed(root, None);
}
