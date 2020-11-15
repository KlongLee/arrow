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

//! Contains `ArrayData`, a generic representation of Arrow array data which encapsulates
//! common attributes and operations for Arrow array.

use std::mem;
use std::sync::Arc;

use crate::buffer::Buffer;
use crate::datatypes::DataType;
use crate::{bitmap::Bitmap, datatypes::ArrowNativeType};

use super::equal::equal;

#[inline]
fn count_nulls(null_bit_buffer: Option<&Buffer>, offset: usize, len: usize) -> usize {
    if let Some(ref buf) = null_bit_buffer {
        let ones = buf.bit_slice().view(offset, len).count_ones();
        len.checked_sub(ones).unwrap()
    } else {
        0
    }
}

/// An generic representation of Arrow array data which encapsulates common attributes and
/// operations for Arrow array. Specific operations for different arrays types (e.g.,
/// primitive, list, struct) are implemented in `Array`.
#[derive(Debug, Clone)]
pub struct ArrayData {
    /// The data type for this array data
    data_type: DataType,

    /// The number of elements in this array data
    len: usize,

    /// The number of null elements in this array data
    null_count: usize,

    /// The offset into this array data, in number of items
    offset: usize,

    /// The buffers for this array data. Note that depending on the array types, this
    /// could hold different kinds of buffers (e.g., value buffer, value offset buffer)
    /// at different positions.
    buffers: Vec<Buffer>,

    /// The child(ren) of this array. Only non-empty for nested types, currently
    /// `ListArray` and `StructArray`.
    child_data: Vec<ArrayDataRef>,

    /// The null bitmap. A `None` value for this indicates all values are non-null in
    /// this array.
    null_bitmap: Option<Bitmap>,
}

pub type ArrayDataRef = Arc<ArrayData>;

impl ArrayData {
    pub fn new(
        data_type: DataType,
        len: usize,
        null_count: Option<usize>,
        null_bit_buffer: Option<Buffer>,
        offset: usize,
        buffers: Vec<Buffer>,
        child_data: Vec<ArrayDataRef>,
    ) -> Self {
        let null_count = match null_count {
            None => count_nulls(null_bit_buffer.as_ref(), offset, len),
            Some(null_count) => null_count,
        };
        let null_bitmap = null_bit_buffer.map(Bitmap::from);
        Self {
            data_type,
            len,
            null_count,
            offset,
            buffers,
            child_data,
            null_bitmap,
        }
    }

    /// Returns a builder to construct a `ArrayData` instance.
    #[inline]
    pub const fn builder(data_type: DataType) -> ArrayDataBuilder {
        ArrayDataBuilder::new(data_type)
    }

    /// Returns a reference to the data type of this array data
    #[inline]
    pub const fn data_type(&self) -> &DataType {
        &self.data_type
    }

    /// Returns a slice of buffers for this array data
    pub fn buffers(&self) -> &[Buffer] {
        &self.buffers[..]
    }

    /// Returns a slice of children data arrays
    pub fn child_data(&self) -> &[ArrayDataRef] {
        &self.child_data[..]
    }

    /// Returns whether the element at index `i` is null
    pub fn is_null(&self, i: usize) -> bool {
        if let Some(ref b) = self.null_bitmap {
            return !b.is_set(self.offset + i);
        }
        false
    }

    /// Returns a reference to the null bitmap of this array data
    #[inline]
    pub const fn null_bitmap(&self) -> &Option<Bitmap> {
        &self.null_bitmap
    }

    /// Returns a reference to the null buffer of this array data.
    pub fn null_buffer(&self) -> Option<&Buffer> {
        self.null_bitmap().as_ref().map(|b| b.buffer_ref())
    }

    /// Returns whether the element at index `i` is not null
    pub fn is_valid(&self, i: usize) -> bool {
        if let Some(ref b) = self.null_bitmap {
            return b.is_set(self.offset + i);
        }
        true
    }

    /// Returns the length (i.e., number of elements) of this array
    #[inline]
    pub const fn len(&self) -> usize {
        self.len
    }

    // Returns whether array data is empty
    #[inline]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the offset of this array
    #[inline]
    pub const fn offset(&self) -> usize {
        self.offset
    }

    /// Returns the total number of nulls in this array
    #[inline]
    pub const fn null_count(&self) -> usize {
        self.null_count
    }

    /// Returns the total number of bytes of memory occupied by the buffers owned by this [ArrayData].
    pub fn get_buffer_memory_size(&self) -> usize {
        let mut size = 0;
        for buffer in &self.buffers {
            size += buffer.capacity();
        }
        if let Some(bitmap) = &self.null_bitmap {
            size += bitmap.get_buffer_memory_size()
        }
        for child in &self.child_data {
            size += child.get_buffer_memory_size();
        }
        size
    }

    /// Returns the total number of bytes of memory occupied physically by this [ArrayData].
    pub fn get_array_memory_size(&self) -> usize {
        let mut size = 0;
        // Calculate size of the fields that don't have [get_array_memory_size] method internally.
        size += mem::size_of_val(self)
            - mem::size_of_val(&self.buffers)
            - mem::size_of_val(&self.null_bitmap)
            - mem::size_of_val(&self.child_data);

        // Calculate rest of the fields top down which contain actual data
        for buffer in &self.buffers {
            size += mem::size_of_val(&buffer);
            size += buffer.capacity();
        }
        if let Some(bitmap) = &self.null_bitmap {
            size += bitmap.get_array_memory_size()
        }
        for child in &self.child_data {
            size += child.get_array_memory_size();
        }

        size
    }

    /// Creates a zero-copy slice of itself. This creates a new [ArrayData]
    /// with a different offset, len and a shifted null bitmap.
    ///
    /// # Panics
    ///
    /// Panics if `offset + length > self.len()`.
    pub fn slice(&self, offset: usize, length: usize) -> ArrayData {
        assert!((offset + length) <= self.len());

        let mut new_data = self.clone();

        new_data.len = length;
        new_data.offset = offset + self.offset;

        new_data.null_count =
            count_nulls(new_data.null_buffer(), new_data.offset, new_data.len);

        new_data
    }

    /// Returns the `buffer` as a slice of type `T` starting at self.offset
    /// # Panics
    /// This function panics if:
    /// * the buffer is not byte-aligned with type T, or
    /// * the datatype is `Boolean` (it corresponds to a bit-packed buffer where the offset is not applicable)
    #[inline]
    pub(super) fn buffer<T: ArrowNativeType>(&self, buffer: usize) -> &[T] {
        let values = unsafe { self.buffers[buffer].data().align_to::<T>() };
        if values.0.len() != 0 || values.2.len() != 0 {
            panic!("The buffer is not byte-aligned with its interpretation")
        };
        assert_ne!(self.data_type, DataType::Boolean);
        &values.1[self.offset..]
    }
}

impl PartialEq for ArrayData {
    fn eq(&self, other: &Self) -> bool {
        equal(self, other)
    }
}

/// Builder for `ArrayData` type
#[derive(Debug)]
pub struct ArrayDataBuilder {
    data_type: DataType,
    len: usize,
    null_count: Option<usize>,
    null_bit_buffer: Option<Buffer>,
    offset: usize,
    buffers: Vec<Buffer>,
    child_data: Vec<ArrayDataRef>,
}

impl ArrayDataBuilder {
    #[inline]
    pub const fn new(data_type: DataType) -> Self {
        Self {
            data_type,
            len: 0,
            null_count: None,
            null_bit_buffer: None,
            offset: 0,
            buffers: vec![],
            child_data: vec![],
        }
    }

    #[inline]
    pub const fn len(mut self, n: usize) -> Self {
        self.len = n;
        self
    }

    #[inline]
    pub const fn null_count(mut self, n: usize) -> Self {
        self.null_count = Some(n);
        self
    }

    pub fn null_bit_buffer(mut self, buf: Buffer) -> Self {
        self.null_bit_buffer = Some(buf);
        self
    }

    #[inline]
    pub const fn offset(mut self, n: usize) -> Self {
        self.offset = n;
        self
    }

    pub fn buffers(mut self, v: Vec<Buffer>) -> Self {
        self.buffers = v;
        self
    }

    pub fn add_buffer(mut self, b: Buffer) -> Self {
        self.buffers.push(b);
        self
    }

    pub fn child_data(mut self, v: Vec<ArrayDataRef>) -> Self {
        self.child_data = v;
        self
    }

    pub fn add_child_data(mut self, r: ArrayDataRef) -> Self {
        self.child_data.push(r);
        self
    }

    pub fn build(self) -> ArrayDataRef {
        let data = ArrayData::new(
            self.data_type,
            self.len,
            self.null_count,
            self.null_bit_buffer,
            self.offset,
            self.buffers,
            self.child_data,
        );
        Arc::new(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use crate::buffer::Buffer;
    use crate::datatypes::ToByteSlice;
    use crate::util::bit_ops::BufferBitSliceMut;

    #[test]
    fn test_new() {
        let arr_data =
            ArrayData::new(DataType::Boolean, 10, Some(1), None, 2, vec![], vec![]);
        assert_eq!(10, arr_data.len());
        assert_eq!(1, arr_data.null_count());
        assert_eq!(2, arr_data.offset());
        assert_eq!(0, arr_data.buffers().len());
        assert_eq!(0, arr_data.child_data().len());
    }

    #[test]
    fn test_builder() {
        let child_arr_data = Arc::new(ArrayData::new(
            DataType::Int32,
            5,
            Some(0),
            None,
            0,
            vec![Buffer::from([1i32, 2, 3, 4, 5].to_byte_slice())],
            vec![],
        ));
        let v = vec![0, 1, 2, 3];
        let b1 = Buffer::from(&v[..]);
        let arr_data = ArrayData::builder(DataType::Int32)
            .len(20)
            .null_count(10)
            .offset(5)
            .add_buffer(b1)
            .add_child_data(child_arr_data.clone())
            .build();

        assert_eq!(20, arr_data.len());
        assert_eq!(10, arr_data.null_count());
        assert_eq!(5, arr_data.offset());
        assert_eq!(1, arr_data.buffers().len());
        assert_eq!(&[0, 1, 2, 3], arr_data.buffers()[0].data());
        assert_eq!(1, arr_data.child_data().len());
        assert_eq!(child_arr_data, arr_data.child_data()[0]);
    }

    #[test]
    fn test_null_count() {
        let mut bit_v: [u8; 2] = [0; 2];
        let mut bit_v_slice = BufferBitSliceMut::new(&mut bit_v);
        bit_v_slice.set_bit(0, true);
        bit_v_slice.set_bit(3, true);
        bit_v_slice.set_bit(10, true);
        let arr_data = ArrayData::builder(DataType::Int32)
            .len(16)
            .null_bit_buffer(Buffer::from(bit_v))
            .build();
        assert_eq!(13, arr_data.null_count());

        // Test with offset
        let mut bit_v: [u8; 2] = [0; 2];
        let mut bit_v_slice = BufferBitSliceMut::new(&mut bit_v);
        bit_v_slice.set_bit(0, true);
        bit_v_slice.set_bit(3, true);
        bit_v_slice.set_bit(10, true);
        let arr_data = ArrayData::builder(DataType::Int32)
            .len(12)
            .offset(2)
            .null_bit_buffer(Buffer::from(bit_v))
            .build();
        assert_eq!(10, arr_data.null_count());
    }

    #[test]
    fn test_null_buffer_ref() {
        let mut bit_v: [u8; 2] = [0; 2];
        let mut bit_v_slice = BufferBitSliceMut::new(&mut bit_v);
        bit_v_slice.set_bit(0, true);
        bit_v_slice.set_bit(3, true);
        bit_v_slice.set_bit(10, true);
        let arr_data = ArrayData::builder(DataType::Int32)
            .len(16)
            .null_bit_buffer(Buffer::from(bit_v))
            .build();
        assert!(arr_data.null_buffer().is_some());
        assert_eq!(&bit_v, arr_data.null_buffer().unwrap().data());
    }

    #[test]
    fn test_slice() {
        let mut bit_v: [u8; 2] = [0; 2];
        let mut bit_v_slice = BufferBitSliceMut::new(&mut bit_v);
        bit_v_slice.set_bit(0, true);
        bit_v_slice.set_bit(3, true);
        bit_v_slice.set_bit(10, true);
        let data = ArrayData::builder(DataType::Int32)
            .len(16)
            .null_bit_buffer(Buffer::from(bit_v))
            .build();
        let data = data.as_ref();
        let new_data = data.slice(1, 15);
        assert_eq!(data.len() - 1, new_data.len());
        assert_eq!(1, new_data.offset());
        assert_eq!(data.null_count(), new_data.null_count());

        // slice of a slice (removes one null)
        let new_data = new_data.slice(1, 14);
        assert_eq!(data.len() - 2, new_data.len());
        assert_eq!(2, new_data.offset());
        assert_eq!(data.null_count() - 1, new_data.null_count());
    }

    #[test]
    fn test_equality() {
        let int_data = ArrayData::builder(DataType::Int32).build();
        let float_data = ArrayData::builder(DataType::Float32).build();
        assert_ne!(int_data, float_data);
    }
}
