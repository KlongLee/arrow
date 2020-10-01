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

//! This module contains an implementation of a contiguous immutable memory region that knows
//! how to de-allocate itself, [`Bytes`].
//! Note that this is a low-level functionality of this crate, and is only required to be used
//! when implementing FFI.

use core::slice;
use std::{fmt::Debug, fmt::Formatter, sync::Arc};

use crate::memory;

/// function resposible for de-allocating `Bytes`.
pub type DropFn = Arc<dyn Fn(&mut Bytes)>;

/// Mode of deallocating memory regions
pub enum Deallocation {
    /// Native deallocation, using Rust deallocator with Arrow-specific memory aligment
    Native(usize),
    /// Foreign deallocation, using some other form of memory deallocation
    Foreign(DropFn),
}

impl Debug for Deallocation {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            Deallocation::Native(capacity) => {
                write!(f, "Deallocation::Native {{ capacity: {} }}", capacity)
            }
            Deallocation::Foreign(_) => {
                write!(f, "Deallocation::Foreign {{ capacity: unknown }}")
            }
        }
    }
}

/// A continuous, fixed-size, immutable memory region that knows how to de-allocate itself.
/// This structs' API is inspired by the `bytes::Bytes`, but it is not limited to using rust's
/// global allocator nor u8 aligmnent.
///
/// In the most common case, this buffer is allocated using [`allocate_aligned`](memory::allocate_aligned)
/// and deallocated accordingly [`free_aligned`](memory::free_aligned).
/// When the region is allocated by an foreign allocator, [Deallocation::Foreign], this calls the
/// foreign deallocator to deallocate the region when it is no longer needed.
pub struct Bytes {
    /// The raw pointer to be begining of the region
    ptr: *const u8,

    /// The number of bytes visible to this region. This is always smaller than its capacity (when avaliable).
    len: usize,

    /// how to deallocate this region
    deallocation: Deallocation,
}

impl Bytes {
    /// Takes ownership of an allocated memory region,
    ///
    /// # Arguments
    ///
    /// * `ptr` - Pointer to raw parts
    /// * `len` - Length of raw parts in **bytes**
    /// * `capacity` - Total allocated memory for the pointer `ptr`, in **bytes**
    ///
    /// # Safety
    ///
    /// This function is unsafe as there is no guarantee that the given pointer is valid for `len`
    /// bytes. If the `ptr` and `capacity` come from a `Buffer`, then this is guaranteed.
    pub unsafe fn new(ptr: *const u8, len: usize, deallocation: Deallocation) -> Bytes {
        Bytes {
            ptr,
            len,
            deallocation,
        }
    }

    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.ptr, self.len) }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline]
    pub fn raw_data(&self) -> *const u8 {
        self.ptr
    }

    #[inline]
    pub fn raw_data_mut(&mut self) -> *mut u8 {
        self.ptr as *mut u8
    }

    pub fn capacity(&self) -> usize {
        match self.deallocation {
            Deallocation::Native(capacity) => capacity,
            // we cannot determine this in general,
            // and thus we state that this is externally-owned memory
            Deallocation::Foreign(_) => 0,
        }
    }
}

impl Drop for Bytes {
    #[inline]
    fn drop(&mut self) {
        match &self.deallocation {
            Deallocation::Native(capacity) => {
                if !self.ptr.is_null() {
                    unsafe { memory::free_aligned(self.ptr as *mut u8, *capacity) };
                }
            }
            Deallocation::Foreign(drop) => {
                (drop.clone())(self);
            }
        }
    }
}

impl PartialEq for Bytes {
    fn eq(&self, other: &Bytes) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl Debug for Bytes {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "Bytes {{ ptr: {:?}, len: {}, data: ", self.ptr, self.len,)?;

        f.debug_list().entries(self.as_slice().iter()).finish()?;

        write!(f, " }}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dealloc() {
        let a = Box::new(b"hello");

        let dealloc = Arc::new(|bytes: &mut Bytes| {
            // println!(""); seems to be the only way to validate that this is actually called, as this
            // is an immutable FFI call
            assert_eq!(bytes.as_slice(), &b"hello"[1..4]);
        });

        let b = unsafe { Bytes::new(a[1..].as_ptr(), 3, Deallocation::Foreign(dealloc)) };
        drop(b);
        // the content is still valid (as the dealloc above does not actually free it)
        assert_eq!(a.as_ref(), &b"hello");
    }

    #[test]
    fn test_dealloc_native() {
        let capacity = 5;
        let a = memory::allocate_aligned(capacity);
        // create Bytes and release it. This will make `a` be an invalid pointer, but it is defined behavior
        unsafe { Bytes::new(a, 3, Deallocation::Native(capacity)) };
    }
}
