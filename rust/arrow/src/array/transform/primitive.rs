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

use std::mem::size_of;

use crate::{array::ArrayData, datatypes::ArrowNativeType};

use super::{Extend, _MutableArrayData};

pub(super) fn build_extend<T: ArrowNativeType>(array: &ArrayData) -> Extend {
    let values = &array.buffers()[0].data()[array.offset() * size_of::<T>()..];
    Box::new(
        move |mutable: &mut _MutableArrayData, start: usize, len: usize| {
            let start = start * size_of::<T>();
            let len = len * size_of::<T>();
            let bytes = &values[start..start + len];
            let buffer = &mut mutable.buffers[0];
            buffer.extend_from_slice(bytes);
        },
    )
}

pub(super) fn extend_nulls<T: ArrowNativeType>(
    mutable: &mut _MutableArrayData,
    len: usize,
) {
    let buffer = &mut mutable.buffers[0];
    buffer.extend(len * size_of::<T>());
}

pub(super) fn push_null<T: ArrowNativeType>(mutable: &mut _MutableArrayData) {
    let buffer = &mut mutable.buffers[0];
    buffer.extend(size_of::<T>());
}
