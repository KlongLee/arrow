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

//! Contains functions and function factories to compare arrays.

use std::cmp::Ordering;

use crate::array::*;
use crate::datatypes::*;
use crate::error::{ArrowError, Result};

use num::Float;

/// Compare the values at two arbitrary indices in two arrays.
pub type DynComparator<'a> = Box<dyn Fn(usize, usize) -> Ordering + 'a>;

/// compares two floats, placing NaNs at last
fn cmp_nans_last<T: Float>(a: &T, b: &T) -> Ordering {
    match (a.is_nan(), b.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        _ => a.partial_cmp(b).unwrap(),
    }
}

fn compare_primitives<'a, T: ArrowNativeType + Ord>(
    left: &'a Array,
    right: &'a Array,
) -> DynComparator<'a> {
    let left = left.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
    let right = right.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
    Box::new(move |i, j| left.value(i).cmp(&right.value(j)))
}

fn compare_boolean<'a>(left: &'a Array, right: &'a Array) -> DynComparator<'a> {
    let left = left.as_any().downcast_ref::<BooleanArray>().unwrap();
    let right = right.as_any().downcast_ref::<BooleanArray>().unwrap();
    Box::new(move |i, j| left.value(i).cmp(&right.value(j)))
}

fn compare_float<'a, T: ArrowNativeType + Float>(
    left: &'a Array,
    right: &'a Array,
) -> DynComparator<'a> {
    let left = left.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
    let right = right.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
    Box::new(move |i, j| cmp_nans_last(&left.value(i), &right.value(j)))
}

fn compare_string<'a, T>(left: &'a Array, right: &'a Array) -> DynComparator<'a>
where
    T: StringOffsetSizeTrait,
{
    let left = left
        .as_any()
        .downcast_ref::<GenericStringArray<T>>()
        .unwrap();
    let right = right
        .as_any()
        .downcast_ref::<GenericStringArray<T>>()
        .unwrap();
    Box::new(move |i, j| left.value(i).cmp(&right.value(j)))
}

fn compare_dict_string<'a, T>(left: &'a Array, right: &'a Array) -> DynComparator<'a>
where
    T: ArrowDictionaryKeyType,
{
    let left = left.as_any().downcast_ref::<DictionaryArray<T>>().unwrap();
    let right = right.as_any().downcast_ref::<DictionaryArray<T>>().unwrap();
    let left_keys = left.keys_array();
    let right_keys = right.keys_array();

    let left_values = StringArray::from(left.values().data());
    let right_values = StringArray::from(left.values().data());

    Box::new(move |i: usize, j: usize| {
        let key_left = left_keys.value(i).to_usize().unwrap();
        let key_right = right_keys.value(j).to_usize().unwrap();
        let left = left_values.value(key_left);
        let right = right_values.value(key_right);
        left.cmp(&right)
    })
}

/// returns a comparison function that compares two values at two different positions
/// between the two arrays.
/// The arrays' types must be equal.
/// # Example
/// ```
/// use arrow::array::{build_compare, Int32Array};
///
/// # fn main() -> arrow::error::Result<()> {
/// let array1 = Int32Array::from(vec![1, 2]);
/// let array2 = Int32Array::from(vec![3, 4]);
///
/// let cmp = build_compare(&array1, &array2)?;
///
/// // 1 (index 0 of array1) is smaller than 4 (index 1 of array2)
/// assert_eq!(std::cmp::Ordering::Less, (cmp)(0, 1));
/// # Ok(())
/// # }
/// ```
// This is a factory of comparisons.
// The lifetime 'a enforces that we cannot use the closure beyond any of the array's lifetime.
pub fn build_compare<'a>(left: &'a Array, right: &'a Array) -> Result<DynComparator<'a>> {
    use DataType::*;
    use IntervalUnit::*;
    Ok(match (left.data_type(), right.data_type()) {
        (a, b) if a != b => {
            return Err(ArrowError::InvalidArgumentError(
                "Can't compare arrays of different types".to_string(),
            ));
        }
        (Boolean, Boolean) => compare_boolean(left, right),
        (UInt8, UInt8) => compare_primitives::<u8>(left, right),
        (UInt16, UInt16) => compare_primitives::<u16>(left, right),
        (UInt32, UInt32) => compare_primitives::<u32>(left, right),
        (UInt64, UInt64) => compare_primitives::<u64>(left, right),
        (Int8, Int8) => compare_primitives::<i8>(left, right),
        (Int16, Int16) => compare_primitives::<i16>(left, right),
        (Int32, Int32) => compare_primitives::<i32>(left, right),
        (Int64, Int64) => compare_primitives::<i64>(left, right),
        (Float32, Float32) => compare_float::<f32>(left, right),
        (Float64, Float64) => compare_float::<f64>(left, right),
        (Date32, Date32) => compare_primitives::<i32>(left, right),
        (Date64, Date64) => compare_primitives::<i64>(left, right),
        (Time32(_), Time32(_)) => compare_primitives::<i32>(left, right),
        (Time32(_), Time32(_)) => compare_primitives::<i32>(left, right),
        (Time64(_), Time64(_)) => compare_primitives::<i64>(left, right),
        (Timestamp(_, None), Timestamp(_, None)) => {
            compare_primitives::<i64>(left, right)
        }
        (Interval(YearMonth), Interval(YearMonth)) => {
            compare_primitives::<i32>(left, right)
        }
        (Interval(DayTime), Interval(DayTime)) => compare_primitives::<i64>(left, right),
        (Duration(_), Duration(_)) => compare_primitives::<i64>(left, right),
        (Utf8, Utf8) => compare_string::<i32>(left, right),
        (LargeUtf8, LargeUtf8) => compare_string::<i64>(left, right),
        (
            Dictionary(key_type_lhs, value_type_lhs),
            Dictionary(key_type_rhs, value_type_rhs),
        ) => {
            if value_type_lhs.as_ref() != &DataType::Utf8
                || value_type_rhs.as_ref() != &DataType::Utf8
            {
                return Err(ArrowError::InvalidArgumentError(
                    "Arrow still does not support comparisons of non-string dictionary arrays"
                        .to_string(),
                ));
            }
            match (key_type_lhs.as_ref(), key_type_rhs.as_ref()) {
                (a, b) if a != b => {
                    return Err(ArrowError::InvalidArgumentError(
                        "Can't compare arrays of different types".to_string(),
                    ));
                }
                (UInt8, UInt8) => compare_dict_string::<u8>(left, right),
                (UInt16, UInt16) => compare_dict_string::<u16>(left, right),
                (UInt32, UInt32) => compare_dict_string::<u32>(left, right),
                (UInt64, UInt64) => compare_dict_string::<u64>(left, right),
                (Int8, Int8) => compare_dict_string::<i8>(left, right),
                (Int16, Int16) => compare_dict_string::<i16>(left, right),
                (Int32, Int32) => compare_dict_string::<i32>(left, right),
                (Int64, Int64) => compare_dict_string::<i64>(left, right),
                (lhs, _) => {
                    return Err(ArrowError::InvalidArgumentError(format!(
                        "Dictionaries do not support keys of type {:?}",
                        lhs
                    )))
                }
            }
        }
        (lhs, _) => {
            return Err(ArrowError::InvalidArgumentError(format!(
                "The data type type {:?} has no natural order",
                lhs
            )))
        }
    })
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::array::{Float64Array, Int32Array};
    use crate::error::Result;
    use std::cmp::Ordering;
    use std::iter::FromIterator;

    #[test]
    fn test_i32() -> Result<()> {
        let array = Int32Array::from(vec![1, 2]);

        let cmp = build_compare(&array, &array)?;

        assert_eq!(Ordering::Less, (cmp)(0, 1));
        Ok(())
    }

    #[test]
    fn test_i32_i32() -> Result<()> {
        let array1 = Int32Array::from(vec![1]);
        let array2 = Int32Array::from(vec![2]);

        let cmp = build_compare(&array1, &array2)?;

        assert_eq!(Ordering::Less, (cmp)(0, 0));
        Ok(())
    }

    #[test]
    fn test_f64() -> Result<()> {
        let array = Float64Array::from(vec![1.0, 2.0]);

        let cmp = build_compare(&array, &array)?;

        assert_eq!(Ordering::Less, (cmp)(0, 1));
        Ok(())
    }

    #[test]
    fn test_f64_nan() -> Result<()> {
        let array = Float64Array::from(vec![1.0, f64::NAN]);

        let cmp = build_compare(&array, &array)?;

        assert_eq!(Ordering::Less, (cmp)(0, 1));
        Ok(())
    }

    #[test]
    fn test_f64_zeros() -> Result<()> {
        let array = Float64Array::from(vec![-0.0, 0.0]);

        let cmp = build_compare(&array, &array)?;

        assert_eq!(Ordering::Equal, (cmp)(0, 1));
        assert_eq!(Ordering::Equal, (cmp)(1, 0));
        Ok(())
    }

    #[test]
    fn test_dict() -> Result<()> {
        let data = vec!["a", "b", "c", "a", "a", "c", "c"];
        let array = DictionaryArray::<i16>::from_iter(data.into_iter());

        let cmp = build_compare(&array, &array)?;

        assert_eq!(Ordering::Less, (cmp)(0, 1));
        assert_eq!(Ordering::Equal, (cmp)(3, 4));
        assert_eq!(Ordering::Greater, (cmp)(2, 3));
        Ok(())
    }
}
