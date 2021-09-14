# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# cython: language_level = 3

import sys

from cython.operator cimport dereference as deref

from collections import namedtuple

from pyarrow.lib import frombytes, tobytes, ordered_dict
from pyarrow.lib cimport *
from pyarrow.includes.libarrow cimport *
import pyarrow.lib as lib

import numpy as np


cdef wrap_scalar_function(const shared_ptr[CFunction]& sp_func):
    """
    Wrap a C++ scalar Function in a ScalarFunction object.
    """
    cdef ScalarFunction func = ScalarFunction.__new__(ScalarFunction)
    func.init(sp_func)
    return func


cdef wrap_vector_function(const shared_ptr[CFunction]& sp_func):
    """
    Wrap a C++ vector Function in a VectorFunction object.
    """
    cdef VectorFunction func = VectorFunction.__new__(VectorFunction)
    func.init(sp_func)
    return func


cdef wrap_scalar_aggregate_function(const shared_ptr[CFunction]& sp_func):
    """
    Wrap a C++ aggregate Function in a ScalarAggregateFunction object.
    """
    cdef ScalarAggregateFunction func = \
        ScalarAggregateFunction.__new__(ScalarAggregateFunction)
    func.init(sp_func)
    return func


cdef wrap_hash_aggregate_function(const shared_ptr[CFunction]& sp_func):
    """
    Wrap a C++ aggregate Function in a HashAggregateFunction object.
    """
    cdef HashAggregateFunction func = \
        HashAggregateFunction.__new__(HashAggregateFunction)
    func.init(sp_func)
    return func


cdef wrap_meta_function(const shared_ptr[CFunction]& sp_func):
    """
    Wrap a C++ meta Function in a MetaFunction object.
    """
    cdef MetaFunction func = MetaFunction.__new__(MetaFunction)
    func.init(sp_func)
    return func


cdef wrap_function(const shared_ptr[CFunction]& sp_func):
    """
    Wrap a C++ Function in a Function object.

    This dispatches to specialized wrappers depending on the function kind.
    """
    if sp_func.get() == NULL:
        raise ValueError("Function was NULL")

    kind_dict = {
        FunctionKind_SCALAR: wrap_scalar_function,
        FunctionKind_VECTOR: wrap_vector_function,
        FunctionKind_SCALAR_AGGREGATE: wrap_scalar_aggregate_function,
        FunctionKind_HASH_AGGREGATE: wrap_hash_aggregate_function,
        FunctionKind_META: wrap_meta_function,
    }

    cdef FunctionKind c_kind = sp_func.get().kind()
    if c_kind not in kind_dict:
        raise NotImplementedError("Unknown Function::Kind")
    return kind_dict[c_kind](sp_func)


cdef wrap_scalar_kernel(const CScalarKernel* c_kernel):
    if c_kernel == NULL:
        raise ValueError("Kernel was NULL")
    cdef ScalarKernel kernel = ScalarKernel.__new__(ScalarKernel)
    kernel.init(c_kernel)
    return kernel


cdef wrap_vector_kernel(const CVectorKernel* c_kernel):
    if c_kernel == NULL:
        raise ValueError("Kernel was NULL")
    cdef VectorKernel kernel = VectorKernel.__new__(VectorKernel)
    kernel.init(c_kernel)
    return kernel


cdef wrap_scalar_aggregate_kernel(const CScalarAggregateKernel* c_kernel):
    if c_kernel == NULL:
        raise ValueError("Kernel was NULL")
    cdef ScalarAggregateKernel kernel = \
        ScalarAggregateKernel.__new__(ScalarAggregateKernel)
    kernel.init(c_kernel)
    return kernel


cdef wrap_hash_aggregate_kernel(const CHashAggregateKernel* c_kernel):
    if c_kernel == NULL:
        raise ValueError("Kernel was NULL")
    cdef HashAggregateKernel kernel = \
        HashAggregateKernel.__new__(HashAggregateKernel)
    kernel.init(c_kernel)
    return kernel


cdef class Kernel(_Weakrefable):
    """
    A kernel object.

    Kernels handle the execution of a Function for a certain signature.
    """

    def __init__(self):
        raise TypeError("Do not call {}'s constructor directly"
                        .format(self.__class__.__name__))


cdef class ScalarKernel(Kernel):
    cdef:
        const CScalarKernel* kernel

    cdef void init(self, const CScalarKernel* kernel) except *:
        self.kernel = kernel

    def __repr__(self):
        return ("ScalarKernel<{}>"
                .format(frombytes(self.kernel.signature.get().ToString())))


cdef class VectorKernel(Kernel):
    cdef:
        const CVectorKernel* kernel

    cdef void init(self, const CVectorKernel* kernel) except *:
        self.kernel = kernel

    def __repr__(self):
        return ("VectorKernel<{}>"
                .format(frombytes(self.kernel.signature.get().ToString())))


cdef class ScalarAggregateKernel(Kernel):
    cdef:
        const CScalarAggregateKernel* kernel

    cdef void init(self, const CScalarAggregateKernel* kernel) except *:
        self.kernel = kernel

    def __repr__(self):
        return ("ScalarAggregateKernel<{}>"
                .format(frombytes(self.kernel.signature.get().ToString())))


cdef class HashAggregateKernel(Kernel):
    cdef:
        const CHashAggregateKernel* kernel

    cdef void init(self, const CHashAggregateKernel* kernel) except *:
        self.kernel = kernel

    def __repr__(self):
        return ("HashAggregateKernel<{}>"
                .format(frombytes(self.kernel.signature.get().ToString())))


FunctionDoc = namedtuple(
    "FunctionDoc",
    ("summary", "description", "arg_names", "options_class"))


cdef class Function(_Weakrefable):
    """
    A compute function.

    A function implements a certain logical computation over a range of
    possible input signatures.  Each signature accepts a range of input
    types and is implemented by a given Kernel.

    Functions can be of different kinds:

    * "scalar" functions apply an item-wise computation over all items
      of their inputs.  Each item in the output only depends on the values
      of the inputs at the same position.  Examples: addition, comparisons,
      string predicates...

    * "vector" functions apply a collection-wise computation, such that
      each item in the output may depend on the values of several items
      in each input.  Examples: dictionary encoding, sorting, extracting
      unique values...

    * "scalar_aggregate" functions reduce the dimensionality of the inputs by
      applying a reduction function.  Examples: sum, min_max, mode...

    * "hash_aggregate" functions apply a reduction function to an input
      subdivided by grouping criteria.  They may not be directly called.
      Examples: hash_sum, hash_min_max...

    * "meta" functions dispatch to other functions.
    """

    cdef:
        shared_ptr[CFunction] sp_func
        CFunction* base_func

    kind_dict = {
        FunctionKind_SCALAR: "scalar",
        FunctionKind_VECTOR: "vector",
        FunctionKind_SCALAR_AGGREGATE: "scalar_aggregate",
        FunctionKind_HASH_AGGREGATE: "hash_aggregate",
        FunctionKind_META: "meta",
    }

    def __init__(self):
        raise TypeError("Do not call {}'s constructor directly"
                        .format(self.__class__.__name__))

    cdef void init(self, const shared_ptr[CFunction]& sp_func) except *:
        self.sp_func = sp_func
        self.base_func = sp_func.get()

    def __repr__(self):
        return ("arrow.compute.Function<name={}, kind={}, "
                "arity={}, num_kernels={}>"
                ).format(self.name, self.kind, self.arity, self.num_kernels)

    def __reduce__(self):
        # Reduction uses the global registry
        return get_function, (self.name,)

    @property
    def name(self):
        """
        The function name.
        """
        return frombytes(self.base_func.name())

    @property
    def arity(self):
        """
        The function arity.

        If Ellipsis (i.e. `...`) is returned, the function takes a variable
        number of arguments.
        """
        cdef CArity arity = self.base_func.arity()
        if arity.is_varargs:
            return ...
        else:
            return arity.num_args

    @property
    def kind(self):
        """
        The function kind.
        """
        cdef FunctionKind c_kind = self.base_func.kind()
        if c_kind not in kind_dict:
            raise NotImplementedError("Unknown Function::Kind")
        return kind_dict[c_kind]

    @property
    def _doc(self):
        """
        The C++-like function documentation (for internal use).
        """
        cdef CFunctionDoc c_doc = self.base_func.doc()

        return FunctionDoc(frombytes(c_doc.summary),
                           frombytes(c_doc.description),
                           [frombytes(s) for s in c_doc.arg_names],
                           frombytes(c_doc.options_class))

    @property
    def num_kernels(self):
        """
        The number of kernels implementing this function.
        """
        return self.base_func.num_kernels()

    def call(self, args, FunctionOptions options=None,
             MemoryPool memory_pool=None):
        """
        Call the function on the given arguments.
        """
        cdef:
            const CFunctionOptions* c_options = NULL
            CMemoryPool* pool = maybe_unbox_memory_pool(memory_pool)
            CExecContext c_exec_ctx = CExecContext(pool)
            vector[CDatum] c_args
            CDatum result

        _pack_compute_args(args, &c_args)

        if options is not None:
            c_options = options.get_options()

        with nogil:
            result = GetResultValue(self.base_func.Execute(c_args, c_options,
                                                           &c_exec_ctx))

        return wrap_datum(result)


cdef class ScalarFunction(Function):
    cdef:
        const CScalarFunction* func

    cdef void init(self, const shared_ptr[CFunction]& sp_func) except *:
        Function.init(self, sp_func)
        self.func = <const CScalarFunction*> sp_func.get()

    @property
    def kernels(self):
        """
        The kernels implementing this function.
        """
        cdef vector[const CScalarKernel*] kernels = self.func.kernels()
        return [wrap_scalar_kernel(k) for k in kernels]


cdef class VectorFunction(Function):
    cdef:
        const CVectorFunction* func

    cdef void init(self, const shared_ptr[CFunction]& sp_func) except *:
        Function.init(self, sp_func)
        self.func = <const CVectorFunction*> sp_func.get()

    @property
    def kernels(self):
        """
        The kernels implementing this function.
        """
        cdef vector[const CVectorKernel*] kernels = self.func.kernels()
        return [wrap_vector_kernel(k) for k in kernels]


cdef class ScalarAggregateFunction(Fdunction):
    cdef:
        const CScalarAggregateFunction* func

    cdef void init(self, const shared_ptr[CFunction]& sp_func) except *:
        Function.init(self, sp_func)
        self.func = <const CScalarAggregateFunction*> sp_func.get()

    @property
    def kernels(self):
        """
        The kernels implementing this function.
        """
        cdef vector[const CScalarAggregateKernel*] kernels = \
            self.func.kernels()
        return [wrap_scalar_aggregate_kernel(k) for k in kernels]


cdef class HashAggregateFunction(Function):
    cdef:
        const CHashAggregateFunction* func

    cdef void init(self, const shared_ptr[CFunction]& sp_func) except *:
        Function.init(self, sp_func)
        self.func = <const CHashAggregateFunction*> sp_func.get()

    @property
    def kernels(self):
        """
        The kernels implementing this function.
        """
        cdef vector[const CHashAggregateKernel*] kernels = self.func.kernels()
        return [wrap_hash_aggregate_kernel(k) for k in kernels]


cdef class MetaFunction(Function):
    cdef:
        const CMetaFunction* func

    cdef void init(self, const shared_ptr[CFunction]& sp_func) except *:
        Function.init(self, sp_func)
        self.func = <const CMetaFunction*> sp_func.get()

    # Since num_kernels is exposed, also expose a kernels property
    @property
    def kernels(self):
        """
        The kernels implementing this function.
        """
        return []


cdef _pack_compute_args(object values, vector[CDatum]* out):
    for val in values:
        if isinstance(val, (list, np.ndarray)):
            val = lib.asarray(val)

        if isinstance(val, Array):
            out.push_back(CDatum((<Array> val).sp_array))
            continue
        elif isinstance(val, ChunkedArray):
            out.push_back(CDatum((<ChunkedArray> val).sp_chunked_array))
            continue
        elif isinstance(val, Scalar):
            out.push_back(CDatum((<Scalar> val).unwrap()))
            continue
        elif isinstance(val, RecordBatch):
            out.push_back(CDatum((<RecordBatch> val).sp_batch))
            continue
        elif isinstance(val, Table):
            out.push_back(CDatum((<Table> val).sp_table))
            continue
        else:
            # Is it a Python scalar?
            try:
                scal = lib.scalar(val)
            except Exception:
                # Raise dedicated error below
                pass
            else:
                out.push_back(CDatum((<Scalar> scal).unwrap()))
                continue

        raise TypeError(
            f'Got unexpected argument type "{type(val)}" for compute function')


cdef class FunctionRegistry(_Weakrefable):
    cdef:
        CFunctionRegistry* registry

    def __init__(self):
        self.registry = GetFunctionRegistry()

    def list_functions(self):
        """
        Return all function names in the registry.
        """
        cdef vector[c_string] names = self.registry.GetFunctionNames()
        return [frombytes(name) for name in names]

    def get_function(self, name):
        """
        Look up a function by name in the registry.
        """
        cdef:
            c_string c_name = tobytes(name)
            shared_ptr[CFunction] func
        with nogil:
            func = GetResultValue(self.registry.GetFunction(c_name))
        return wrap_function(func)


cdef FunctionRegistry _global_func_registry = FunctionRegistry()


def function_registry():
    return _global_func_registry


def get_function(name):
    """
    Get a function by name.

    The function is looked up in the global registry
    (as returned by `function_registry()`).
    """
    return _global_func_registry.get_function(name)


def list_functions():
    """
    Return all function names in the global registry.
    """
    return _global_func_registry.list_fundctions()


def call_function(name, args, options=None, memory_pool=None):
    """
    Call a named function.

    The function is looked up in the global registry
    (as returned by `function_registry()`).
    """
    func = _global_func_registry.get_function(name)
    return func.call(args, options=options, memory_pool=memory_pool)


cdef class FunctionOptions(_Weakrefable):
    __slots__ = ()  # avoid mistakingly creating attributes

    cdef const CFunctionOptions* get_options(self) except NULL:
        return self.wrapped.get()

    cdef void init(self, unique_ptr[CFunctionOptions] options):
        self.wrapped = move(options)

    def serialize(self):
        cdef:
            CResult[shared_ptr[CBuffer]] res = self.get_options().Serialize()
            shared_ptr[CBuffer] c_buf = GetResultValue(res)
        return pyarrow_wrap_buffer(c_buf)

    @staticmethod
    def deserialize(buf):
        cdef:
            shared_ptr[CBuffer] c_buf = pyarrow_unwrap_buffer(buf)
            CResult[unique_ptr[CFunctionOptions]] maybe_options = \
                DeserializeFunctionOptions(deref(c_buf))
            unique_ptr[CFunctionOptions] c_options
        c_options = move(GetResultValue(move(maybe_options)))
        type_name = frombytes(c_options.get().options_type().type_name())
        module = globals()
        if type_name not in module:
            raise ValueError(f'Cannot deserialize "{type_name}"')
        klass = module[type_name]
        options = klass.__new__(klass)
        (<FunctionOptions> options).init(move(c_options))
        return options

    def __repr__(self):
        type_name = self.__class__.__name__
        # Remove {} so we can use our own braces
        string_repr = frombytes(self.get_options().ToString())[1:-1]
        return f"{type_name}({string_repr})"

    def __eq__(self, FunctionOptions other):
        return self.get_options().Equals(deref(other.get_options()))


# NOTE:
# To properly expose the constructor signature of FunctionOptions
# subclasses, we use a two-level inheritance:
# 1. a C extension class that implements option validation and setting
#    (won't expose function signatures because of
#     https://github.com/cython/cython/issues/3873)
# 2. a Python derived class that implements the constructor

cdef class _CastOptions(FunctionOptions):
    cdef:
        CCastOptions* options

    cdef void init(self, unique_ptr[CFunctionOptions] options):
        FunctionOptions.init(self, move(options))
        self.options = <CCastOptions*> self.wrapped.get()

    def _set_options(self, DataType target_type, allow_int_overflow,
                     allow_time_truncate, allow_time_overflow,
                     allow_decimal_truncate, allow_float_truncate,
                     allow_invalid_utf8):
        self.init(unique_ptr[CFunctionOptions](new CCastOptions()))
        self._set_type(target_type)
        if allow_int_overflow is not None:
            self.allow_int_overflow = allow_int_overflow
        if allow_time_truncate is not None:
            self.allow_time_truncate = allow_time_truncate
        if allow_time_overflow is not None:
            self.allow_time_overflow = allow_time_overflow
        if allow_decimal_truncate is not None:
            self.allow_decimal_truncate = allow_decimal_truncate
        if allow_float_truncate is not None:
            self.allow_float_truncate = allow_float_truncate
        if allow_invalid_utf8 is not None:
            self.allow_invalid_utf8 = allow_invalid_utf8

    def _set_type(self, target_type=None):
        if target_type is not None:
            deref(self.options).to_type = \
                (<DataType> ensure_type(target_type)).sp_type

    def _set_safe(self):
        self.init(unique_ptr[CFunctionOptions](
            new CCastOptions(CCastOptions.Safe())))

    def _set_unsafe(self):
        self.init(unique_ptr[CFunctionOptions](
            new CCastOptions(CCastOptions.Unsafe())))

    def is_safe(self):
        return not (deref(self.options).allow_int_overflow or
                    deref(self.options).allow_time_truncate or
                    deref(self.options).allow_time_overflow or
                    deref(self.options).allow_decimal_truncate or
                    deref(self.options).allow_float_truncate or
                    deref(self.options).allow_invalid_utf8)

    @property
    def allow_int_overflow(self):
        return deref(self.options).allow_int_overflow

    @allow_int_overflow.setter
    def allow_int_overflow(self, c_bool flag):
        deref(self.options).allow_int_overflow = flag

    @property
    def allow_time_truncate(self):
        return deref(self.options).allow_time_truncate

    @allow_time_truncate.setter
    def allow_time_truncate(self, c_bool flag):
        deref(self.options).allow_time_truncate = flag

    @property
    def allow_time_overflow(self):
        return deref(self.options).allow_time_overflow

    @allow_time_overflow.setter
    def allow_time_overflow(self, c_bool flag):
        deref(self.options).allow_time_overflow = flag

    @property
    def allow_decimal_truncate(self):
        return deref(self.options).allow_decimal_truncate

    @allow_decimal_truncate.setter
    def allow_decimal_truncate(self, c_bool flag):
        deref(self.options).allow_decimal_truncate = flag

    @property
    def allow_float_truncate(self):
        return deref(self.options).allow_float_truncate

    @allow_float_truncate.setter
    def allow_float_truncate(self, c_bool flag):
        deref(self.options).allow_float_truncate = flag

    @property
    def allow_invalid_utf8(self):
        return deref(self.options).allow_invalid_utf8

    @allow_invalid_utf8.setter
    def allow_invalid_utf8(self, c_bool flag):
        deref(self.options).allow_invalid_utf8 = flag


class CastOptions(_CastOptions):

    def __init__(self, target_type=None, *, allow_int_overflow=None,
                 allow_time_truncate=None, allow_time_overflow=None,
                 allow_decimal_truncate=None, allow_float_truncate=None,
                 allow_invalid_utf8=None):
        self._set_options(target_type, allow_int_overflow, allow_time_truncate,
                          allow_time_overflow, allow_decimal_truncate,
                          allow_float_truncate, allow_invalid_utf8)

    @staticmethod
    def safe(target_type=None):
        self = CastOptions()
        self._set_safe()
        self._set_type(target_type)
        return self

    @staticmethod
    def unsafe(target_type=None):
        self = CastOptions()
        self._set_unsafe()
        self._set_type(target_type)
        return self


cdef class _ElementWiseAggregateOptions(FunctionOptions):
    def _set_options(self, skip_nulls):
        self.wrapped.reset(new CElementWiseAggregateOptions(skip_nulls))


class ElementWiseAggregateOptions(_ElementWiseAggregateOptions):
    def __init__(self, skip_nulls=True):
        self._set_options(skip_nulls)


cdef CRoundMode unwrap_round_mode(round_mode) except *:
    round_mode_dict = {
        "down": CRoundMode_DOWN,
        "up": CRoundMode_UP,
        "towards_zero": CRoundMode_TOWARDS_ZERO,
        "towards_infinity": CRoundMode_TOWARDS_INFINITY,
        "half_down": CRoundMode_HALF_DOWN,
        "half_up": CRoundMode_HALF_UP,
        "half_towards_zero": CRoundMode_HALF_TOWARDS_ZERO,
        "half_towards_infinity": CRoundMode_HALF_TOWARDS_INFINITY,
        "half_to_even": CRoundMode_HALF_TO_EVEN,
        "half_to_odd": CRoundMode_HALF_TO_ODD,
    }
    if round_mode not in round_mode_dict:
        raise ValueError(f"\"{round_mode}\" is not a valid 'round mode'")
    return round_mode_dict[round_mode]


cdef class _RoundOptions(FunctionOptions):
    def _set_options(self, ndigits, round_mode):
        self.wrapped.reset(new CRoundOptions(ndigits,
                                             unwrap_round_mode(round_mode)))


class RoundOptions(_RoundOptions):
    def __init__(self, ndigits=0, round_mode="half_to_even"):
        self._set_options(ndigits, round_mode)


cdef class _RoundToMultipleOptions(FunctionOptions):
    def _set_options(self, multiple, round_mode):
        self.wrapped.reset(
            new CRoundToMultipleOptions(multiple,
                                        unwrap_round_mode(round_mode)))


class RoundToMultipleOptions(_RoundToMultipleOptions):
    def __init__(self, multiple=1.0, round_mode="half_to_even"):
        self._set_options(multiple, round_mode)


cdef class _JoinOptions(FunctionOptions):
    null_handling_dict = {
        "emit_null": CJoinNullHandlingBehavior_EMIT_NULL,
        "skip": CJoinNullHandlingBehavior_SKIP,
        "replace": CJoinNullHandlingBehavior_REPLACE,
    }

    def _set_options(self, null_handling, null_replacement):
        if null_handling not in null_handling_dict:
            raise ValueError(
                f"\"{null_handling}\" is not a valid 'null handling'")
        self.wrapped.reset(new CJoinOptions(null_handling_dict[null_handling],
                                            tobytes(null_replacement)))


class JoinOptions(_JoinOptions):
    def __init__(self, null_handling="emit_null", null_replacement=""):
        self._set_options(null_handling, null_replacement)


cdef class _MatchSubstringOptions(FunctionOptions):
    def _set_options(self, pattern, ignore_case):
        self.wrapped.reset(new CMatchSubstringOptions(tobytes(pattern),
                                                      ignore_case))


class MatchSubstringOptions(_MatchSubstringOptions):
    def __init__(self, pattern, ignore_case=False):
        self._set_options(pattern, ignore_case)


cdef class _PadOptions(FunctionOptions):
    def _set_options(self, width, padding):
        self.wrapped.reset(new CPadOptions(width, tobytes(padding)))


class PadOptions(_PadOptions):
    def __init__(self, width, padding=" "):
        self._set_options(width, padding)


cdef class _TrimOptions(FunctionOptions):
    def _set_options(self, characters):
        self.wrapped.reset(new CTrimOptions(tobytes(characters)))


class TrimOptions(_TrimOptions):
    def __init__(self, characters):
        self._set_options(tobytes(characters))


cdef class _ReplaceSliceOptions(FunctionOptions):
    def _set_options(self, start, stop, replacement):
        self.wrapped.reset(
            new CReplaceSliceOptions(start, stop, tobytes(replacement)))


class ReplaceSliceOptions(_ReplaceSliceOptions):
    def __init__(self, start, stop, replacement):
        self._set_options(start, stop, replacement)


cdef class _ReplaceSubstringOptions(FunctionOptions):
    def _set_options(self, pattern, replacement, max_replacements):
        self.wrapped.reset(new CReplaceSubstringOptions(tobytes(pattern),
                                                        tobytes(replacement),
                                                        max_replacements))


class ReplaceSubstringOptions(_ReplaceSubstringOptions):
    def __init__(self, pattern, replacement, max_replacements=-1):
        self._set_options(pattern, replacement, max_replacements)


cdef class _ExtractRegexOptions(FunctionOptions):
    def _set_options(self, pattern):
        self.wrapped.reset(new CExtractRegexOptions(tobytes(pattern)))


class ExtractRegexOptions(_ExtractRegexOptions):
    def __init__(self, pattern):
        self._set_options(pattern)


cdef class _SliceOptions(FunctionOptions):
    def _set_options(self, start, stop, step):
        self.wrapped.reset(new CSliceOptions(start, stop, step))


class SliceOptions(_SliceOptions):
    def __init__(self, start, stop=sys.maxsize, step=1):
        self._set_options(start, stop, step)


cdef class _FilterOptions(FunctionOptions):
    null_selection_dict = {
        "drop": CFilterNullSelectionBehavior_DROP,
        "emit_null": CFilterNullSelectionBehavior_EMIT_NULL,
    }

    def _set_options(self, null_selection_behavior):
        if null_selection_behavior not in null_selection_dict:
            raise ValueError(f"\"{null_selection_behavior}\""
                             "is not a valid 'null selection behavior'")
        self.wrapped.reset(
            new CFilterOptions(null_selection_dict[null_selection_behavior]))


class FilterOptions(_FilterOptions):
    def __init__(self, null_selection_behavior="drop"):
        self._set_options(null_selection_behavior)


cdef class _DictionaryEncodeOptions(FunctionOptions):
    null_encoding_dict = {
        "encode": CDictionaryEncodeNullEncodingBehavior_ENCODE,
        "mask": CDictionaryEncodeNullEncodingBehavior_MASK,
    }

    def _set_options(self, null_encoding):
        if null_encoding not in null_encoding_dict:
            raise ValueError(
                f"\"{null_encoding}\" is not a valid 'null encoding'")
        self.wrapped.reset(
            new CDictionaryEncodeOptions(null_encoding_dict[null_encoding]))


class DictionaryEncodeOptions(_DictionaryEncodeOptions):
    def __init__(self, null_encoding="mask"):
        self._set_options(null_encoding)


cdef class _TakeOptions(FunctionOptions):
    def _set_options(self, boundscheck):
        self.wrapped.reset(new CTakeOptions(boundscheck))


class TakeOptions(_TakeOptions):
    def __init__(self, boundscheck=True):
        self._set_options(boundscheck)


cdef class _PartitionNthOptions(FunctionOptions):
    def _set_options(self, pivot):
        self.wrapped.reset(new CPartitionNthOptions(pivot))


class PartitionNthOptions(_PartitionNthOptions):
    def __init__(self, pivot):
        self._set_options(pivot)


cdef class _MakeStructOptions(FunctionOptions):
    def _set_options(self, field_names):
        cdef:
            vector[c_string] c_field_names
        for name in field_names:
            c_field_names.push_back(tobytes(name))
        self.wrapped.reset(new CMakeStructOptions(c_field_names))


class MakeStructOptions(_MakeStructOptions):
    def __init__(self, field_names):
        self._set_options(field_names)


cdef class _ScalarAggregateOptions(FunctionOptions):
    def _set_options(self, skip_nulls, min_count):
        self.wrapped.reset(new CScalarAggregateOptions(skip_nulls, min_count))


class ScalarAggregateOptions(_ScalarAggregateOptions):
    def __init__(self, skip_nulls=True, min_count=1):
        self._set_options(skip_nulls, min_count)


cdef class _CountOptions(FunctionOptions):
    mode_dict = {
        "only_valid": CCountMode_ONLY_VALID,
        "only_null": CCountMode_ONLY_NULL,
        "all": CCountMode_ALL,
    }

    def _set_options(self, mode):
        if mode not in mode_dict:
            raise ValueError(f"\"{mode}\" is not a valid 'count mode'")
        self.wrapped.reset(new CCountOptions(mode_dict[mode]))


class CountOptions(_CountOptions):
    def __init__(self, mode="only_valid"):
        self._set_options(mode)


cdef class _IndexOptions(FunctionOptions):
    def _set_options(self, Scalar scalar):
        self.wrapped.reset(new CIndexOptions(pyarrow_unwrap_scalar(scalar)))


class IndexOptions(_IndexOptions):
    """
    Options for the index kernel.

    Parameters
    ----------
    value : Scalar
        The value to search for.
    """

    def __init__(self, value):
        self._set_options(value)


cdef class _ModeOptions(FunctionOptions):
    def _set_options(self, n, skip_nulls, min_count):
        self.wrapped.reset(new CModeOptions(n, skip_nulls, min_count))


class ModeOptions(_ModeOptions):
    def __init__(self, n=1, skip_nulls=True, min_count=0):
        self._set_options(n, skip_nulls, min_count)


cdef class _SetLookupOptions(FunctionOptions):
    def _set_options(self, value_set, c_bool skip_nulls):
        cdef:
            unique_ptr[CDatum] valset
        if isinstance(value_set, Array):
            valset.reset(new CDatum((<Array> value_set).sp_array))
        elif isinstance(value_set, ChunkedArray):
            valset.reset(
                new CDatum((<ChunkedArray> value_set).sp_chunked_array))
        elif isinstance(value_set, Scalar):
            valset.reset(new CDatum((<Scalar> value_set).unwrap()))
        else:
            raise ValueError(f"\"{value_set}\" is not a valid 'value set'")
        self.wrapped.reset(new CSetLookupOptions(deref(valset), skip_nulls))


class SetLookupOptions(_SetLookupOptions):
    def __init__(self, value_set, skip_nulls=False):
        self._set_options(value_set, skip_nulls)


cdef class _StrptimeOptions(FunctionOptions):
    unit_dict = {
        "s": TimeUnit_SECOND,
        "ms": TimeUnit_MILLI,
        "us": TimeUnit_MICRO,
        "ns": TimeUnit_NANO,
    }

    def _set_options(self, format, unit):
        if unit not in unit_dict:
            raise ValueError(f'"{unit}" is not a valid time unit')
        self.wrapped.reset(
            new CStrptimeOptions(tobytes(format), unit_dict[unit]))


class StrptimeOptions(_StrptimeOptions):
    def __init__(self, format, unit):
        self._set_options(format, unit)


cdef class _StrftimeOptions(FunctionOptions):
    def _set_options(self, format, locale):
        self.wrapped.reset(
            new CStrftimeOptions(tobytes(format), tobytes(locale)))


class StrftimeOptions(_StrftimeOptions):
    def __init__(self, format="%Y-%m-%dT%H:%M:%S", locale="C"):
        self._set_options(format, locale)


cdef class _DayOfWeekOptions(FunctionOptions):
    def _set_options(self, one_based_numbering, week_start):
        self.wrapped.reset(
            new CDayOfWeekOptions(one_based_numbering, week_start))


class DayOfWeekOptions(_DayOfWeekOptions):
    def __init__(self, one_based_numbering=False, week_start=1):
        self._set_options(one_based_numbering, week_start)


cdef class _AssumeTimezoneOptions(FunctionOptions):
    def _set_options(self, timezone, ambiguous, nonexistent):
        ambiguous_dict = {
            "raise": CAssumeTimezoneAmbiguous_AMBIGUOUS_RAISE,
            "earliest": CAssumeTimezoneAmbiguous_AMBIGUOUS_EARLIEST,
            "latest": CAssumeTimezoneAmbiguous_AMBIGUOUS_LATEST,
        }
        nonexistent_dict = {
            "raise": CAssumeTimezoneNonexistent_NONEXISTENT_RAISE,
            "earliest": CAssumeTimezoneNonexistent_NONEXISTENT_EARLIEST,
            "latest": CAssumeTimezoneNonexistent_NONEXISTENT_LATEST,
        }

        if ambiguous not in ambiguous_dict:
            raise ValueError(
                f"\"{ambiguous}\" is not a valid 'ambiguous' keyword")
        if nonexistent not in nonexistent_dict:
            raise ValueError(
                f"\"{nonexistent}\" is not a valid 'nonexistent' keyword")

        self.wrapped.reset(
            new CAssumeTimezoneOptions(tobytes(timezone),
                                       ambiguous_dict[ambiguous],
                                       nonexistent_dict[nonexistent]))


class AssumeTimezoneOptions(_AssumeTimezoneOptions):
    def __init__(self, timezone, ambiguous="raise", nonexistent="raise"):
        self._set_options(timezone, ambiguous, nonexistent)


cdef class _NullOptions(FunctionOptions):
    def _set_options(self, nan_is_null):
        self.wrapped.reset(new CNullOptions(nan_is_null))


class NullOptions(_NullOptions):
    def __init__(self, nan_is_null=False):
        self._set_options(nan_is_null)


cdef class _VarianceOptions(FunctionOptions):
    def _set_options(self, ddof, skip_nulls, min_count):
        self.wrapped.reset(new CVarianceOptions(ddof, skip_nulls, min_count))


class VarianceOptions(_VarianceOptions):
    def __init__(self, ddof=0, skip_nulls=True, min_count=0):
        self._set_options(ddof, skip_nulls, min_count)


cdef class _SplitOptions(FunctionOptions):
    def _set_options(self, max_splits, reverse):
        self.wrapped.reset(new CSplitOptions(max_splits, reverse))


class SplitOptions(_SplitOptions):
    def __init__(self, max_splits=-1, reverse=False):
        self._set_options(max_splits, reverse)


cdef class _SplitPatternOptions(FunctionOptions):
    def _set_options(self, pattern, max_splits, reverse):
        self.wrapped.reset(
            new CSplitPatternOptions(tobytes(pattern), max_splits, reverse))


class SplitPatternOptions(_SplitPatternOptions):
    def __init__(self, pattern, max_splits=-1, reverse=False):
        self._set_options(pattern, max_splits, reverse)


cdef class _ArraySortOptions(FunctionOptions):
    order_dict = {
        "ascending": CSortOrder_Ascending,
        "descending": CSortOrder_Descending,
    }

    def _set_options(self, order):
        if order not in order_dict:
            raise ValueError(f'"{order}" is not a valid order')
        self.wrapped.reset(new CArraySortOptions(order_dict[order]))


class ArraySortOptions(_ArraySortOptions):
    def __init__(self, order="ascending"):
        self._set_options(order)


cdef class _SortOptions(FunctionOptions):
    order_dict = {
        "ascending": CSortOrder_Ascending,
        "descending": CSortOrder_Descending,
    }

    def _set_options(self, sort_keys):
        cdef:
            vector[CSortKey] c_sort_keys
        for name, order in sort_keys:
            if order not in order_dict:
                raise ValueError(f'"{order}" is not a valid order')
            c_sort_keys.push_back(CSortKey(tobytes(name), order_dict[order]))
        self.wrapped.reset(new CSortOptions(c_sort_keys))


class SortOptions(_SortOptions):
    def __init__(self, sort_keys=None):
        if sort_keys is None:
            sort_keys = []
        self._set_options(sort_keys)


cdef class _SelectKOptions(FunctionOptions):
    order_dict = {
        "ascending": CSortOrder_Ascending,
        "descending": CSortOrder_Descending,
    }

    def _set_options(self, k, sort_keys):
        cdef:
            vector[CSortKey] c_sort_keys
        for name, order in sort_keys:
            if order not in order_dict:
                raise ValueError(f'"{order}" is not a valid order')
            c_sort_keys.push_back(CSortKey(tobytes(c_name), order_dict[order]))
        self.wrapped.reset(new CSelectKOptions(k, c_sort_keys))


class SelectKOptions(_SelectKOptions):
    def __init__(self, k, sort_keys=None):
        if sort_keys is None:
            sort_keys = []
        self._set_options(k, sort_keys)


cdef class _QuantileOptions(FunctionOptions):
    interp_dict = {
        "linear": CQuantileInterp_LINEAR,
        "lower": CQuantileInterp_LOWER,
        "higher": CQuantileInterp_HIGHER,
        "nearest": CQuantileInterp_NEAREST,
        "midpoint": CQuantileInterp_MIDPOINT,
    }

    def _set_options(self, quantiles, interp, skip_nulls, min_count):
        if interp not in interp_dict:
            raise ValueError(f'"{interp}" is not a valid interpolation')
        self.wrapped.reset(new CQuantileOptions(quantiles, interp_dict[interp],
                                                skip_nulls, min_count))


class QuantileOptions(_QuantileOptions):
    def __init__(self, q=0.5, interpolation="linear",
                 skip_nulls=True, min_count=0):
        if not isinstance(q, (list, tuple, np.ndarray)):
            q = [q]
        self._set_options(q, interpolation, skip_nulls, min_count)


cdef class _TDigestOptions(FunctionOptions):
    def _set_options(self, quantiles, delta, buffer_size,
                     skip_nulls, min_count):
        self.wrapped.reset(new CTDigestOptions(quantiles, delta, buffer_size,
                                               skip_nulls, min_count))


class TDigestOptions(_TDigestOptions):
    def __init__(self, q=0.5, delta=100, buffer_size=500,
                 skip_nulls=True, min_count=0):
        if not isinstance(q, (list, tuple, np.ndarray)):
            q = [q]
        self._set_options(q, delta, buffer_size, skip_nulls, min_count)
