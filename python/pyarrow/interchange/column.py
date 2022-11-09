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

from __future__ import annotations

from typing import (
    Any,
    Dict,
    Iterable,
    Optional,
    Tuple,
)

import pyarrow as pa
from pyarrow.interchange.buffer import PyArrowBuffer
from pyarrow.interchange.dataframe_protocol import (Column, ColumnBuffers,
                                                    ColumnNullType, DtypeKind,
                                                    CategoricalDescription)

_PYARROW_KINDS = {
    pa.int8(): (DtypeKind.INT, "c"),
    pa.int16(): (DtypeKind.INT, "s"),
    pa.int32(): (DtypeKind.INT, "i"),
    pa.int64(): (DtypeKind.INT, "l"),
    pa.uint8(): (DtypeKind.UINT, "C"),
    pa.uint16(): (DtypeKind.UINT, "S"),
    pa.uint32(): (DtypeKind.UINT, "I"),
    pa.uint64(): (DtypeKind.UINT, "L"),
    pa.float16(): (DtypeKind.FLOAT, "e"),
    pa.float32(): (DtypeKind.FLOAT, "f"),
    pa.float64(): (DtypeKind.FLOAT, "g"),
    pa.bool_(): (DtypeKind.BOOL, "b"),
    pa.string(): (DtypeKind.STRING, "u"),  # utf-8
    pa.large_string(): (DtypeKind.STRING, "U"),
}


class Endianness:
    """Enum indicating the byte-order of a data-type."""

    LITTLE = "<"
    BIG = ">"
    NATIVE = "="
    NA = "|"


class NoBufferPresent(Exception):
    """Exception to signal that there is no requested buffer."""


class PyArrowColumn(Column):
    """
    A column object, with only the methods and properties required by the
    interchange protocol defined.
    A column can contain one or more chunks. Each chunk can contain up to three
    buffers - a data buffer, a mask buffer (depending on null representation),
    and an offsets buffer (if variable-size binary; e.g., variable-length
    strings).
    Note: this Column object can only be produced by ``__dataframe__``, so
          doesn't need its own version or ``__column__`` protocol.
    """

    def __init__(
        self, column: pa.Array | pa.ChunkedArray, allow_copy: bool = True
    ) -> None:
        """
        Handles PyArrow Arrays and ChunkedArrays.
        """
        # Store the column as a private attribute
        self._col = column
        self._allow_copy = allow_copy

    def size(self) -> int:
        """
        Size of the column, in elements.
        """
        return len(self._col)

    @property
    def offset(self) -> int:
        """
        Offset of first element.
        """
        return 0

    @property
    def dtype(self) -> Tuple[DtypeKind, int, str, str]:
        """
        Dtype description as a tuple ``(kind, bit-width, format string,
        endianness)``.
        Bit-width : the number of bits as an integer
        Format string : data type description format string in Apache Arrow
                        C Data Interface format.
        Endianness : current only native endianness (``=``) is supported
        Notes:
            - Kind specifiers are aligned with DLPack where possible (hence
            the jump to 20, leave enough room for future extension)
            - Masks must be specified as boolean with either bit width 1
              (for bit masks) or 8 (for byte masks).
            - Dtype width in bits was preferred over bytes
            - Endianness isn't too useful, but included now in case in the
              future we need to support non-native endianness
            - Went with Apache Arrow format strings over NumPy format strings
              because they're more complete from a dataframe perspective
            - Format strings are mostly useful for datetime specification, and
              for categoricals.
            - For categoricals, the format string describes the type of the
              categorical in the data buffer. In case of a separate encoding of
              the categorical (e.g. an integer to string mapping), this can
              be derived from ``self.describe_categorical``.
            - Data types not included: complex, Arrow-style null, binary,
              decimal, and nested (list, struct, map, union) dtypes.
        """
        dtype = self._col.type
        try:
            bit_width = dtype.bit_width
        except ValueError:  # in case of a variable-length strings
            bit_width = 8
        # In case of bool data type, bit_width is 1 and has to be multiplied
        # by 8 (why is that not the case for other dtypes?)
        if pa.types.is_boolean(dtype):
            bit_width *= 8

        if pa.types.is_timestamp(dtype):
            kind = DtypeKind.DATETIME
            f_string = "ts{dtype.unit}:{dtype.tz}"
            return kind, bit_width, f_string, Endianness.NATIVE
        elif pa.types.is_dictionary(dtype):
            kind = DtypeKind.CATEGORICAL
            f_string = "L"
            return kind, bit_width, f_string, Endianness.NATIVE
        else:
            return self._dtype_from_arrowdtype(dtype, bit_width)

    def _dtype_from_arrowdtype(
        self, dtype, bit_width
    ) -> Tuple[DtypeKind, int, str, str]:
        """
        See `self.dtype` for details.
        """
        # Note: 'c' (complex) not handled yet (not in array spec v1).
        #       'b', 'B' (bytes), 'S', 'a', (old-style string) 'V' (void)
        #       not handled datetime and timedelta both map to datetime
        #       (is timedelta handled?)

        kind, f_string = _PYARROW_KINDS.get(dtype, (None, None))
        if kind is None:
            raise ValueError(
                f"Data type {dtype} not supported by interchange protocol")

        return kind, bit_width, f_string, Endianness.NATIVE

    @property
    def describe_categorical(self) -> CategoricalDescription:
        """
        If the dtype is categorical, there are two options:
        - There are only values in the data buffer.
        - There is a separate non-categorical Column encoding for categorical
          values.
        Raises TypeError if the dtype is not categorical
        Content of returned dict:
            - "is_ordered" : bool, whether the ordering of dictionary indices
                             is semantically meaningful.
            - "is_dictionary" : bool, whether a dictionary-style mapping of
                                categorical values to other objects exists
            - "categories" : Column representing the (implicit) mapping of
                             indices to category values (e.g. an array of
                             cat1, cat2, ...). None if not a dictionary-style
                             categorical.
        """
        if isinstance(self._col, pa.ChunkedArray):
            arr = self._col.combine_chunks()
        else:
            arr = self._col

        if not pa.types.is_dictionary(arr.type):
            raise TypeError(
                "describe_categorical only works on a column with "
                "categorical dtype!"
            )

        return {
            "is_ordered": self._col.type.ordered,
            "is_dictionary": True,
            "categories": PyArrowColumn(arr.dictionary),
        }

    @property
    def describe_null(self) -> Tuple[ColumnNullType, Any]:
        return ColumnNullType.USE_BYTEMASK, 0

    @property
    def null_count(self) -> int:
        """
        Number of null elements. Should always be known.
        """
        return self._col.null_count

    @property
    def metadata(self) -> Dict[str, Any]:
        """
        Store specific metadata of the column.
        """
        pass

    def num_chunks(self) -> int:
        """
        Return the number of chunks the column consists of.
        """
        if isinstance(self._col, pa.Array):
            n_chunks = 1
        else:
            n_chunks = self._col.num_chunks
        return n_chunks

    def get_chunks(self, n_chunks: Optional[int] = None) -> Iterable["Column"]:
        """
        Return an iterator yielding the chunks.
        See `DataFrame.get_chunks` for details on ``n_chunks``.
        """
        if n_chunks and n_chunks > 1:
            chunk_size = self.size() // n_chunks
            if self.size() % n_chunks != 0:
                chunk_size += 1

            if isinstance(self._col, pa.ChunkedArray):
                array = self._col.combine_chunks()
            else:
                array = self._col

            i = 0
            for start in range(0, chunk_size * n_chunks, chunk_size):
                yield PyArrowColumn(
                    array.slice(start, chunk_size), self._allow_copy
                )
                i += 1
            # In case when the size of the chunk is such that the resulting
            # list is one less chunk then n_chunks -> append an empty chunk
            if i == n_chunks - 1:
                yield PyArrowColumn(pa.array([]), self._allow_copy)
        elif isinstance(self._col, pa.ChunkedArray):
            return [
                PyArrowColumn(chunk, self._allow_copy)
                for chunk in self._col.chunks
            ]
        else:
            yield self

    def get_buffers(self) -> ColumnBuffers:
        """
        Return a dictionary containing the underlying buffers.
        The returned dictionary has the following contents:
            - "data": a two-element tuple whose first element is a buffer
                      containing the data and whose second element is the data
                      buffer's associated dtype.
            - "validity": a two-element tuple whose first element is a buffer
                          containing mask values indicating missing data and
                          whose second element is the mask value buffer's
                          associated dtype. None if the null representation is
                          not a bit or byte mask.
            - "offsets": a two-element tuple whose first element is a buffer
                         containing the offset values for variable-size binary
                         data (e.g., variable-length strings) and whose second
                         element is the offsets buffer's associated dtype. None
                         if the data buffer does not have an associated offsets
                         buffer.
        """
        buffers: ColumnBuffers = {
            "data": self._get_data_buffer(),
            "validity": None,
            "offsets": None,
        }

        try:
            buffers["validity"] = self._get_validity_buffer()
        except NoBufferPresent:
            pass

        try:
            buffers["offsets"] = self._get_offsets_buffer()
        except NoBufferPresent:
            pass

        return buffers

    def _get_data_buffer(
        self,
    ) -> Tuple[PyArrowBuffer, Any]:  # Any is for self.dtype tuple
        """
        Return the buffer containing the data and the buffer's
        associated dtype.
        """
        if isinstance(self._col, pa.ChunkedArray):
            array = self._col.combine_chunks()
        else:
            array = self._col
        n = len(array.buffers())
        if n == 2:
            return PyArrowBuffer(array.buffers()[1]), self.dtype
        elif n == 3:
            return PyArrowBuffer(array.buffers()[2]), self.dtype

    def _get_validity_buffer(self) -> Tuple[PyArrowBuffer, Any]:
        """
        Return the buffer containing the mask values indicating missing data
        and the buffer's associated dtype.
        Raises NoBufferPresent if null representation is not a bit or byte
        mask.
        """
        # Define the dtype of the returned buffer
        dtype = (DtypeKind.BOOL, 8, "b", Endianness.NATIVE)
        if isinstance(self._col, pa.ChunkedArray):
            array = self._col.combine_chunks()
        else:
            array = self._col
        buff = array.buffers()[0]
        if buff:
            return PyArrowBuffer(buff), dtype
        else:
            raise NoBufferPresent(
                "There are no missing values so "
                "does not have a separate mask")

    def _get_offsets_buffer(self) -> Tuple[PyArrowBuffer, Any]:
        """
        Return the buffer containing the offset values for variable-size binary
        data (e.g., variable-length strings) and the buffer's associated dtype.
        Raises NoBufferPresent if the data buffer does not have an associated
        offsets buffer.
        """
        if isinstance(self._col, pa.ChunkedArray):
            array = self._col.combine_chunks()
        else:
            array = self._col
        n = len(array.buffers())
        if n == 2:
            raise NoBufferPresent(
                "This column has a fixed-length dtype so "
                "it does not have an offsets buffer"
            )
        elif n == 3:
            # Define the dtype of the returned buffer
            dtype = (DtypeKind.INT, 64, "L", Endianness.NATIVE)
            return PyArrowBuffer(array.buffers()[2]), dtype
