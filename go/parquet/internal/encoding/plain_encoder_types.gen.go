// Code generated by plain_encoder_types.gen.go.tmpl. DO NOT EDIT.

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

package encoding

import (
	"bytes"
	"encoding/binary"
	"math"

	"github.com/apache/arrow/go/v6/arrow"
	"github.com/apache/arrow/go/v6/arrow/endian"
	"github.com/apache/arrow/go/v6/parquet"
	"github.com/apache/arrow/go/v6/parquet/internal/utils"
	"golang.org/x/xerrors"
)

var (
	writeInt32LE      func(*encoder, []int32)
	copyFromInt32LE   func(dst []int32, src []byte)
	writeInt64LE      func(*encoder, []int64)
	copyFromInt64LE   func(dst []int64, src []byte)
	writeInt96LE      func(*encoder, []parquet.Int96)
	copyFromInt96LE   func(dst []parquet.Int96, src []byte)
	writeFloat32LE    func(*encoder, []float32)
	copyFromFloat32LE func(dst []float32, src []byte)
	writeFloat64LE    func(*encoder, []float64)
	copyFromFloat64LE func(dst []float64, src []byte)
)

func init() {
	// int96 is already internally represented as little endian data
	// no need to have special behavior on big endian architectures
	// for read/write, consumers will need to be aware of the fact
	// that it is internally 12 bytes little endian when attempting
	// to utilize it.
	writeInt96LE = func(e *encoder, in []parquet.Int96) {
		e.append(parquet.Int96Traits.CastToBytes(in))
	}
	copyFromInt96LE = func(dst []parquet.Int96, src []byte) {
		copy(parquet.Int96Traits.CastToBytes(dst), src)
	}

	if endian.IsBigEndian {
		writeInt32LE = func(e *encoder, in []int32) {
			binary.Write(e.sink, binary.LittleEndian, in)
		}
		copyFromInt32LE = func(dst []int32, src []byte) {
			r := bytes.NewReader(src)
			binary.Read(r, binary.LittleEndian, &dst)
		}
		writeInt64LE = func(e *encoder, in []int64) {
			binary.Write(e.sink, binary.LittleEndian, in)
		}
		copyFromInt64LE = func(dst []int64, src []byte) {
			r := bytes.NewReader(src)
			binary.Read(r, binary.LittleEndian, &dst)
		}
		writeFloat32LE = func(e *encoder, in []float32) {
			binary.Write(e.sink, binary.LittleEndian, in)
		}
		copyFromFloat32LE = func(dst []float32, src []byte) {
			r := bytes.NewReader(src)
			binary.Read(r, binary.LittleEndian, &dst)
		}
		writeFloat64LE = func(e *encoder, in []float64) {
			binary.Write(e.sink, binary.LittleEndian, in)
		}
		copyFromFloat64LE = func(dst []float64, src []byte) {
			r := bytes.NewReader(src)
			binary.Read(r, binary.LittleEndian, &dst)
		}
	} else {
		writeInt32LE = func(e *encoder, in []int32) {
			e.append(arrow.Int32Traits.CastToBytes(in))
		}
		copyFromInt32LE = func(dst []int32, src []byte) {
			copy(arrow.Int32Traits.CastToBytes(dst), src)
		}
		writeInt64LE = func(e *encoder, in []int64) {
			e.append(arrow.Int64Traits.CastToBytes(in))
		}
		copyFromInt64LE = func(dst []int64, src []byte) {
			copy(arrow.Int64Traits.CastToBytes(dst), src)
		}
		writeFloat32LE = func(e *encoder, in []float32) {
			e.append(arrow.Float32Traits.CastToBytes(in))
		}
		copyFromFloat32LE = func(dst []float32, src []byte) {
			copy(arrow.Float32Traits.CastToBytes(dst), src)
		}
		writeFloat64LE = func(e *encoder, in []float64) {
			e.append(arrow.Float64Traits.CastToBytes(in))
		}
		copyFromFloat64LE = func(dst []float64, src []byte) {
			copy(arrow.Float64Traits.CastToBytes(dst), src)
		}
	}
}

// PlainInt32Encoder is an encoder for int32 values using Plain Encoding
// which in general is just storing the values as raw bytes of the appropriate size
type PlainInt32Encoder struct {
	encoder

	bitSetReader utils.SetBitRunReader
}

// Put encodes a slice of values into the underlying buffer
func (enc *PlainInt32Encoder) Put(in []int32) {
	writeInt32LE(&enc.encoder, in)
}

// PutSpaced encodes a slice of values into the underlying buffer which are spaced out
// including null values defined by the validBits bitmap starting at a given bit offset.
// the values are first compressed by having the null slots removed before writing to the buffer
func (enc *PlainInt32Encoder) PutSpaced(in []int32, validBits []byte, validBitsOffset int64) {
	nbytes := arrow.Int32Traits.BytesRequired(len(in))
	enc.ReserveForWrite(nbytes)

	if enc.bitSetReader == nil {
		enc.bitSetReader = utils.NewSetBitRunReader(validBits, validBitsOffset, int64(len(in)))
	} else {
		enc.bitSetReader.Reset(validBits, validBitsOffset, int64(len(in)))
	}

	for {
		run := enc.bitSetReader.NextRun()
		if run.Length == 0 {
			break
		}
		enc.Put(in[int(run.Pos):int(run.Pos+run.Length)])
	}
}

// Type returns the underlying physical type this encoder is able to encode
func (PlainInt32Encoder) Type() parquet.Type {
	return parquet.Types.Int32
}

// PlainInt32Decoder is a decoder specifically for decoding Plain Encoding data
// of int32 type.
type PlainInt32Decoder struct {
	decoder

	bitSetReader utils.SetBitRunReader
}

// Type returns the physical type this decoder is able to decode for
func (PlainInt32Decoder) Type() parquet.Type {
	return parquet.Types.Int32
}

// Decode populates the given slice with values from the data to be decoded,
// decoding the min(len(out), remaining values).
// It returns the number of values actually decoded and any error encountered.
func (dec *PlainInt32Decoder) Decode(out []int32) (int, error) {
	max := utils.MinInt(len(out), dec.nvals)
	nbytes := int64(max) * int64(arrow.Int32SizeBytes)
	if nbytes > int64(len(dec.data)) || nbytes > math.MaxInt32 {
		return 0, xerrors.Errorf("parquet: eof exception decode plain Int32, nvals: %d, nbytes: %d, datalen: %d", dec.nvals, nbytes, len(dec.data))
	}

	copyFromInt32LE(out, dec.data[:nbytes])
	dec.data = dec.data[nbytes:]
	dec.nvals -= max
	return max, nil
}

// DecodeSpaced is the same as decode, except it expands the data out to leave spaces for null values
// as defined by the bitmap provided.
func (dec *PlainInt32Decoder) DecodeSpaced(out []int32, nullCount int, validBits []byte, validBitsOffset int64) (int, error) {
	toread := len(out) - nullCount
	values, err := dec.Decode(out[:toread])
	if err != nil {
		return 0, err
	}
	if values != toread {
		return 0, xerrors.New("parquet: number of values / definition levels read did not match")
	}

	nvalues := len(out)
	if nullCount == 0 {
		return nvalues, nil
	}

	idxDecode := nvalues - nullCount
	if dec.bitSetReader == nil {
		dec.bitSetReader = utils.NewReverseSetBitRunReader(validBits, validBitsOffset, int64(nvalues))
	} else {
		dec.bitSetReader.Reset(validBits, validBitsOffset, int64(nvalues))
	}

	for {
		run := dec.bitSetReader.NextRun()
		if run.Length == 0 {
			break
		}

		idxDecode -= int(run.Length)
		copy(out[int(run.Pos):], out[idxDecode:idxDecode+int(run.Length)])
	}
	return nvalues, nil
}

// PlainInt64Encoder is an encoder for int64 values using Plain Encoding
// which in general is just storing the values as raw bytes of the appropriate size
type PlainInt64Encoder struct {
	encoder

	bitSetReader utils.SetBitRunReader
}

// Put encodes a slice of values into the underlying buffer
func (enc *PlainInt64Encoder) Put(in []int64) {
	writeInt64LE(&enc.encoder, in)
}

// PutSpaced encodes a slice of values into the underlying buffer which are spaced out
// including null values defined by the validBits bitmap starting at a given bit offset.
// the values are first compressed by having the null slots removed before writing to the buffer
func (enc *PlainInt64Encoder) PutSpaced(in []int64, validBits []byte, validBitsOffset int64) {
	nbytes := arrow.Int64Traits.BytesRequired(len(in))
	enc.ReserveForWrite(nbytes)

	if enc.bitSetReader == nil {
		enc.bitSetReader = utils.NewSetBitRunReader(validBits, validBitsOffset, int64(len(in)))
	} else {
		enc.bitSetReader.Reset(validBits, validBitsOffset, int64(len(in)))
	}

	for {
		run := enc.bitSetReader.NextRun()
		if run.Length == 0 {
			break
		}
		enc.Put(in[int(run.Pos):int(run.Pos+run.Length)])
	}
}

// Type returns the underlying physical type this encoder is able to encode
func (PlainInt64Encoder) Type() parquet.Type {
	return parquet.Types.Int64
}

// PlainInt64Decoder is a decoder specifically for decoding Plain Encoding data
// of int64 type.
type PlainInt64Decoder struct {
	decoder

	bitSetReader utils.SetBitRunReader
}

// Type returns the physical type this decoder is able to decode for
func (PlainInt64Decoder) Type() parquet.Type {
	return parquet.Types.Int64
}

// Decode populates the given slice with values from the data to be decoded,
// decoding the min(len(out), remaining values).
// It returns the number of values actually decoded and any error encountered.
func (dec *PlainInt64Decoder) Decode(out []int64) (int, error) {
	max := utils.MinInt(len(out), dec.nvals)
	nbytes := int64(max) * int64(arrow.Int64SizeBytes)
	if nbytes > int64(len(dec.data)) || nbytes > math.MaxInt32 {
		return 0, xerrors.Errorf("parquet: eof exception decode plain Int64, nvals: %d, nbytes: %d, datalen: %d", dec.nvals, nbytes, len(dec.data))
	}

	copyFromInt64LE(out, dec.data[:nbytes])
	dec.data = dec.data[nbytes:]
	dec.nvals -= max
	return max, nil
}

// DecodeSpaced is the same as decode, except it expands the data out to leave spaces for null values
// as defined by the bitmap provided.
func (dec *PlainInt64Decoder) DecodeSpaced(out []int64, nullCount int, validBits []byte, validBitsOffset int64) (int, error) {
	toread := len(out) - nullCount
	values, err := dec.Decode(out[:toread])
	if err != nil {
		return 0, err
	}
	if values != toread {
		return 0, xerrors.New("parquet: number of values / definition levels read did not match")
	}

	nvalues := len(out)
	if nullCount == 0 {
		return nvalues, nil
	}

	idxDecode := nvalues - nullCount
	if dec.bitSetReader == nil {
		dec.bitSetReader = utils.NewReverseSetBitRunReader(validBits, validBitsOffset, int64(nvalues))
	} else {
		dec.bitSetReader.Reset(validBits, validBitsOffset, int64(nvalues))
	}

	for {
		run := dec.bitSetReader.NextRun()
		if run.Length == 0 {
			break
		}

		idxDecode -= int(run.Length)
		copy(out[int(run.Pos):], out[idxDecode:idxDecode+int(run.Length)])
	}
	return nvalues, nil
}

// PlainInt96Encoder is an encoder for parquet.Int96 values using Plain Encoding
// which in general is just storing the values as raw bytes of the appropriate size
type PlainInt96Encoder struct {
	encoder

	bitSetReader utils.SetBitRunReader
}

// Put encodes a slice of values into the underlying buffer
func (enc *PlainInt96Encoder) Put(in []parquet.Int96) {
	writeInt96LE(&enc.encoder, in)
}

// PutSpaced encodes a slice of values into the underlying buffer which are spaced out
// including null values defined by the validBits bitmap starting at a given bit offset.
// the values are first compressed by having the null slots removed before writing to the buffer
func (enc *PlainInt96Encoder) PutSpaced(in []parquet.Int96, validBits []byte, validBitsOffset int64) {
	nbytes := parquet.Int96Traits.BytesRequired(len(in))
	enc.ReserveForWrite(nbytes)

	if enc.bitSetReader == nil {
		enc.bitSetReader = utils.NewSetBitRunReader(validBits, validBitsOffset, int64(len(in)))
	} else {
		enc.bitSetReader.Reset(validBits, validBitsOffset, int64(len(in)))
	}

	for {
		run := enc.bitSetReader.NextRun()
		if run.Length == 0 {
			break
		}
		enc.Put(in[int(run.Pos):int(run.Pos+run.Length)])
	}
}

// Type returns the underlying physical type this encoder is able to encode
func (PlainInt96Encoder) Type() parquet.Type {
	return parquet.Types.Int96
}

// PlainInt96Decoder is a decoder specifically for decoding Plain Encoding data
// of parquet.Int96 type.
type PlainInt96Decoder struct {
	decoder

	bitSetReader utils.SetBitRunReader
}

// Type returns the physical type this decoder is able to decode for
func (PlainInt96Decoder) Type() parquet.Type {
	return parquet.Types.Int96
}

// Decode populates the given slice with values from the data to be decoded,
// decoding the min(len(out), remaining values).
// It returns the number of values actually decoded and any error encountered.
func (dec *PlainInt96Decoder) Decode(out []parquet.Int96) (int, error) {
	max := utils.MinInt(len(out), dec.nvals)
	nbytes := int64(max) * int64(parquet.Int96SizeBytes)
	if nbytes > int64(len(dec.data)) || nbytes > math.MaxInt32 {
		return 0, xerrors.Errorf("parquet: eof exception decode plain Int96, nvals: %d, nbytes: %d, datalen: %d", dec.nvals, nbytes, len(dec.data))
	}

	copyFromInt96LE(out, dec.data[:nbytes])
	dec.data = dec.data[nbytes:]
	dec.nvals -= max
	return max, nil
}

// DecodeSpaced is the same as decode, except it expands the data out to leave spaces for null values
// as defined by the bitmap provided.
func (dec *PlainInt96Decoder) DecodeSpaced(out []parquet.Int96, nullCount int, validBits []byte, validBitsOffset int64) (int, error) {
	toread := len(out) - nullCount
	values, err := dec.Decode(out[:toread])
	if err != nil {
		return 0, err
	}
	if values != toread {
		return 0, xerrors.New("parquet: number of values / definition levels read did not match")
	}

	nvalues := len(out)
	if nullCount == 0 {
		return nvalues, nil
	}

	idxDecode := nvalues - nullCount
	if dec.bitSetReader == nil {
		dec.bitSetReader = utils.NewReverseSetBitRunReader(validBits, validBitsOffset, int64(nvalues))
	} else {
		dec.bitSetReader.Reset(validBits, validBitsOffset, int64(nvalues))
	}

	for {
		run := dec.bitSetReader.NextRun()
		if run.Length == 0 {
			break
		}

		idxDecode -= int(run.Length)
		copy(out[int(run.Pos):], out[idxDecode:idxDecode+int(run.Length)])
	}
	return nvalues, nil
}

// PlainFloat32Encoder is an encoder for float32 values using Plain Encoding
// which in general is just storing the values as raw bytes of the appropriate size
type PlainFloat32Encoder struct {
	encoder

	bitSetReader utils.SetBitRunReader
}

// Put encodes a slice of values into the underlying buffer
func (enc *PlainFloat32Encoder) Put(in []float32) {
	writeFloat32LE(&enc.encoder, in)
}

// PutSpaced encodes a slice of values into the underlying buffer which are spaced out
// including null values defined by the validBits bitmap starting at a given bit offset.
// the values are first compressed by having the null slots removed before writing to the buffer
func (enc *PlainFloat32Encoder) PutSpaced(in []float32, validBits []byte, validBitsOffset int64) {
	nbytes := arrow.Float32Traits.BytesRequired(len(in))
	enc.ReserveForWrite(nbytes)

	if enc.bitSetReader == nil {
		enc.bitSetReader = utils.NewSetBitRunReader(validBits, validBitsOffset, int64(len(in)))
	} else {
		enc.bitSetReader.Reset(validBits, validBitsOffset, int64(len(in)))
	}

	for {
		run := enc.bitSetReader.NextRun()
		if run.Length == 0 {
			break
		}
		enc.Put(in[int(run.Pos):int(run.Pos+run.Length)])
	}
}

// Type returns the underlying physical type this encoder is able to encode
func (PlainFloat32Encoder) Type() parquet.Type {
	return parquet.Types.Float
}

// PlainFloat32Decoder is a decoder specifically for decoding Plain Encoding data
// of float32 type.
type PlainFloat32Decoder struct {
	decoder

	bitSetReader utils.SetBitRunReader
}

// Type returns the physical type this decoder is able to decode for
func (PlainFloat32Decoder) Type() parquet.Type {
	return parquet.Types.Float
}

// Decode populates the given slice with values from the data to be decoded,
// decoding the min(len(out), remaining values).
// It returns the number of values actually decoded and any error encountered.
func (dec *PlainFloat32Decoder) Decode(out []float32) (int, error) {
	max := utils.MinInt(len(out), dec.nvals)
	nbytes := int64(max) * int64(arrow.Float32SizeBytes)
	if nbytes > int64(len(dec.data)) || nbytes > math.MaxInt32 {
		return 0, xerrors.Errorf("parquet: eof exception decode plain Float32, nvals: %d, nbytes: %d, datalen: %d", dec.nvals, nbytes, len(dec.data))
	}

	copyFromFloat32LE(out, dec.data[:nbytes])
	dec.data = dec.data[nbytes:]
	dec.nvals -= max
	return max, nil
}

// DecodeSpaced is the same as decode, except it expands the data out to leave spaces for null values
// as defined by the bitmap provided.
func (dec *PlainFloat32Decoder) DecodeSpaced(out []float32, nullCount int, validBits []byte, validBitsOffset int64) (int, error) {
	toread := len(out) - nullCount
	values, err := dec.Decode(out[:toread])
	if err != nil {
		return 0, err
	}
	if values != toread {
		return 0, xerrors.New("parquet: number of values / definition levels read did not match")
	}

	nvalues := len(out)
	if nullCount == 0 {
		return nvalues, nil
	}

	idxDecode := nvalues - nullCount
	if dec.bitSetReader == nil {
		dec.bitSetReader = utils.NewReverseSetBitRunReader(validBits, validBitsOffset, int64(nvalues))
	} else {
		dec.bitSetReader.Reset(validBits, validBitsOffset, int64(nvalues))
	}

	for {
		run := dec.bitSetReader.NextRun()
		if run.Length == 0 {
			break
		}

		idxDecode -= int(run.Length)
		copy(out[int(run.Pos):], out[idxDecode:idxDecode+int(run.Length)])
	}
	return nvalues, nil
}

// PlainFloat64Encoder is an encoder for float64 values using Plain Encoding
// which in general is just storing the values as raw bytes of the appropriate size
type PlainFloat64Encoder struct {
	encoder

	bitSetReader utils.SetBitRunReader
}

// Put encodes a slice of values into the underlying buffer
func (enc *PlainFloat64Encoder) Put(in []float64) {
	writeFloat64LE(&enc.encoder, in)
}

// PutSpaced encodes a slice of values into the underlying buffer which are spaced out
// including null values defined by the validBits bitmap starting at a given bit offset.
// the values are first compressed by having the null slots removed before writing to the buffer
func (enc *PlainFloat64Encoder) PutSpaced(in []float64, validBits []byte, validBitsOffset int64) {
	nbytes := arrow.Float64Traits.BytesRequired(len(in))
	enc.ReserveForWrite(nbytes)

	if enc.bitSetReader == nil {
		enc.bitSetReader = utils.NewSetBitRunReader(validBits, validBitsOffset, int64(len(in)))
	} else {
		enc.bitSetReader.Reset(validBits, validBitsOffset, int64(len(in)))
	}

	for {
		run := enc.bitSetReader.NextRun()
		if run.Length == 0 {
			break
		}
		enc.Put(in[int(run.Pos):int(run.Pos+run.Length)])
	}
}

// Type returns the underlying physical type this encoder is able to encode
func (PlainFloat64Encoder) Type() parquet.Type {
	return parquet.Types.Double
}

// PlainFloat64Decoder is a decoder specifically for decoding Plain Encoding data
// of float64 type.
type PlainFloat64Decoder struct {
	decoder

	bitSetReader utils.SetBitRunReader
}

// Type returns the physical type this decoder is able to decode for
func (PlainFloat64Decoder) Type() parquet.Type {
	return parquet.Types.Double
}

// Decode populates the given slice with values from the data to be decoded,
// decoding the min(len(out), remaining values).
// It returns the number of values actually decoded and any error encountered.
func (dec *PlainFloat64Decoder) Decode(out []float64) (int, error) {
	max := utils.MinInt(len(out), dec.nvals)
	nbytes := int64(max) * int64(arrow.Float64SizeBytes)
	if nbytes > int64(len(dec.data)) || nbytes > math.MaxInt32 {
		return 0, xerrors.Errorf("parquet: eof exception decode plain Float64, nvals: %d, nbytes: %d, datalen: %d", dec.nvals, nbytes, len(dec.data))
	}

	copyFromFloat64LE(out, dec.data[:nbytes])
	dec.data = dec.data[nbytes:]
	dec.nvals -= max
	return max, nil
}

// DecodeSpaced is the same as decode, except it expands the data out to leave spaces for null values
// as defined by the bitmap provided.
func (dec *PlainFloat64Decoder) DecodeSpaced(out []float64, nullCount int, validBits []byte, validBitsOffset int64) (int, error) {
	toread := len(out) - nullCount
	values, err := dec.Decode(out[:toread])
	if err != nil {
		return 0, err
	}
	if values != toread {
		return 0, xerrors.New("parquet: number of values / definition levels read did not match")
	}

	nvalues := len(out)
	if nullCount == 0 {
		return nvalues, nil
	}

	idxDecode := nvalues - nullCount
	if dec.bitSetReader == nil {
		dec.bitSetReader = utils.NewReverseSetBitRunReader(validBits, validBitsOffset, int64(nvalues))
	} else {
		dec.bitSetReader.Reset(validBits, validBitsOffset, int64(nvalues))
	}

	for {
		run := dec.bitSetReader.NextRun()
		if run.Length == 0 {
			break
		}

		idxDecode -= int(run.Length)
		copy(out[int(run.Pos):], out[idxDecode:idxDecode+int(run.Length)])
	}
	return nvalues, nil
}
