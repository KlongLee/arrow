// automatically generated by the FlatBuffers compiler, do not modify

import * as flatbuffers from 'flatbuffers';

import { Buffer } from './buffer.js';
import { Int } from './int.js';


/**
 * ----------------------------------------------------------------------
 * EXPERIMENTAL: Data structures for sparse tensors
 * Coordinate (COO) format of sparse tensor index.
 *
 * COO's index list are represented as a NxM matrix,
 * where N is the number of non-zero values,
 * and M is the number of dimensions of a sparse tensor.
 *
 * indicesBuffer stores the location and size of the data of this indices
 * matrix.  The value type and the stride of the indices matrix is
 * specified in indicesType and indicesStrides fields.
 *
 * For example, let X be a 2x3x4x5 tensor, and it has the following
 * 6 non-zero values:
 * ```text
 *   X[0, 1, 2, 0] := 1
 *   X[1, 1, 2, 3] := 2
 *   X[0, 2, 1, 0] := 3
 *   X[0, 1, 3, 0] := 4
 *   X[0, 1, 2, 1] := 5
 *   X[1, 2, 0, 4] := 6
 * ```
 * In COO format, the index matrix of X is the following 4x6 matrix:
 * ```text
 *   [[0, 0, 0, 0, 1, 1],
 *    [1, 1, 1, 2, 1, 2],
 *    [2, 2, 3, 1, 2, 0],
 *    [0, 1, 0, 0, 3, 4]]
 * ```
 * When isCanonical is true, the indices is sorted in lexicographical order
 * (row-major order), and it does not have duplicated entries.  Otherwise,
 * the indices may not be sorted, or may have duplicated entries.
 */
export class SparseTensorIndexCOO {
  bb: flatbuffers.ByteBuffer|null = null;
  bb_pos = 0;
__init(i:number, bb:flatbuffers.ByteBuffer):SparseTensorIndexCOO {
  this.bb_pos = i;
  this.bb = bb;
  return this;
}

static getRootAsSparseTensorIndexCOO(bb:flatbuffers.ByteBuffer, obj?:SparseTensorIndexCOO):SparseTensorIndexCOO {
  return (obj || new SparseTensorIndexCOO()).__init(bb.readInt32(bb.position()) + bb.position(), bb);
}

static getSizePrefixedRootAsSparseTensorIndexCOO(bb:flatbuffers.ByteBuffer, obj?:SparseTensorIndexCOO):SparseTensorIndexCOO {
  bb.setPosition(bb.position() + flatbuffers.SIZE_PREFIX_LENGTH);
  return (obj || new SparseTensorIndexCOO()).__init(bb.readInt32(bb.position()) + bb.position(), bb);
}

/**
 * The type of values in indicesBuffer
 */
indicesType(obj?:Int):Int|null {
  const offset = this.bb!.__offset(this.bb_pos, 4);
  return offset ? (obj || new Int()).__init(this.bb!.__indirect(this.bb_pos + offset), this.bb!) : null;
}

/**
 * Non-negative byte offsets to advance one value cell along each dimension
 * If omitted, default to row-major order (C-like).
 */
indicesStrides(index: number):flatbuffers.Long|null {
  const offset = this.bb!.__offset(this.bb_pos, 6);
  return offset ? this.bb!.readInt64(this.bb!.__vector(this.bb_pos + offset) + index * 8) : this.bb!.createLong(0, 0);
}

indicesStridesLength():number {
  const offset = this.bb!.__offset(this.bb_pos, 6);
  return offset ? this.bb!.__vector_len(this.bb_pos + offset) : 0;
}

/**
 * The location and size of the indices matrix's data
 */
indicesBuffer(obj?:Buffer):Buffer|null {
  const offset = this.bb!.__offset(this.bb_pos, 8);
  return offset ? (obj || new Buffer()).__init(this.bb_pos + offset, this.bb!) : null;
}

/**
 * This flag is true if and only if the indices matrix is sorted in
 * row-major order, and does not have duplicated entries.
 * This sort order is the same as of Tensorflow's SparseTensor,
 * but it is inverse order of SciPy's canonical coo_matrix
 * (SciPy employs column-major order for its coo_matrix).
 */
isCanonical():boolean {
  const offset = this.bb!.__offset(this.bb_pos, 10);
  return offset ? !!this.bb!.readInt8(this.bb_pos + offset) : false;
}

static startSparseTensorIndexCOO(builder:flatbuffers.Builder) {
  builder.startObject(4);
}

static addIndicesType(builder:flatbuffers.Builder, indicesTypeOffset:flatbuffers.Offset) {
  builder.addFieldOffset(0, indicesTypeOffset, 0);
}

static addIndicesStrides(builder:flatbuffers.Builder, indicesStridesOffset:flatbuffers.Offset) {
  builder.addFieldOffset(1, indicesStridesOffset, 0);
}

static createIndicesStridesVector(builder:flatbuffers.Builder, data:flatbuffers.Long[]):flatbuffers.Offset {
  builder.startVector(8, data.length, 8);
  for (let i = data.length - 1; i >= 0; i--) {
    builder.addInt64(data[i]!);
  }
  return builder.endVector();
}

static startIndicesStridesVector(builder:flatbuffers.Builder, numElems:number) {
  builder.startVector(8, numElems, 8);
}

static addIndicesBuffer(builder:flatbuffers.Builder, indicesBufferOffset:flatbuffers.Offset) {
  builder.addFieldStruct(2, indicesBufferOffset, 0);
}

static addIsCanonical(builder:flatbuffers.Builder, isCanonical:boolean) {
  builder.addFieldInt8(3, +isCanonical, +false);
}

static endSparseTensorIndexCOO(builder:flatbuffers.Builder):flatbuffers.Offset {
  const offset = builder.endObject();
  builder.requiredField(offset, 4) // indicesType
  builder.requiredField(offset, 8) // indicesBuffer
  return offset;
}

}
