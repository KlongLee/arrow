// <auto-generated>
//  automatically generated by the FlatBuffers compiler, do not modify
// </auto-generated>

namespace Apache.Arrow.Flatbuf
{

using global::System;
using global::FlatBuffers;

/// ----------------------------------------------------------------------
/// A Schema describes the columns in a row batch
internal struct Schema : IFlatbufferObject
{
  private Table __p;
  public ByteBuffer ByteBuffer { get { return __p.bb; } }
  public static Schema GetRootAsSchema(ByteBuffer _bb) { return GetRootAsSchema(_bb, new Schema()); }
  public static Schema GetRootAsSchema(ByteBuffer _bb, Schema obj) { return (obj.__assign(_bb.GetInt(_bb.Position) + _bb.Position, _bb)); }
  public void __init(int _i, ByteBuffer _bb) { __p.bb_pos = _i; __p.bb = _bb; }
  public Schema __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  /// endianness of the buffer
  /// it is Little Endian by default
  /// if endianness doesn't match the underlying system then the vectors need to be converted
  public Endianness Endianness { get { int o = __p.__offset(4); return o != 0 ? (Endianness)__p.bb.GetShort(o + __p.bb_pos) : Endianness.Little; } }
  public Field? Fields(int j) { int o = __p.__offset(6); return o != 0 ? (Field?)(new Field()).__assign(__p.__indirect(__p.__vector(o) + j * 4), __p.bb) : null; }
  public int FieldsLength { get { int o = __p.__offset(6); return o != 0 ? __p.__vector_len(o) : 0; } }
  public KeyValue? CustomMetadata(int j) { int o = __p.__offset(8); return o != 0 ? (KeyValue?)(new KeyValue()).__assign(__p.__indirect(__p.__vector(o) + j * 4), __p.bb) : null; }
  public int CustomMetadataLength { get { int o = __p.__offset(8); return o != 0 ? __p.__vector_len(o) : 0; } }

  public static Offset<Schema> CreateSchema(FlatBufferBuilder builder,
      Endianness endianness = Endianness.Little,
      VectorOffset fieldsOffset = default(VectorOffset),
      VectorOffset custom_metadataOffset = default(VectorOffset)) {
    builder.StartObject(3);
    Schema.AddCustomMetadata(builder, custom_metadataOffset);
    Schema.AddFields(builder, fieldsOffset);
    Schema.AddEndianness(builder, endianness);
    return Schema.EndSchema(builder);
  }

  public static void StartSchema(FlatBufferBuilder builder) { builder.StartObject(3); }
  public static void AddEndianness(FlatBufferBuilder builder, Endianness endianness) { builder.AddShort(0, (short)endianness, 0); }
  public static void AddFields(FlatBufferBuilder builder, VectorOffset fieldsOffset) { builder.AddOffset(1, fieldsOffset.Value, 0); }
  public static VectorOffset CreateFieldsVector(FlatBufferBuilder builder, Offset<Field>[] data) { builder.StartVector(4, data.Length, 4); for (int i = data.Length - 1; i >= 0; i--) builder.AddOffset(data[i].Value); return builder.EndVector(); }
  public static VectorOffset CreateFieldsVectorBlock(FlatBufferBuilder builder, Offset<Field>[] data) { builder.StartVector(4, data.Length, 4); builder.Add(data); return builder.EndVector(); }
  public static void StartFieldsVector(FlatBufferBuilder builder, int numElems) { builder.StartVector(4, numElems, 4); }
  public static void AddCustomMetadata(FlatBufferBuilder builder, VectorOffset customMetadataOffset) { builder.AddOffset(2, customMetadataOffset.Value, 0); }
  public static VectorOffset CreateCustomMetadataVector(FlatBufferBuilder builder, Offset<KeyValue>[] data) { builder.StartVector(4, data.Length, 4); for (int i = data.Length - 1; i >= 0; i--) builder.AddOffset(data[i].Value); return builder.EndVector(); }
  public static VectorOffset CreateCustomMetadataVectorBlock(FlatBufferBuilder builder, Offset<KeyValue>[] data) { builder.StartVector(4, data.Length, 4); builder.Add(data); return builder.EndVector(); }
  public static void StartCustomMetadataVector(FlatBufferBuilder builder, int numElems) { builder.StartVector(4, numElems, 4); }
  public static Offset<Schema> EndSchema(FlatBufferBuilder builder) {
    int o = builder.EndObject();
    return new Offset<Schema>(o);
  }
  public static void FinishSchemaBuffer(FlatBufferBuilder builder, Offset<Schema> offset) { builder.Finish(offset.Value); }
  public static void FinishSizePrefixedSchemaBuffer(FlatBufferBuilder builder, Offset<Schema> offset) { builder.FinishSizePrefixed(offset.Value); }
};


}
