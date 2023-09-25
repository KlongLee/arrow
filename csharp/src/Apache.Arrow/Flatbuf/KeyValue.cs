// <auto-generated>
//  automatically generated by the FlatBuffers compiler, do not modify
// </auto-generated>

namespace Apache.Arrow.Flatbuf
{

using global::System;
using global::System.Collections.Generic;
using global::Google.FlatBuffers;

/// ----------------------------------------------------------------------
/// user defined key value pairs to add custom metadata to arrow
/// key namespacing is the responsibility of the user
internal struct KeyValue : IFlatbufferObject
{
  private Table __p;
  public ByteBuffer ByteBuffer { get { return __p.bb; } }
  public static void ValidateVersion() { FlatBufferConstants.FLATBUFFERS_23_5_9(); }
  public static KeyValue GetRootAsKeyValue(ByteBuffer _bb) { return GetRootAsKeyValue(_bb, new KeyValue()); }
  public static KeyValue GetRootAsKeyValue(ByteBuffer _bb, KeyValue obj) { return (obj.__assign(_bb.GetInt(_bb.Position) + _bb.Position, _bb)); }
  public void __init(int _i, ByteBuffer _bb) { __p = new Table(_i, _bb); }
  public KeyValue __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  public string Key { get { int o = __p.__offset(4); return o != 0 ? __p.__string(o + __p.bb_pos) : null; } }
#if ENABLE_SPAN_T
  public Span<byte> GetKeyBytes() { return __p.__vector_as_span<byte>(4, 1); }
#else
  public ArraySegment<byte>? GetKeyBytes() { return __p.__vector_as_arraysegment(4); }
#endif
  public byte[] GetKeyArray() { return __p.__vector_as_array<byte>(4); }
  public string Value { get { int o = __p.__offset(6); return o != 0 ? __p.__string(o + __p.bb_pos) : null; } }
#if ENABLE_SPAN_T
  public Span<byte> GetValueBytes() { return __p.__vector_as_span<byte>(6, 1); }
#else
  public ArraySegment<byte>? GetValueBytes() { return __p.__vector_as_arraysegment(6); }
#endif
  public byte[] GetValueArray() { return __p.__vector_as_array<byte>(6); }

  public static Offset<KeyValue> CreateKeyValue(FlatBufferBuilder builder,
      StringOffset keyOffset = default(StringOffset),
      StringOffset valueOffset = default(StringOffset)) {
    builder.StartTable(2);
    KeyValue.AddValue(builder, valueOffset);
    KeyValue.AddKey(builder, keyOffset);
    return KeyValue.EndKeyValue(builder);
  }

  public static void StartKeyValue(FlatBufferBuilder builder) { builder.StartTable(2); }
  public static void AddKey(FlatBufferBuilder builder, StringOffset keyOffset) { builder.AddOffset(0, keyOffset.Value, 0); }
  public static void AddValue(FlatBufferBuilder builder, StringOffset valueOffset) { builder.AddOffset(1, valueOffset.Value, 0); }
  public static Offset<KeyValue> EndKeyValue(FlatBufferBuilder builder) {
    int o = builder.EndTable();
    return new Offset<KeyValue>(o);
  }
}


static internal class KeyValueVerify
{
  static public bool Verify(Google.FlatBuffers.Verifier verifier, uint tablePos)
  {
    return verifier.VerifyTableStart(tablePos)
      && verifier.VerifyString(tablePos, 4 /*Key*/, false)
      && verifier.VerifyString(tablePos, 6 /*Value*/, false)
      && verifier.VerifyTableEnd(tablePos);
  }
}

}
