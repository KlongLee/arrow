// <auto-generated>
//  automatically generated by the FlatBuffers compiler, do not modify
// </auto-generated>

namespace Apache.Arrow.Flatbuf
{

using global::System;
using global::FlatBuffers;

public struct FloatingPoint : IFlatbufferObject
{
  private Table __p;
  public ByteBuffer ByteBuffer { get { return __p.bb; } }
  public static FloatingPoint GetRootAsFloatingPoint(ByteBuffer _bb) { return GetRootAsFloatingPoint(_bb, new FloatingPoint()); }
  public static FloatingPoint GetRootAsFloatingPoint(ByteBuffer _bb, FloatingPoint obj) { return (obj.__assign(_bb.GetInt(_bb.Position) + _bb.Position, _bb)); }
  public void __init(int _i, ByteBuffer _bb) { __p.bb_pos = _i; __p.bb = _bb; }
  public FloatingPoint __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  public Precision Precision { get { int o = __p.__offset(4); return o != 0 ? (Precision)__p.bb.GetShort(o + __p.bb_pos) : Precision.HALF; } }

  public static Offset<FloatingPoint> CreateFloatingPoint(FlatBufferBuilder builder,
      Precision precision = Precision.HALF) {
    builder.StartObject(1);
    FloatingPoint.AddPrecision(builder, precision);
    return FloatingPoint.EndFloatingPoint(builder);
  }

  public static void StartFloatingPoint(FlatBufferBuilder builder) { builder.StartObject(1); }
  public static void AddPrecision(FlatBufferBuilder builder, Precision precision) { builder.AddShort(0, (short)precision, 0); }
  public static Offset<FloatingPoint> EndFloatingPoint(FlatBufferBuilder builder) {
    int o = builder.EndObject();
    return new Offset<FloatingPoint>(o);
  }
};


}
