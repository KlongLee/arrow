// <auto-generated>
//  automatically generated by the FlatBuffers compiler, do not modify
// </auto-generated>

namespace Apache.Arrow.Flatbuf
{

using global::System;
using global::System.Collections.Generic;
using global::Google.FlatBuffers;

/// Time is either a 32-bit or 64-bit signed integer type representing an
/// elapsed time since midnight, stored in either of four units: seconds,
/// milliseconds, microseconds or nanoseconds.
///
/// The integer `bitWidth` depends on the `unit` and must be one of the following:
/// * SECOND and MILLISECOND: 32 bits
/// * MICROSECOND and NANOSECOND: 64 bits
///
/// The allowed values are between 0 (inclusive) and 86400 (=24*60*60) seconds
/// (exclusive), adjusted for the time unit (for example, up to 86400000
/// exclusive for the MILLISECOND unit).
/// This definition doesn't allow for leap seconds. Time values from
/// measurements with leap seconds will need to be corrected when ingesting
/// into Arrow (for example by replacing the value 86400 with 86399).
internal struct Time : IFlatbufferObject
{
  private Table __p;
  public ByteBuffer ByteBuffer { get { return __p.bb; } }
  public static void ValidateVersion() { FlatBufferConstants.FLATBUFFERS_23_5_9(); }
  public static Time GetRootAsTime(ByteBuffer _bb) { return GetRootAsTime(_bb, new Time()); }
  public static Time GetRootAsTime(ByteBuffer _bb, Time obj) { return (obj.__assign(_bb.GetInt(_bb.Position) + _bb.Position, _bb)); }
  public void __init(int _i, ByteBuffer _bb) { __p = new Table(_i, _bb); }
  public Time __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  public TimeUnit Unit { get { int o = __p.__offset(4); return o != 0 ? (TimeUnit)__p.bb.GetShort(o + __p.bb_pos) : TimeUnit.MILLISECOND; } }
  public int BitWidth { get { int o = __p.__offset(6); return o != 0 ? __p.bb.GetInt(o + __p.bb_pos) : (int)32; } }

  public static Offset<Time> CreateTime(FlatBufferBuilder builder,
      TimeUnit unit = TimeUnit.MILLISECOND,
      int bitWidth = 32) {
    builder.StartTable(2);
    Time.AddBitWidth(builder, bitWidth);
    Time.AddUnit(builder, unit);
    return Time.EndTime(builder);
  }

  public static void StartTime(FlatBufferBuilder builder) { builder.StartTable(2); }
  public static void AddUnit(FlatBufferBuilder builder, TimeUnit unit) { builder.AddShort(0, (short)unit, 1); }
  public static void AddBitWidth(FlatBufferBuilder builder, int bitWidth) { builder.AddInt(1, bitWidth, 32); }
  public static Offset<Time> EndTime(FlatBufferBuilder builder) {
    int o = builder.EndTable();
    return new Offset<Time>(o);
  }
}


static internal class TimeVerify
{
  static public bool Verify(Google.FlatBuffers.Verifier verifier, uint tablePos)
  {
    return verifier.VerifyTableStart(tablePos)
      && verifier.VerifyField(tablePos, 4 /*Unit*/, 2 /*TimeUnit*/, 2, false)
      && verifier.VerifyField(tablePos, 6 /*BitWidth*/, 4 /*int*/, 4, false)
      && verifier.VerifyTableEnd(tablePos);
  }
}

}
