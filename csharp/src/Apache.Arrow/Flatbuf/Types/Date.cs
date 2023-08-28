// <auto-generated>
//  automatically generated by the FlatBuffers compiler, do not modify
// </auto-generated>

namespace Apache.Arrow.Flatbuf
{

using global::System;
using global::System.Collections.Generic;
using global::Google.FlatBuffers;

/// Date is either a 32-bit or 64-bit signed integer type representing an
/// elapsed time since UNIX epoch (1970-01-01), stored in either of two units:
///
/// * Milliseconds (64 bits) indicating UNIX time elapsed since the epoch (no
///   leap seconds), where the values are evenly divisible by 86400000
/// * Days (32 bits) since the UNIX epoch
internal struct Date : IFlatbufferObject
{
  private Table __p;
  public ByteBuffer ByteBuffer { get { return __p.bb; } }
  public static void ValidateVersion() { FlatBufferConstants.FLATBUFFERS_23_5_9(); }
  public static Date GetRootAsDate(ByteBuffer _bb) { return GetRootAsDate(_bb, new Date()); }
  public static Date GetRootAsDate(ByteBuffer _bb, Date obj) { return (obj.__assign(_bb.GetInt(_bb.Position) + _bb.Position, _bb)); }
  public void __init(int _i, ByteBuffer _bb) { __p = new Table(_i, _bb); }
  public Date __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  public DateUnit Unit { get { int o = __p.__offset(4); return o != 0 ? (DateUnit)__p.bb.GetShort(o + __p.bb_pos) : DateUnit.MILLISECOND; } }

  public static Offset<Date> CreateDate(FlatBufferBuilder builder,
      DateUnit unit = DateUnit.MILLISECOND) {
    builder.StartTable(1);
    Date.AddUnit(builder, unit);
    return Date.EndDate(builder);
  }

  public static void StartDate(FlatBufferBuilder builder) { builder.StartTable(1); }
  public static void AddUnit(FlatBufferBuilder builder, DateUnit unit) { builder.AddShort(0, (short)unit, 1); }
  public static Offset<Date> EndDate(FlatBufferBuilder builder) {
    int o = builder.EndTable();
    return new Offset<Date>(o);
  }
}


static internal class DateVerify
{
  static public bool Verify(Google.FlatBuffers.Verifier verifier, uint tablePos)
  {
    return verifier.VerifyTableStart(tablePos)
      && verifier.VerifyField(tablePos, 4 /*Unit*/, 2 /*DateUnit*/, 2, false)
      && verifier.VerifyTableEnd(tablePos);
  }
}

}
