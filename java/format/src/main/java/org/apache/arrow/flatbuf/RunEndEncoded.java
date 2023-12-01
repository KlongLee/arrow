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
// 
// automatically generated by the FlatBuffers compiler, do not modify

package org.apache.arrow.flatbuf;

import com.google.flatbuffers.BaseVector;
import com.google.flatbuffers.BooleanVector;
import com.google.flatbuffers.ByteVector;
import com.google.flatbuffers.Constants;
import com.google.flatbuffers.DoubleVector;
import com.google.flatbuffers.FlatBufferBuilder;
import com.google.flatbuffers.FloatVector;
import com.google.flatbuffers.IntVector;
import com.google.flatbuffers.LongVector;
import com.google.flatbuffers.ShortVector;
import com.google.flatbuffers.StringVector;
import com.google.flatbuffers.Struct;
import com.google.flatbuffers.Table;
import com.google.flatbuffers.UnionVector;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Contains two child arrays, run_ends and values.
 * The run_ends child array must be a 16/32/64-bit integer array
 * which encodes the indices at which the run with the value in 
 * each corresponding index in the values child array ends.
 * Like list/struct types, the value array can be of any type.
 */
@SuppressWarnings("unused")
public final class RunEndEncoded extends Table {
  public static void ValidateVersion() { Constants.FLATBUFFERS_23_5_26(); }
  public static RunEndEncoded getRootAsRunEndEncoded(ByteBuffer _bb) { return getRootAsRunEndEncoded(_bb, new RunEndEncoded()); }
  public static RunEndEncoded getRootAsRunEndEncoded(ByteBuffer _bb, RunEndEncoded obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__assign(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public void __init(int _i, ByteBuffer _bb) { __reset(_i, _bb); }
  public RunEndEncoded __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }


  public static void startRunEndEncoded(FlatBufferBuilder builder) { builder.startTable(0); }
  public static int endRunEndEncoded(FlatBufferBuilder builder) {
    int o = builder.endTable();
    return o;
  }

  public static final class Vector extends BaseVector {
    public Vector __assign(int _vector, int _element_size, ByteBuffer _bb) { __reset(_vector, _element_size, _bb); return this; }

    public RunEndEncoded get(int j) { return get(new RunEndEncoded(), j); }
    public RunEndEncoded get(RunEndEncoded obj, int j) {  return obj.__assign(__indirect(__element(j), bb), bb); }
  }
}

