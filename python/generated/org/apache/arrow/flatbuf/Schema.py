# automatically generated by the FlatBuffers compiler, do not modify

# namespace: flatbuf

import flatbuffers
from flatbuffers.compat import import_numpy
np = import_numpy()

# ----------------------------------------------------------------------
# A Schema describes the columns in a row batch
class Schema(object):
    __slots__ = ['_tab']

    @classmethod
    def GetRootAsSchema(cls, buf, offset):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        x = Schema()
        x.Init(buf, n + offset)
        return x

    # Schema
    def Init(self, buf, pos):
        self._tab = flatbuffers.table.Table(buf, pos)

    # endianness of the buffer
    # it is Little Endian by default
    # if endianness doesn't match the underlying system then the vectors need to be converted
    # Schema
    def Endianness(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            return self._tab.Get(flatbuffers.number_types.Int16Flags, o + self._tab.Pos)
        return 0

    # Schema
    def Fields(self, j):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        if o != 0:
            x = self._tab.Vector(o)
            x += flatbuffers.number_types.UOffsetTFlags.py_type(j) * 4
            x = self._tab.Indirect(x)
            from org.apache.arrow.flatbuf.Field import Field
            obj = Field()
            obj.Init(self._tab.Bytes, x)
            return obj
        return None

    # Schema
    def FieldsLength(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        if o != 0:
            return self._tab.VectorLen(o)
        return 0

    # Schema
    def FieldsIsNone(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        return o == 0

    # Schema
    def CustomMetadata(self, j):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(8))
        if o != 0:
            x = self._tab.Vector(o)
            x += flatbuffers.number_types.UOffsetTFlags.py_type(j) * 4
            x = self._tab.Indirect(x)
            from org.apache.arrow.flatbuf.KeyValue import KeyValue
            obj = KeyValue()
            obj.Init(self._tab.Bytes, x)
            return obj
        return None

    # Schema
    def CustomMetadataLength(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(8))
        if o != 0:
            return self._tab.VectorLen(o)
        return 0

    # Schema
    def CustomMetadataIsNone(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(8))
        return o == 0

    # Features used in the stream/file.
    # Schema
    def Features(self, j):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(10))
        if o != 0:
            a = self._tab.Vector(o)
            return self._tab.Get(flatbuffers.number_types.Int64Flags, a + flatbuffers.number_types.UOffsetTFlags.py_type(j * 8))
        return 0

    # Schema
    def FeaturesAsNumpy(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(10))
        if o != 0:
            return self._tab.GetVectorAsNumpy(flatbuffers.number_types.Int64Flags, o)
        return 0

    # Schema
    def FeaturesLength(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(10))
        if o != 0:
            return self._tab.VectorLen(o)
        return 0

    # Schema
    def FeaturesIsNone(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(10))
        return o == 0

def SchemaStart(builder): builder.StartObject(4)
def SchemaAddEndianness(builder, endianness): builder.PrependInt16Slot(0, endianness, 0)
def SchemaAddFields(builder, fields): builder.PrependUOffsetTRelativeSlot(1, flatbuffers.number_types.UOffsetTFlags.py_type(fields), 0)
def SchemaStartFieldsVector(builder, numElems): return builder.StartVector(4, numElems, 4)
def SchemaAddCustomMetadata(builder, customMetadata): builder.PrependUOffsetTRelativeSlot(2, flatbuffers.number_types.UOffsetTFlags.py_type(customMetadata), 0)
def SchemaStartCustomMetadataVector(builder, numElems): return builder.StartVector(4, numElems, 4)
def SchemaAddFeatures(builder, features): builder.PrependUOffsetTRelativeSlot(3, flatbuffers.number_types.UOffsetTFlags.py_type(features), 0)
def SchemaStartFeaturesVector(builder, numElems): return builder.StartVector(8, numElems, 8)
def SchemaEnd(builder): return builder.EndObject()
