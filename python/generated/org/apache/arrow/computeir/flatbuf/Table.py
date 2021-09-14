# automatically generated by the FlatBuffers compiler, do not modify

# namespace: flatbuf

import flatbuffers
from flatbuffers.compat import import_numpy
np = import_numpy()

# A table read
class Table(object):
    __slots__ = ['_tab']

    @classmethod
    def GetRootAsTable(cls, buf, offset):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        x = Table()
        x.Init(buf, n + offset)
        return x

    # Table
    def Init(self, buf, pos):
        self._tab = flatbuffers.table.Table(buf, pos)

    # Table
    def Base(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            x = self._tab.Indirect(o + self._tab.Pos)
            from org.apache.arrow.computeir.flatbuf.RelBase import RelBase
            obj = RelBase()
            obj.Init(self._tab.Bytes, x)
            return obj
        return None

    # Table
    def Name(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        if o != 0:
            return self._tab.String(o + self._tab.Pos)
        return None

    # Table
    def Schema(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(8))
        if o != 0:
            x = self._tab.Indirect(o + self._tab.Pos)
            from org.apache.arrow.flatbuf.Schema import Schema
            obj = Schema()
            obj.Init(self._tab.Bytes, x)
            return obj
        return None

def TableStart(builder): builder.StartObject(3)
def TableAddBase(builder, base): builder.PrependUOffsetTRelativeSlot(0, flatbuffers.number_types.UOffsetTFlags.py_type(base), 0)
def TableAddName(builder, name): builder.PrependUOffsetTRelativeSlot(1, flatbuffers.number_types.UOffsetTFlags.py_type(name), 0)
def TableAddSchema(builder, schema): builder.PrependUOffsetTRelativeSlot(2, flatbuffers.number_types.UOffsetTFlags.py_type(schema), 0)
def TableEnd(builder): return builder.EndObject()
