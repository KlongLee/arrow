# automatically generated by the FlatBuffers compiler, do not modify

# namespace: flatbuf

import flatbuffers
from flatbuffers.compat import import_numpy
np = import_numpy()

class Int32Literal(object):
    __slots__ = ['_tab']

    @classmethod
    def GetRootAsInt32Literal(cls, buf, offset):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        x = Int32Literal()
        x.Init(buf, n + offset)
        return x

    # Int32Literal
    def Init(self, buf, pos):
        self._tab = flatbuffers.table.Table(buf, pos)

    # Int32Literal
    def Value(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            return self._tab.Get(flatbuffers.number_types.Int32Flags, o + self._tab.Pos)
        return 0

def Int32LiteralStart(builder): builder.StartObject(1)
def Int32LiteralAddValue(builder, value): builder.PrependInt32Slot(0, value, 0)
def Int32LiteralEnd(builder): return builder.EndObject()
