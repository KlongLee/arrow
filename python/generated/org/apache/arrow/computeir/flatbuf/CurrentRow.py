# automatically generated by the FlatBuffers compiler, do not modify

# namespace: flatbuf

import flatbuffers
from flatbuffers.compat import import_numpy
np = import_numpy()

# Boundary is the current row
class CurrentRow(object):
    __slots__ = ['_tab']

    @classmethod
    def GetRootAsCurrentRow(cls, buf, offset):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        x = CurrentRow()
        x.Init(buf, n + offset)
        return x

    # CurrentRow
    def Init(self, buf, pos):
        self._tab = flatbuffers.table.Table(buf, pos)

def CurrentRowStart(builder): builder.StartObject(0)
def CurrentRowEnd(builder): return builder.EndObject()
