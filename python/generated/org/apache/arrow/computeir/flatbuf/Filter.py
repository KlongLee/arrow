# automatically generated by the FlatBuffers compiler, do not modify

# namespace: flatbuf

import flatbuffers
from flatbuffers.compat import import_numpy
np = import_numpy()

# Filter operation
class Filter(object):
    __slots__ = ['_tab']

    @classmethod
    def GetRootAsFilter(cls, buf, offset):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        x = Filter()
        x.Init(buf, n + offset)
        return x

    # Filter
    def Init(self, buf, pos):
        self._tab = flatbuffers.table.Table(buf, pos)

    # Common options
    # Filter
    def Base(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            x = self._tab.Indirect(o + self._tab.Pos)
            from org.apache.arrow.computeir.flatbuf.RelBase import RelBase
            obj = RelBase()
            obj.Init(self._tab.Bytes, x)
            return obj
        return None

    # Child relation
    # Filter
    def Rel(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        if o != 0:
            x = self._tab.Indirect(o + self._tab.Pos)
            from org.apache.arrow.computeir.flatbuf.Relation import Relation
            obj = Relation()
            obj.Init(self._tab.Bytes, x)
            return obj
        return None

    # The expression which will be evaluated against input rows
    # to determine whether they should be excluded from the
    # filter relation's output.
    # Filter
    def Predicate(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(8))
        if o != 0:
            x = self._tab.Indirect(o + self._tab.Pos)
            from org.apache.arrow.computeir.flatbuf.Expression import Expression
            obj = Expression()
            obj.Init(self._tab.Bytes, x)
            return obj
        return None

def FilterStart(builder): builder.StartObject(3)
def FilterAddBase(builder, base): builder.PrependUOffsetTRelativeSlot(0, flatbuffers.number_types.UOffsetTFlags.py_type(base), 0)
def FilterAddRel(builder, rel): builder.PrependUOffsetTRelativeSlot(1, flatbuffers.number_types.UOffsetTFlags.py_type(rel), 0)
def FilterAddPredicate(builder, predicate): builder.PrependUOffsetTRelativeSlot(2, flatbuffers.number_types.UOffsetTFlags.py_type(predicate), 0)
def FilterEnd(builder): return builder.EndObject()
