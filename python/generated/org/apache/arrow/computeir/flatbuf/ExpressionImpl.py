# automatically generated by the FlatBuffers compiler, do not modify

# namespace: flatbuf

# Various expression types
#
# WindowCall is a separate variant
# due to special options for each that don't apply to generic
# function calls. Again this is done to make it easier
# for consumers to deal with the structure of the operation
class ExpressionImpl(object):
    NONE = 0
    Literal = 1
    FieldRef = 2
    Call = 3
    ConditionalCase = 4
    SimpleCase = 5
    WindowCall = 6

