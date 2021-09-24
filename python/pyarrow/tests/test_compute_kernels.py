import re
from abc import abstractmethod, ABC
from typing import List, Dict, Set, Tuple

import pytest

import pyarrow as pa
import pyarrow.compute as pc


def sample_integral_types():
    return [
        pa.int8(),
        pa.int16(),
        pa.int32(),
        pa.int64(),
        pa.uint8(),
        pa.uint16(),
        pa.uint32(),
        pa.uint64(),
    ]


def sample_signed_integral_types():
    return [
        pa.int8(),
        pa.int16(),
        pa.int32(),
        pa.int64()
    ]


def sample_simple_float_types():
    return [
        pa.float32(),
        pa.float64()
    ]


def sample_decimal_types():
    return [
        pa.decimal128(7, 3),
        pa.decimal128(10, 4)
    ]


def sample_float_types():
    return [
        pa.float32(),
        pa.float64(),
        pa.decimal128(7, 3),
        pa.decimal128(10, 4)
    ]


def sample_simple_numeric_types():
    return sample_integral_types() + sample_simple_float_types()


def sample_numeric_types():
    return sample_integral_types() + sample_float_types()


def sample_signed_numeric_types():
    return sample_signed_integral_types() + sample_float_types()


def sample_timestamp_no_tz_types():
    return [
        pa.timestamp('s'),
        pa.timestamp('ms'),
        pa.timestamp('us'),
        pa.timestamp('ns')
    ]


def sample_timestamptz_types():
    return [
        pa.timestamp('s', 'America/New_York'),
        pa.timestamp('ms', 'America/New_York'),
        pa.timestamp('us', 'America/New_York'),
        pa.timestamp('ns', 'America/New_York'),
        pa.timestamp('s', 'UTC'),
        pa.timestamp('ms', 'UTC'),
        pa.timestamp('us', 'UTC'),
        pa.timestamp('ns', 'UTC')
    ]


def sample_timestamp_types():
    return sample_timestamptz_types() + sample_timestamp_no_tz_types()


def sample_date_only_types():
    return [
        pa.date32(),
        pa.date64()
    ]


def sample_date_types():
    return sample_date_only_types() + sample_timestamp_types()


def sample_time_only_types():
    return [
        pa.time32('s'),
        pa.time32('ms'),
        pa.time64('us'),
        pa.time64('ns')
    ]


def sample_time_types():
    return sample_time_only_types() + sample_timestamp_types()


def sample_temporal_types():
    return sample_date_only_types() + \
        sample_time_only_types() + \
        sample_timestamp_types()


def sample_logical_types():
    return [pa.bool_()]


def sample_bytes_types():
    return [
        pa.binary(),
        pa.binary(32),
        pa.large_binary(),
        pa.string(),
        pa.large_string()
    ]


def sample_fixed_bytes_types():
    return [
        pa.binary(32),
    ]


def sample_string_types():
    return [
        pa.string(),
        pa.large_string()
    ]


def sample_primitive_types():
    return sample_numeric_types() + \
        sample_temporal_types() + \
        sample_timestamp_types() + \
        sample_bytes_types()


def __listify_types(types):
    return [pa.list_(t) for t in types] + [pa.list_(t, 32) for t in types] + [
        pa.large_list(t) for t in types]


def __structify_types(types):
    return [pa.struct([pa.field('data', t)]) for t in types]


def sample_sortable_types():
    return sample_primitive_types()


def sample_list_types():
    return __listify_types(sample_primitive_types() + [pa.null()])


def sample_struct_types():
    return __structify_types(sample_primitive_types() + [pa.null()])


def sample_all_types():
    return sample_primitive_types() + \
        sample_list_types() + \
        sample_struct_types()


type_categories = {
    'boolean': sample_logical_types(),
    'bytes': sample_bytes_types(),
    'date': sample_date_only_types(),
    'datelike': sample_date_types(),
    'decimal': sample_decimal_types(),
    'equatable': sample_sortable_types(),
    'fixed_bytes': sample_fixed_bytes_types(),
    'floating': sample_float_types(),
    'integral': sample_integral_types(),
    'list': sample_list_types(),
    'logical': sample_logical_types(),
    'null': [],
    'numeric': sample_numeric_types(),
    'signed_numeric': sample_signed_numeric_types(),
    'simple_numeric': sample_simple_numeric_types(),
    'sortable': sample_sortable_types(),
    'string': sample_string_types(),
    'struct': sample_struct_types(),
    'temporal': sample_temporal_types(),
    'time': sample_time_only_types(),
    'timelike': sample_time_types(),
    'timestamp': sample_timestamp_no_tz_types(),
    'timestamptz': sample_timestamptz_types(),
    'timestamp_all': sample_timestamp_types(),
}


def get_sample_types(category):
    types = type_categories.get(category, None)
    if types is None:
        raise Exception(f'Unrecognized type category {category}')
    return types + [pa.null()]


class DynamicParameter(ABC):

    def __init__(self, key: str):
        self.key = key

    @abstractmethod
    def compute_type(self, parameters_map: Dict[str, pa.DataType]):
        pass


class DecimalDynamicParameter(DynamicParameter):

    def __init__(self, key, left_name, right_name):
        super(DecimalDynamicParameter, self).__init__(key)
        self.left_name = left_name
        self.right_name = right_name

    def _ensure_decimal(self, type_):
        if not pa.types.is_decimal(type_):
            raise Exception(
                'DECIMAL_* type function was used for a type '
                f'{type_} which is not decimal')

    def compute_type(self, parameters_map):
        left_type = parameters_map[self.left_name]
        right_type = parameters_map[self.right_name]
        if pa.types.is_null(left_type):
            return right_type
        elif pa.types.is_null(right_type):
            return left_type
        self._ensure_decimal(left_type)
        self._ensure_decimal(right_type)
        scale, precision = self._do_compute(left_type.scale,
                                            left_type.precision,
                                            right_type.scale,
                                            right_type.precision)
        if precision <= 38 and pa.types.is_decimal128(
                left_type) and pa.types.is_decimal128(right_type):
            return pa.decimal128(precision, scale)
        else:
            return pa.decimal256(precision, scale)

    @abstractmethod
    def _do_compute(self, s1, p1, s2, p2):
        pass


class DecimalAddDynamicParameter(DecimalDynamicParameter):

    def __init__(self, key, left_name, right_name):
        super(DecimalAddDynamicParameter, self).__init__(
            key, left_name, right_name)

    def _do_compute(self, s1, p1, s2, p2):
        scale = max(s1, s2)
        precision = max(p1 - s1, p2 - s2) + scale + 1
        return scale, precision


class DecimalMultiplyDynamicParameter(DecimalDynamicParameter):

    def __init__(self, key, left_name, right_name):
        super(DecimalMultiplyDynamicParameter, self).__init__(
            key, left_name, right_name)

    def _do_compute(self, s1, p1, s2, p2):
        scale = s1 + s2
        precision = p1 + p2 + 1
        return scale, precision


class DecimalDivideDynamicParameter(DecimalDynamicParameter):

    def __init__(self, key, left_name, right_name):
        super(DecimalDivideDynamicParameter, self).__init__(
            key, left_name, right_name)

    def _do_compute(self, s1, p1, s2, p2):
        scale = max(4, s1 + p2 - s2 + 1)
        precision = p1 - s1 + s2 + scale
        return scale, precision


class StructifyDynamicParameter(DynamicParameter):

    def __init__(self, key):
        super(StructifyDynamicParameter, self).__init__(key)

    def compute_type(self, parameters_map):
        fields = [pa.field(key, value)
                  for key, value in parameters_map.items()]
        return pa.struct(fields)


class WithTzParameter(DynamicParameter):

    def __init__(self, key, source_name):
        super(WithTzParameter, self).__init__(key)
        self.name = source_name

    def compute_type(self, parameters_map: Dict[str, pa.DataType]):
        src_type = parameters_map[self.name]
        if pa.types.is_null(src_type):
            return pa.null()
        return pa.timestamp(src_type.unit, 'UTC')


dynamic_parameter_types = {
    'DECIMAL_ADD': DecimalAddDynamicParameter,
    'DECIMAL_MULTIPLY': DecimalMultiplyDynamicParameter,
    'DECIMAL_DIVIDE': DecimalDivideDynamicParameter,
    'STRUCTIFY': StructifyDynamicParameter,
    'WITH_TZ': WithTzParameter
}


class ConstrainedParameter(ABC):

    def __init__(self, key: str):
        self.key = key

    @abstractmethod
    def sample(self, parameters_map: Dict[str, pa.DataType]) -> List[
            pa.DataType]:
        pass

    @abstractmethod
    def satisfied_with(self, data_type: pa.DataType,
                       parameters_map: Dict[str, pa.DataType]) -> bool:
        pass


class IsListOfGivenType(ConstrainedParameter):

    def __init__(self, key, parameter_name):
        super(IsListOfGivenType, self).__init__(key)
        self.name = parameter_name

    def sample(self, parameters_map):
        type_ = parameters_map[self.name]
        return [
            pa.list_(type_),
            pa.list_(type_, 32)
        ]

    def satisfied_with(self, data_type: pa.DataType,
                       parameters_map: Dict[str, pa.DataType]) -> bool:
        if not pa.types.is_list(data_type):
            return False
        if self.name not in parameters_map:
            return False
        target_type = parameters_map[self.name]
        return target_type == data_type.value_type


class IsFixedSizeListOfGivenType(ConstrainedParameter):

    def __init__(self, key, parameter_name):
        super(IsFixedSizeListOfGivenType, self).__init__(key)
        self.name = parameter_name

    def sample(self, parameters_map):
        type_ = parameters_map[self.name]
        return [
            pa.list_(type_, 32)
        ]

    def satisfied_with(self, data_type: pa.DataType,
                       parameters_map: Dict[str, pa.DataType]) -> bool:
        if not pa.types.is_fixed_size_list(data_type):
            return False
        if self.name not in parameters_map:
            return False
        target_type = parameters_map[self.name]
        return target_type == data_type.value_type


class IsCaseWhen(ConstrainedParameter):

    def __init__(self, key, *args):
        super(IsCaseWhen, self).__init__(key)

    def sample(self, parameters_map):
        fields = []
        for idx in range(len(parameters_map)):
            fields.append(pa.field(f'f{idx}', pa.bool_()))
        return [
            pa.struct(fields)
        ]

    def satisfied_with(self, data_type: pa.DataType,
                       parameters_map: Dict[str, pa.DataType]) -> bool:
        if not pa.types.is_struct(data_type):
            return False
        for field in data_type:
            if not pa.types.is_boolean(field):
                return False
        return True


condition_types = {
    'LIST': IsListOfGivenType,
    'FIXED_SIZE_LIST': IsFixedSizeListOfGivenType,
    'CASE_WHEN': IsCaseWhen
}


class InSetOfTypes(ConstrainedParameter):

    def __init__(self, key, example_types):
        super(InSetOfTypes, self).__init__(key)
        self.example_types = example_types

    def sample(self, _):
        return self.example_types

    def satisfied_with(self, data_type: pa.DataType,
                       parameters_map: Dict[str, pa.DataType]) -> bool:
        return data_type in self.example_types


class IsAnyType(ConstrainedParameter):

    def __init__(self, key):
        super(IsAnyType, self).__init__(key)

    def sample(self, _):
        return sample_all_types()

    def satisfied_with(self, data_type: pa.DataType,
                       parameters_map: Dict[str, pa.DataType]) -> bool:
        return True


def parse_parameter_condition_func(key, value):
    func_name = value[1:value.index('(')].upper()
    func_args = value[value.index('(') + 1:value.index(')')].split(',')
    if func_name in condition_types:
        condition_type = condition_types[func_name]
        return condition_type(key, *func_args)
    else:
        raise Exception(
            f'Unrecognized parameter condition function ({func_name}) on '
            f'key {key}')


def parse_parameter_condition_typed(key, value):
    if value:
        sample_types = get_sample_types(value)
        return InSetOfTypes(key, sample_types)
    else:
        return IsAnyType(key)


def parse_parameter_condition(pstr):
    key, _, value = pstr.partition(':')
    if value.startswith('~'):
        return parse_parameter_condition_func(key, value)
    else:
        return parse_parameter_condition_typed(key, value)


def parse_dynamic_parameter(pstr):
    key, _, value = pstr.partition('=')
    func_name = value[0:value.index('(')].upper()
    func_args = value[value.index('(') + 1:value.index(')')].split('|')
    func_args = [arg for arg in func_args if '...' not in arg]
    if func_name.upper() in dynamic_parameter_types:
        dynamic_parameter_type = dynamic_parameter_types[func_name]
        return dynamic_parameter_type(key, *func_args)
    else:
        raise Exception(
            f"Unrecognized dynamic parameter function {func_name} for '"
            f"parameter {key}")


def parse_parameters_string(parameters_str):
    parameters_str = parameters_str[1:-1]
    parameter_strings = parameters_str.split(',')
    conditions = [parse_parameter_condition(
        pstr) for pstr in parameter_strings if '=' not in pstr]
    dynamic_parameters = [parse_dynamic_parameter(
        pstr) for pstr in parameter_strings if '=' in pstr]
    return conditions, dynamic_parameters


class FunctionSignatureArg:

    def __init__(self, key, variadic):
        self.key = key
        self.variadic = variadic


class FunctionSignature:

    def __init__(self, parameter_conditions, dynamic_parameters, args,
                 output_key):
        self.parameter_conditions: List[
            ConstrainedParameter] = parameter_conditions
        self.dynamic_parameters: List[DynamicParameter] = dynamic_parameters
        self.args: List[FunctionSignatureArg] = args
        self.output_key: str = output_key

    def matches_parameters(self, parameters_map: Dict[str, pa.DataType]):
        for parameter_condition in self.parameter_conditions:
            if parameter_condition.key not in parameters_map:
                return False
            actual_type = parameters_map[parameter_condition.key]
            if not parameter_condition.satisfied_with(actual_type,
                                                      parameters_map):
                return False
        return True


def parse_arg(arg_string):
    if arg_string.endswith('...'):
        return FunctionSignatureArg(arg_string[0:-3], True)
    else:
        return FunctionSignatureArg(arg_string, False)


def parse_signature(signature_str):
    arg_strings, _, output_key = signature_str.partition('=>')
    args = [parse_arg(arg_string)
            for arg_string in arg_strings[0:-1].split(',')]
    return args, output_key


def parse_function_signature(shortcut_string):
    shortcut_string = re.sub(r'\s+', '', shortcut_string)
    parameters_str, _, signature_str = shortcut_string.rpartition('(')
    parameter_conditions, dynamic_parameters = parse_parameters_string(
        parameters_str)
    args, output_key = parse_signature(signature_str)
    return FunctionSignature(parameter_conditions, dynamic_parameters, args,
                             output_key)


function_taxonomy_input = {
    'abs': ['<T:numeric>(T)=>T'],
    'abs_checked': ['<T:numeric>(T)=>T'],
    'acos': ['<T:numeric>(T)=>T'],
    'acos_checked': ['<T:numeric>(T)=>T'],
    'add': ['<T:simple_numeric>(T,T)=>T',
            '<T:decimal,V:decimal,O=DECIMAL_ADD(T|V)>(T,V)=>O'],
    'add_checked': ['<T:simple_numeric>(T,T)=>T',
                    '<T:decimal,V:decimal,O=DECIMAL_ADD(T|V)>(T,V)=>O'],
    'all': ['<T:logical>(T)=>T'],
    'and': ['<T:logical>(T,T)=>T'],
    'and_kleene': ['<T:logical>(T,T)=>T'],
    'and_not': ['<T:logical>(T,T)=>T'],
    'and_not_kleene': ['<T:logical>(T,T)=>T'],
    'any': ['<T:logical>(T)=>T'],
    'ascii_capitalize': ['<T:string>(T)=>T'],
    'ascii_center': ['<T:string>(T)=>T'],
    'ascii_is_alnum': ['<T:string>(T)=>T'],
    'ascii_is_alpha': ['<T:string>(T)=>T'],
    'ascii_is_decimal': ['<T:string>(T)=>T'],
    'ascii_is_lower': ['<T:string>(T)=>T'],
    'ascii_is_printable': ['<T:string>(T)=>T'],
    'ascii_is_space': ['<T:string>(T)=>T'],
    'ascii_is_title': ['<T:string>(T)=>T'],
    'ascii_is_upper': ['<T:string>(T)=>T'],
    'ascii_lower': ['<T:string>(T)=>T'],
    'ascii_lpad': ['<T:string>(T)=>T'],
    'ascii_ltrim': ['<T:string>(T)=>T'],
    'ascii_ltrim_whitespace': ['<T:string>(T)=>T'],
    'ascii_reverse': ['<T:string>(T)=>T'],
    'ascii_rpad': ['<T:string>(T)=>T'],
    'ascii_rtrim': ['<T:string>(T)=>T'],
    'ascii_rtrim_whitespace': ['<T:string>(T)=>T'],
    'ascii_split_whitespace': ['<T:string>(T)=>T'],
    'ascii_swapcase': ['<T:string>(T)=>T'],
    'ascii_title': ['<T:string>(T)=>T'],
    'ascii_trim': ['<T:string>(T)=>T'],
    'ascii_trim_whitespace': ['<T:string>(T)=>T'],
    'ascii_upper': ['<T:string>(T)=>T'],
    'asin': ['<T:numeric>(T)=>T'],
    'asin_checked': ['<T:numeric>(T)=>T'],
    'assume_timezone': ['<T:timestamp,O=WITH_TZ(T)>(T)=>O'],
    'atan': ['<T:numeric>(T)=>T'],
    'atan2': ['<T:floating>(T,T)=>T'],
    'binary_join': ['<T:string,L:~LIST(T)>(L,T)=>T'],
    'binary_join_element_wise': ['<T:string>(T)=>T'],
    'binary_length': ['<T:bytes>(T)=>T'],
    'binary_replace_slice': ['<T:bytes>(T)=>T'],
    'bit_wise_and': ['<T:integral>(T,T)=>T'],
    'bit_wise_not': ['<T:integral>(T)=>T'],
    'bit_wise_or': ['<T:integral>(T,T)=>T'],
    'bit_wise_xor': ['<T:integral>(T,T)=>T'],
    # Note, V technically needs to have X keys where X is the arity of T
    'case_when': ['<T,V:~CASE_WHEN(T)>(V,T...)=>T'],
    'cast': [],
    'ceil': ['<T:numeric>(T)=>T'],
    'choose': ['<T,I:integral>(I,T...)=>T'],
    'coalesce': ['<T>(T...)=>T'],
    'cos': ['<T:numeric>(T)=>T'],
    'cos_checked': ['<T:numeric>(T)=>T'],
    'count': ['<T>(T)=>T'],
    'count_substring': ['<T:bytes>(T)=>T'],
    'count_substring_regex': ['<T:bytes>(T)=>T'],
    'day': ['<T:datelike>(T)=>T'],
    'day_of_week': ['<T:datelike>(T)=>T'],
    'day_of_year': ['<T:datelike>(T)=>T'],
    'dictionary_encode': [],
    'divide': ['<T:simple_numeric>(T,T)=>T',
               '<T:decimal,V:decimal,O=DECIMAL_DIVIDE(T|V)>(T,V)=>O'],
    'divide_checked': ['<T:simple_numeric>(T,T)=>T',
                       '<T:decimal,V:decimal,O=DECIMAL_DIVIDE(T|V)>(T,V)=>O'],
    'drop_null': ['<T>(T)=>T'],
    'ends_with': ['<T:string>(T)=>T'],
    'equal': ['<T:equatable>(T,T)=>T'],
    'extract_regex': ['<T:bytes>(T)=>T'],
    'filter': ['<T, B:boolean>(T, B)=>T'],
    'find_substring': ['<T:string>(T)=>T'],
    'find_substring_regex': ['<T:string>(T)=>T'],
    'floor': ['<T:numeric>(T)=>T'],
    'greater': ['<T:sortable>(T,T)=>T'],
    'greater_equal': ['<T:sortable>(T,T)=>T'],
    'hour': ['<T:timelike>(T)=>T'],
    'if_else': ['<T,B:boolean>(B, T, T)=>T'],
    'index': ['<T:equatable>(T)=>T'],
    'index_in': ['<T:equatable>(T)=>T'],
    'invert': ['<T:logical>(T)=>T'],
    'is_finite': ['<T:floating>(T)=>T'],
    'is_in': ['<T:equatable>(T)=>T'],
    'is_inf': ['<T:floating>(T)=>T'],
    'is_nan': ['<T:floating>(T)=>T'],
    'is_null': ['<T,B:boolean>(T)=>B'],
    'is_valid': ['<T,B:boolean>(T)=>B'],
    'iso_calendar': ['<T:datelike>(T)=>T'],
    'iso_week': ['<T:datelike>(T)=>T'],
    'iso_year': ['<T:datelike>(T)=>T'],
    'less': ['<T:sortable>(T,T)=>T'],
    'less_equal': ['<T:sortable>(T,T)=>T'],
    'list_flatten': ['<T:list>(T)=>T'],
    'list_parent_indices': ['<T:list>(T)=>T'],
    'list_value_length': ['<T:list>(T)=>T'],
    'ln': ['<T:numeric>(T)=>T'],
    'ln_checked': ['<T:numeric>(T)=>T'],
    'log10': ['<T:numeric>(T)=>T'],
    'log10_checked': ['<T:numeric>(T)=>T'],
    'log1p': ['<T:numeric>(T)=>T'],
    'log1p_checked': ['<T:numeric>(T)=>T'],
    'log2': ['<T:numeric>(T)=>T'],
    'log2_checked': ['<T:numeric>(T)=>T'],
    'logb': ['<T:simple_numeric>(T,T)=>T'],
    'logb_checked': ['<T:simple_numeric>(T,T)=>T'],
    'make_struct': ['<Args...,O=STRUCTIFY(Args...)>(Args...)=>O'],
    'match_like': ['<T:string>(T)=>T'],
    'match_substring': ['<T:string>(T)=>T'],
    'match_substring_regex': ['<T:string>(T)=>T'],
    'max': ['<T:sortable>(T)=>T'],
    'max_element_wise': ['<T:sortable>(T...)=>T'],
    'mean': ['<T:numeric>(T)=>T'],
    'microsecond': ['<T:timelike>(T)=>T'],
    'millisecond': ['<T:timelike>(T)=>T'],
    'min': ['<T:sortable>(T)=>T'],
    'min_element_wise': ['<T:sortable>(T...)=>T'],
    'min_max': ['<T:sortable>(T)=>T'],
    'minute': ['<T:timelike>(T)=>T'],
    'mode': ['<T:numeric>(T)=>T'],
    'month': ['<T:datelike>(T)=>T'],
    'multiply': ['<T:simple_numeric>(T,T)=>T',
                 '<T:decimal,V:decimal,O=DECIMAL_MULTIPLY(T|V)>(T,V)=>O'],
    'multiply_checked': [
        '<T:simple_numeric>(T,T)=>T',
        '<T:decimal,V:decimal,O=DECIMAL_MULTIPLY(T|V)>(T,V)=>O'],
    'nanosecond': ['<T:timelike>(T)=>T'],
    'negate': ['<T:numeric>(T)=>T'],
    'negate_checked': ['<T:signed_numeric>(T)=>T'],
    'not_equal': ['<T:equatable>(T,T)=>T'],
    'or': ['<T:logical>(T,T)=>T'],
    'or_kleene': ['<T:logical>(T,T)=>T'],
    'partition_nth_indices': ['<T:sortable>(T)=>T'],
    'power': ['<T:simple_numeric>(T,T)=>T'],
    'power_checked': ['<T:simple_numeric>(T,T)=>T'],
    'product': ['<T:simple_numeric>(T)=>T'],
    'quantile': ['<T:numeric>(T)=>T'],
    'quarter': ['<T:datelike>(T)=>T'],
    'replace_substring': ['<T:string>(T)=>T'],
    'replace_substring_regex': ['<T:string>(T)=>T'],
    'replace_with_mask': ['<T,B:boolean>(T,B,T)=>T'],
    'round': ['<T:numeric>(T)=>T'],
    'round_to_multiple': ['<T:numeric>(T)=>T'],
    'second': ['<T:timelike>(T)=>T'],
    'select_k_unstable': ['<T:sortable>(T)=>T'],
    'shift_left': ['<T:integral>(T,T)=>T'],
    'shift_left_checked': ['<T:integral>(T,T)=>T'],
    'shift_right': ['<T:integral>(T,T)=>T'],
    'shift_right_checked': ['<T:integral>(T,T)=>T'],
    'sign': ['<T:numeric>(T)=>T'],
    'sin': ['<T:numeric>(T)=>T'],
    'sin_checked': ['<T:numeric>(T)=>T'],
    'sort_indices': ['<T:sortable>(T)=>T'],
    'split_pattern': ['<T:bytes>(T)=>T'],
    'split_pattern_regex': ['<T:bytes>(T)=>T'],
    'starts_with': ['<T:bytes>(T)=>T'],
    'stddev': ['<T:numeric>(T)=>T'],
    'strftime': ['<T:temporal>(T)=>T'],
    'string_is_ascii': ['<T:string>(T)=>T'],
    'strptime': ['<T:string>(T)=>T'],
    'subsecond': ['<T:timelike>(T)=>T'],
    'subtract': ['<T:simple_numeric>(T,T)=>T',
                 '<T:decimal,V:decimal,O=DECIMAL_ADD(T|V)>(T,V)=>O'],
    'subtract_checked': ['<T:simple_numeric>(T,T)=>T',
                         '<T:decimal,V:decimal,O=DECIMAL_ADD(T|V)>(T,V)=>O'],
    'sum': ['<T:numeric>(T)=>T'],
    'take': ['<T,I:integral>(T,I)=>T'],
    'tan': ['<T:numeric>(T)=>T'],
    'tan_checked': ['<T:numeric>(T)=>T'],
    'tdigest': ['<T:numeric>(T)=>T'],
    'trunc': ['<T:numeric>(T)=>T'],
    'unique': ['<T:equatable>(T)=>T'],
    'utf8_capitalize': ['<T:string>(T)=>T'],
    'utf8_center': ['<T:string>(T)=>T'],
    'utf8_is_alnum': ['<T:string>(T)=>T'],
    'utf8_is_alpha': ['<T:string>(T)=>T'],
    'utf8_is_decimal': ['<T:string>(T)=>T'],
    'utf8_is_digit': ['<T:string>(T)=>T'],
    'utf8_is_lower': ['<T:string>(T)=>T'],
    'utf8_is_numeric': ['<T:string>(T)=>T'],
    'utf8_is_types_numeric': ['<T:string>(T)=>T'],
    'utf8_is_printable': ['<T:string>(T)=>T'],
    'utf8_is_space': ['<T:string>(T)=>T'],
    'utf8_is_title': ['<T:string>(T)=>T'],
    'utf8_is_upper': ['<T:string>(T)=>T'],
    'utf8_length': ['<T:string>(T)=>T'],
    'utf8_lower': ['<T:string>(T)=>T'],
    'utf8_lpad': ['<T:string>(T)=>T'],
    'utf8_ltrim': ['<T:string>(T)=>T'],
    'utf8_ltrim_whitespace': ['<T:string>(T)=>T'],
    'utf8_replace_slice': ['<T:string>(T)=>T'],
    'utf8_reverse': ['<T:string>(T)=>T'],
    'utf8_rpad': ['<T:string>(T)=>T'],
    'utf8_rtrim': ['<T:string>(T)=>T'],
    'utf8_rtrim_whitespace': ['<T:string>(T)=>T'],
    'utf8_slice_codeunits': ['<T:string>(T)=>T'],
    'utf8_split_whitespace': ['<T:string>(T)=>T'],
    'utf8_swapcase': ['<T:string>(T)=>T'],
    'utf8_title': ['<T:string>(T)=>T'],
    'utf8_trim': ['<T:string>(T)=>T'],
    'utf8_trim_whitespace': ['<T:string>(T)=>T'],
    'utf8_upper': ['<T:string>(T)=>T'],
    'value_counts': ['<T:equatable>(T)=>T'],
    'variance': ['<T:numeric>(T)=>T'],
    'xor': ['<T:logical>(T,T)=>T'],
    'year': ['<T:datelike>(T)=>T']
}


def _create_function_taxonomy():
    taxonomy = {}
    for key, value in function_taxonomy_input.items():
        taxonomy[key] = [parse_function_signature(
            signature) for signature in value]
    return taxonomy


function_taxonomy = _create_function_taxonomy()


def sample_combinations_of_types(
        constrained_parameters: List[ConstrainedParameter]):
    iterators = []
    parameters_map = {}
    for constrained_parameter in constrained_parameters:
        it = iter(constrained_parameter.sample(parameters_map))
        iterators.append(it)
        initial_value = next(it)
        parameters_map[constrained_parameter.key] = initial_value
    while True:
        yield parameters_map.copy()
        for idx in reversed(range(len(iterators))):
            constrained_parameter = constrained_parameters[idx]
            try:
                next_value = next(iterators[idx])
                parameters_map[constrained_parameter.key] = next_value
                break
            except StopIteration:
                if idx == 0:
                    return
                del parameters_map[constrained_parameter.key]
                del iterators[idx]
        for reset_idx in range(idx + 1, len(constrained_parameters)):
            constrained_parameter = constrained_parameters[reset_idx]
            it = iter(constrained_parameter.sample(parameters_map))
            iterators.append(it)
            initial_value = next(it)
            parameters_map[constrained_parameter.key] = initial_value


def get_sample_calls(signature: FunctionSignature):
    constrained_parameters = signature.parameter_conditions
    for param_map in sample_combinations_of_types(constrained_parameters):
        for dynamic_parameter in signature.dynamic_parameters:
            param_map[dynamic_parameter.key] = dynamic_parameter.compute_type(
                param_map)
        args = []
        has_variadic = False
        for arg in signature.args:
            if arg.key == 'Args':
                # TODO
                continue
            data_type = param_map[arg.key]
            args.append(data_type)
            has_variadic |= arg.variadic
        return_type = param_map[signature.output_key]
        yield args, return_type, param_map.copy()
        # if has_variadic:
        #     args.append(args[-1])
        #     yield args, return_type, param_map.copy()


if __name__ == '__main__':
    for args, return_type in get_sample_calls(function_taxonomy['add'][1]):
        print(f'{args} -> {return_type}')


def get_sample_opts():
    def assume_timezone_opts(_): return pc.AssumeTimezoneOptions('UTC')

    def extract_regex_opts(_): return pc.ExtractRegexOptions(
        '(?P<letter>[ab])(?P<digit>\\d)')

    def idx_opts(types): return pc.IndexOptions(pa.scalar(None, type=types[0]))

    def idx_in_opts(types): return pc.SetLookupOptions(
        value_set=pa.array([None], type=types[0]))

    def match_substr_opts(_): return pc.MatchSubstringOptions('.*')

    def pad_opts(_): return pc.PadOptions(10)

    def partition_opts(_): return pc.PartitionNthOptions(10)

    def replace_slice_opts(_): return pc.ReplaceSliceOptions(0, 2, '  ')

    def replace_substr_opts(_): return pc.ReplaceSubstringOptions('.*', 'foo')

    def select_k_unstable_opts(_): return pc.SelectKOptions(
        k=3, sort_keys=[("", "ascending")])

    def slice_opts(_): return pc.SliceOptions(0, 10)

    def split_pattern_opts(_): return pc.SplitPatternOptions(pattern='=')

    def strptime_opts(_): return pc.StrptimeOptions(
        format='%Y-%m-%dT%H:%M:%SZ', unit='ns')

    def trim_opts(_): return pc.TrimOptions(' ')

    return {
        'ascii_center': pad_opts,
        'ascii_lpad': pad_opts,
        'ascii_ltrim': trim_opts,
        'ascii_rpad': pad_opts,
        'ascii_rtrim': trim_opts,
        'ascii_trim': trim_opts,
        'assume_timezone': assume_timezone_opts,
        'binary_replace_slice': replace_slice_opts,
        'count_substring': match_substr_opts,
        'count_substring_regex': match_substr_opts,
        'ends_with': match_substr_opts,
        'extract_regex': extract_regex_opts,
        'find_substring': match_substr_opts,
        'find_substring_regex': match_substr_opts,
        'index': idx_opts,
        'index_in': idx_in_opts,
        'is_in': idx_in_opts,
        'match_like': match_substr_opts,
        'match_substring': match_substr_opts,
        'match_substring_regex': match_substr_opts,
        'partition_nth_indices': partition_opts,
        'replace_substring': replace_substr_opts,
        'replace_substring_regex': replace_substr_opts,
        'select_k_unstable': select_k_unstable_opts,
        'split_pattern': split_pattern_opts,
        'split_pattern_regex': split_pattern_opts,
        'strptime': strptime_opts,
        'utf8_center': pad_opts,
        'utf8_lpad': pad_opts,
        'utf8_ltrim': trim_opts,
        'utf8_replace_slice': replace_slice_opts,
        'utf8_rpad': pad_opts,
        'utf8_rtrim': trim_opts,
        'utf8_trim': trim_opts,
        'utf8_slice_codeunits': slice_opts
    }


samples_opts = get_sample_opts()


class SampleCall:

    def __init__(self, function_name: str, args: List[pa.Array], options,
                 parameters_map):
        self.function_name = function_name
        self.args = args
        self.options = options
        self.parameters_map = parameters_map

    def __repr__(self):
        arg_str = ', '.join([str(arr.type) for arr in self.args])
        return f'{self.function_name}({arg_str})'


def get_sample_empty_calls():
    for function_name in pc.function_registry().list_functions():
        if function_name not in function_taxonomy:
            continue
        for signature in function_taxonomy[function_name]:
            for arg_types, _return_type, parameters_map in get_sample_calls(
                    signature):
                args = [pa.array([], type=arg_type) for arg_type in arg_types]
                options_fn = samples_opts.get(function_name, None)
                options = None
                if options_fn is not None:
                    options = options_fn(arg_types)
                yield SampleCall(function_name, args, options, parameters_map)


@pytest.mark.parametrize('function_name',
                         pc.function_registry().list_functions(), ids=str)
def test_all_functions_in_taxonomy(function_name):
    if function_name.startswith('array_'):
        pytest.xfail(
            'ARROW-13873: array_* functions should be hidden from python')
    if function_name.endswith('_meta_binary'):
        pytest.xfail(
            'ARROW-13949: *_meta_binary functions should be '
            'hidden from python')
    if function_name.startswith('hash_'):
        pytest.xfail(
            'ARROW-13943: hash_* functions should be hidden from python')
    if function_name == 'list_element':
        pytest.xfail('ARROW-13594: list_element requires a scalar input')
    assert function_name in function_taxonomy


def _check_expect_fail(sample_call: SampleCall, message: str,
                       signatures_map: List[Tuple[Set[str], List[str]]]):
    for possible_sig in signatures_map:
        if sample_call.function_name in possible_sig[0]:
            signatures = possible_sig[1]
            for signature in signatures:
                parsed_signature = parse_function_signature(signature)
                if parsed_signature.matches_parameters(
                        sample_call.parameters_map):
                    pytest.xfail(message)


def safe_str(o):
    raw_str = str(o)
    return re.sub(r'\s', '', raw_str).replace('(', '-').replace(')', '-')


@pytest.mark.parametrize('sample_call', get_sample_empty_calls(), ids=safe_str)
def test_supports_empty_arrays(sample_call):
    _check_expect_fail(sample_call,
                       'ARROW-13390: Improve type support for coalesce kernel',
                       [
                           ({'choose'}, ['<T:list,I:integral>(I,T...)=>T',
                                         '<T:struct,I:integral>(I,T...)=>T']),
                           ({'coalesce'},
                            ['<T:list>(T)=>T', '<T:struct>(T)=>T']),
                           ({'replace_with_mask'}, [
                               '<T:list,B:boolean>(T,B,T)=>T',
                               '<T:struct,B:boolean>(T,B,T)=>T'])
                       ])
    _check_expect_fail(sample_call,
                       'ARROW-13130: Add decimal support for arithmetic '
                       'compute functions',
                       [
                           ({
                               'abs',
                               'abs_checked',
                               'acos',
                               'acos_checked',
                               'any',
                               'asin',
                               'asin_checked',
                               'atan',
                               'atan2',
                               'ceil',
                               'cos',
                               'cos_checked',
                               'floor',
                               'index',
                               'is_finite',
                               'is_inf',
                               'is_nan',
                               'ln',
                               'ln_checked',
                               'log10',
                               'log10_checked',
                               'log1p',
                               'log1p_checked',
                               'log2',
                               'log2_checked',
                               'max_element_wise',
                               'min_element_wise',
                               'mode',
                               'negate',
                               'negate_checked',
                               'quantile',
                               'round',
                               'round_to_multiple',
                               'sign',
                               'sin',
                               'sin_checked',
                               'stddev',
                               'tan',
                               'tan_checked',
                               'tdigest',
                               'trunc',
                               'variance',
                           }, ['<T:decimal>(T)=>T'])
                       ])
    _check_expect_fail(sample_call,
                       'ARROW-13876: Uniform null handling in compute '
                       'functions',
                       [
                           ({
                               'add',
                               'add_checked',
                               'all',
                               'and',
                               'and_kleene',
                               'and_not',
                               'and_not_kleene',
                               'ascii_capitalize',
                               'ascii_center',
                               'ascii_is_alnum',
                               'ascii_is_alpha',
                               'ascii_is_decimal',
                               'ascii_is_lower',
                               'ascii_is_printable',
                               'ascii_is_space',
                               'ascii_is_title',
                               'ascii_is_upper',
                               'ascii_lower',
                               'ascii_lpad',
                               'ascii_ltrim',
                               'ascii_ltrim_whitespace',
                               'ascii_reverse',
                               'ascii_rpad',
                               'ascii_rtrim',
                               'ascii_rtrim_whitespace',
                               'ascii_split_whitespace',
                               'ascii_swapcase',
                               'ascii_title',
                               'ascii_trim',
                               'ascii_trim_whitespace',
                               'ascii_upper',
                               'assume_timezone',
                               'binary_join',
                               'binary_join_element_wise',
                               'bit_wise_and',
                               'bit_wise_not',
                               'bit_wise_or',
                               'bit_wise_xor',
                               'day',
                               'day_of_week',
                               'day_of_year',
                               'divide',
                               'divide_checked',
                               'ends_with',
                               'find_substring',
                               'find_substring_regex',
                               'invert',
                               'iso_calendar',
                               'iso_week',
                               'iso_year',
                               'list_flatten',
                               'list_parent_indices',
                               'list_value_length',
                               'logb',
                               'logb_checked',
                               'match_like',
                               'match_substring',
                               'match_substring_regex',
                               'mean',
                               'multiply',
                               'multiply_checked',
                               'or',
                               'or_kleene',
                               'partition_nth_indices',
                               'power',
                               'power_checked',
                               'product',
                               'quarter',
                               'replace_substring',
                               'replace_substring_regex',
                               'select_k_unstable',
                               'shift_left',
                               'shift_left_checked',
                               'shift_right',
                               'shift_right_checked',
                               'sort_indices',
                               'string_is_ascii',
                               'strptime',
                               'subtract',
                               'subtract_checked',
                               'sum',
                               'take',
                               'utf8_capitalize',
                               'utf8_center',
                               'utf8_is_alnum',
                               'utf8_is_alpha',
                               'utf8_is_decimal',
                               'utf8_is_digit',
                               'utf8_is_lower',
                               'utf8_is_numeric',
                               'utf8_is_types_numeric',
                               'utf8_is_printable',
                               'utf8_is_space',
                               'utf8_is_title',
                               'utf8_is_upper',
                               'utf8_length',
                               'utf8_lower',
                               'utf8_lpad',
                               'utf8_ltrim',
                               'utf8_ltrim_whitespace',
                               'utf8_replace_slice',
                               'utf8_reverse',
                               'utf8_rpad',
                               'utf8_rtrim',
                               'utf8_rtrim_whitespace',
                               'utf8_slice_codeunits',
                               'utf8_split_whitespace',
                               'utf8_swapcase',
                               'utf8_title',
                               'utf8_trim',
                               'utf8_trim_whitespace',
                               'utf8_upper',
                               'xor',
                               'year'
                           }, ['<T:null>(T)=>T', '<T:null>(T,T)=>T',
                               '<T:decimal,V:null>(T,V)=>T',
                               '<T:null,V:decimal>(T,V)=>T']),
                           ({'filter'}, ['<T,B:null>(T,B)=>T']),
                           ({'take'}, ['<T,I:null>(T,I)=>T']),
                           ({'replace_with_mask'}, ['<T,B:null>(T,B,T)=>T'])
                       ])
    _check_expect_fail(sample_call,
                       'ARROW-13945: fixed list support missing for '
                       'binary_join',
                       [
                           ({'binary_join'},
                            ['<T:string,L:~FIXED_SIZE_LIST(T)>(L,T)=>T'])
                       ])
    _check_expect_fail(sample_call,
                       'ARROW-13878: Add fixed_size_binary support to compute '
                       'functions',
                       [
                           ({
                               'binary_length',
                               'binary_replace_slice',
                               'count_substring',
                               'count_substring_regex',
                               'equal',
                               'greater',
                               'greater_equal',
                               'index',
                               'less',
                               'less_equal',
                               'month',
                               'multiply',
                               'multiply_checked',
                               'not_equal'
                           },
                               ['<T:fixed_bytes>(T)=>T'])
                       ])
    _check_expect_fail(sample_call,
                       'ARROW-13879: Mixed support for binary types in regex '
                       'functions',
                       [
                           ({'extract_regex', 'split_pattern',
                             'split_pattern_regex', 'starts_with'},
                            ['<T:bytes>(T)=>T'])
                       ])
    _check_expect_fail(sample_call,
                       'ARROW-14111: Add extraction function support for '
                       'time32/time64',
                       [
                           ({'hour', 'microsecond', 'millisecond', 'minute',
                             'nanosecond', 'second', 'subsecond'},
                            ['<T:timelike>(T)=>T'])
                       ])
    _check_expect_fail(sample_call,
                       'ARROW-13358: Extend type support for if_else kernel', [
                           ({'if_else'},
                            ['<T:timestamp_all,B:boolean>(B, T, T)=>T',
                             '<T:decimal,B:boolean>(B, T, T)=>T',
                             '<T:list,B:boolean>(B, T, T)=>T',
                             '<T:struct,B:boolean>(B, T, T)=>T'])
                       ])
    _check_expect_fail(sample_call,
                       'ARROW-14112: index_in/is_in does not support '
                       'timestamptz',
                       [
                           ({'index_in'}, ['<T:timestamptz>(T)=>T']),
                           ({'is_in'}, ['<T:timestamptz>(T)=>T'])
                       ])
    _check_expect_fail(sample_call,
                       'ARROW-14113: max_element_wise does not support binary',
                       [
                           ({'max_element_wise', 'min_element_wise'},
                            ['<T:bytes>(T)=>T'])
                       ])
    _check_expect_fail(sample_call,
                       'ARROW-13916: Implement strftime on date32/64 types', [
                           ({'strftime'}, ['<T:time>(T)=>T', '<T:date>(T)=>T'])
                       ])
    pc.call_function(sample_call.function_name,
                     sample_call.args, sample_call.options)
