// Generated by using Rcpp::compileAttributes() -> do not edit by hand
// Generator token: 10BE3573-1514-4C36-9D1C-5A225CD40393

#include "arrow_types.h"
#include <Rcpp.h>

using namespace Rcpp;

// ArrayBuilder
xptr_ArrayBuilder ArrayBuilder(xptr_DataType xptr_type);
RcppExport SEXP _arrow_ArrayBuilder(SEXP xptr_typeSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< xptr_DataType >::type xptr_type(xptr_typeSEXP);
    rcpp_result_gen = Rcpp::wrap(ArrayBuilder(xptr_type));
    return rcpp_result_gen;
END_RCPP
}
// ArrayBuilder__num_children
int ArrayBuilder__num_children(xptr_ArrayBuilder xptr_type);
RcppExport SEXP _arrow_ArrayBuilder__num_children(SEXP xptr_typeSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< xptr_ArrayBuilder >::type xptr_type(xptr_typeSEXP);
    rcpp_result_gen = Rcpp::wrap(ArrayBuilder__num_children(xptr_type));
    return rcpp_result_gen;
END_RCPP
}
// field_pointer
xptr_Field field_pointer(const std::string& name, xptr_DataType type, bool nullable);
RcppExport SEXP _arrow_field_pointer(SEXP nameSEXP, SEXP typeSEXP, SEXP nullableSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< const std::string& >::type name(nameSEXP);
    Rcpp::traits::input_parameter< xptr_DataType >::type type(typeSEXP);
    Rcpp::traits::input_parameter< bool >::type nullable(nullableSEXP);
    rcpp_result_gen = Rcpp::wrap(field_pointer(name, type, nullable));
    return rcpp_result_gen;
END_RCPP
}
// Field_ToString
std::string Field_ToString(xptr_Field type);
RcppExport SEXP _arrow_Field_ToString(SEXP typeSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< xptr_Field >::type type(typeSEXP);
    rcpp_result_gen = Rcpp::wrap(Field_ToString(type));
    return rcpp_result_gen;
END_RCPP
}
// Field_name
std::string Field_name(xptr_Field type);
RcppExport SEXP _arrow_Field_name(SEXP typeSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< xptr_Field >::type type(typeSEXP);
    rcpp_result_gen = Rcpp::wrap(Field_name(type));
    return rcpp_result_gen;
END_RCPP
}
// Field_nullable
bool Field_nullable(xptr_Field type);
RcppExport SEXP _arrow_Field_nullable(SEXP typeSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< xptr_Field >::type type(typeSEXP);
    rcpp_result_gen = Rcpp::wrap(Field_nullable(type));
    return rcpp_result_gen;
END_RCPP
}
// Int8_initialize
xptr_DataType Int8_initialize();
RcppExport SEXP _arrow_Int8_initialize() {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    rcpp_result_gen = Rcpp::wrap(Int8_initialize());
    return rcpp_result_gen;
END_RCPP
}
// Int16_initialize
xptr_DataType Int16_initialize();
RcppExport SEXP _arrow_Int16_initialize() {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    rcpp_result_gen = Rcpp::wrap(Int16_initialize());
    return rcpp_result_gen;
END_RCPP
}
// Int32_initialize
xptr_DataType Int32_initialize();
RcppExport SEXP _arrow_Int32_initialize() {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    rcpp_result_gen = Rcpp::wrap(Int32_initialize());
    return rcpp_result_gen;
END_RCPP
}
// Int64_initialize
xptr_DataType Int64_initialize();
RcppExport SEXP _arrow_Int64_initialize() {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    rcpp_result_gen = Rcpp::wrap(Int64_initialize());
    return rcpp_result_gen;
END_RCPP
}
// UInt8_initialize
xptr_DataType UInt8_initialize();
RcppExport SEXP _arrow_UInt8_initialize() {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    rcpp_result_gen = Rcpp::wrap(UInt8_initialize());
    return rcpp_result_gen;
END_RCPP
}
// UInt16_initialize
xptr_DataType UInt16_initialize();
RcppExport SEXP _arrow_UInt16_initialize() {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    rcpp_result_gen = Rcpp::wrap(UInt16_initialize());
    return rcpp_result_gen;
END_RCPP
}
// UInt32_initialize
xptr_DataType UInt32_initialize();
RcppExport SEXP _arrow_UInt32_initialize() {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    rcpp_result_gen = Rcpp::wrap(UInt32_initialize());
    return rcpp_result_gen;
END_RCPP
}
// UInt64_initialize
xptr_DataType UInt64_initialize();
RcppExport SEXP _arrow_UInt64_initialize() {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    rcpp_result_gen = Rcpp::wrap(UInt64_initialize());
    return rcpp_result_gen;
END_RCPP
}
// Float16_initialize
xptr_DataType Float16_initialize();
RcppExport SEXP _arrow_Float16_initialize() {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    rcpp_result_gen = Rcpp::wrap(Float16_initialize());
    return rcpp_result_gen;
END_RCPP
}
// Float32_initialize
xptr_DataType Float32_initialize();
RcppExport SEXP _arrow_Float32_initialize() {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    rcpp_result_gen = Rcpp::wrap(Float32_initialize());
    return rcpp_result_gen;
END_RCPP
}
// Float64_initialize
xptr_DataType Float64_initialize();
RcppExport SEXP _arrow_Float64_initialize() {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    rcpp_result_gen = Rcpp::wrap(Float64_initialize());
    return rcpp_result_gen;
END_RCPP
}
// Boolean_initialize
xptr_DataType Boolean_initialize();
RcppExport SEXP _arrow_Boolean_initialize() {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    rcpp_result_gen = Rcpp::wrap(Boolean_initialize());
    return rcpp_result_gen;
END_RCPP
}
// Utf8_initialize
xptr_DataType Utf8_initialize();
RcppExport SEXP _arrow_Utf8_initialize() {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    rcpp_result_gen = Rcpp::wrap(Utf8_initialize());
    return rcpp_result_gen;
END_RCPP
}
// Date32_initialize
xptr_DataType Date32_initialize();
RcppExport SEXP _arrow_Date32_initialize() {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    rcpp_result_gen = Rcpp::wrap(Date32_initialize());
    return rcpp_result_gen;
END_RCPP
}
// Date64_initialize
xptr_DataType Date64_initialize();
RcppExport SEXP _arrow_Date64_initialize() {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    rcpp_result_gen = Rcpp::wrap(Date64_initialize());
    return rcpp_result_gen;
END_RCPP
}
// Null_initialize
xptr_DataType Null_initialize();
RcppExport SEXP _arrow_Null_initialize() {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    rcpp_result_gen = Rcpp::wrap(Null_initialize());
    return rcpp_result_gen;
END_RCPP
}
// Decimal128Type_initialize
xptr_DataType Decimal128Type_initialize(int32_t precision, int32_t scale);
RcppExport SEXP _arrow_Decimal128Type_initialize(SEXP precisionSEXP, SEXP scaleSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< int32_t >::type precision(precisionSEXP);
    Rcpp::traits::input_parameter< int32_t >::type scale(scaleSEXP);
    rcpp_result_gen = Rcpp::wrap(Decimal128Type_initialize(precision, scale));
    return rcpp_result_gen;
END_RCPP
}
// FixedSizeBinary_initialize
xptr_DataType FixedSizeBinary_initialize(int32_t byte_width);
RcppExport SEXP _arrow_FixedSizeBinary_initialize(SEXP byte_widthSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< int32_t >::type byte_width(byte_widthSEXP);
    rcpp_result_gen = Rcpp::wrap(FixedSizeBinary_initialize(byte_width));
    return rcpp_result_gen;
END_RCPP
}
// Timestamp_initialize1
xptr_DataType Timestamp_initialize1(arrow::TimeUnit::type unit);
RcppExport SEXP _arrow_Timestamp_initialize1(SEXP unitSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< arrow::TimeUnit::type >::type unit(unitSEXP);
    rcpp_result_gen = Rcpp::wrap(Timestamp_initialize1(unit));
    return rcpp_result_gen;
END_RCPP
}
// Timestamp_initialize2
xptr_DataType Timestamp_initialize2(arrow::TimeUnit::type unit, const std::string& timezone);
RcppExport SEXP _arrow_Timestamp_initialize2(SEXP unitSEXP, SEXP timezoneSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< arrow::TimeUnit::type >::type unit(unitSEXP);
    Rcpp::traits::input_parameter< const std::string& >::type timezone(timezoneSEXP);
    rcpp_result_gen = Rcpp::wrap(Timestamp_initialize2(unit, timezone));
    return rcpp_result_gen;
END_RCPP
}
// Time32_initialize
xptr_DataType Time32_initialize(arrow::TimeUnit::type unit);
RcppExport SEXP _arrow_Time32_initialize(SEXP unitSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< arrow::TimeUnit::type >::type unit(unitSEXP);
    rcpp_result_gen = Rcpp::wrap(Time32_initialize(unit));
    return rcpp_result_gen;
END_RCPP
}
// Time64_initialize
xptr_DataType Time64_initialize(arrow::TimeUnit::type unit);
RcppExport SEXP _arrow_Time64_initialize(SEXP unitSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< arrow::TimeUnit::type >::type unit(unitSEXP);
    rcpp_result_gen = Rcpp::wrap(Time64_initialize(unit));
    return rcpp_result_gen;
END_RCPP
}
// list__
SEXP list__(SEXP x);
RcppExport SEXP _arrow_list__(SEXP xSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< SEXP >::type x(xSEXP);
    rcpp_result_gen = Rcpp::wrap(list__(x));
    return rcpp_result_gen;
END_RCPP
}
// struct_
xptr_DataType struct_(ListOf<xptr_Field> fields);
RcppExport SEXP _arrow_struct_(SEXP fieldsSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< ListOf<xptr_Field> >::type fields(fieldsSEXP);
    rcpp_result_gen = Rcpp::wrap(struct_(fields));
    return rcpp_result_gen;
END_RCPP
}
// DataType_ToString
std::string DataType_ToString(xptr_DataType type);
RcppExport SEXP _arrow_DataType_ToString(SEXP typeSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< xptr_DataType >::type type(typeSEXP);
    rcpp_result_gen = Rcpp::wrap(DataType_ToString(type));
    return rcpp_result_gen;
END_RCPP
}
// DataType_name
std::string DataType_name(xptr_DataType type);
RcppExport SEXP _arrow_DataType_name(SEXP typeSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< xptr_DataType >::type type(typeSEXP);
    rcpp_result_gen = Rcpp::wrap(DataType_name(type));
    return rcpp_result_gen;
END_RCPP
}
// DataType_Equals
bool DataType_Equals(xptr_DataType lhs, xptr_DataType rhs);
RcppExport SEXP _arrow_DataType_Equals(SEXP lhsSEXP, SEXP rhsSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< xptr_DataType >::type lhs(lhsSEXP);
    Rcpp::traits::input_parameter< xptr_DataType >::type rhs(rhsSEXP);
    rcpp_result_gen = Rcpp::wrap(DataType_Equals(lhs, rhs));
    return rcpp_result_gen;
END_RCPP
}
// DataType_num_children
int DataType_num_children(xptr_DataType type);
RcppExport SEXP _arrow_DataType_num_children(SEXP typeSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< xptr_DataType >::type type(typeSEXP);
    rcpp_result_gen = Rcpp::wrap(DataType_num_children(type));
    return rcpp_result_gen;
END_RCPP
}
// DataType_children_pointer
List DataType_children_pointer(xptr_DataType type);
RcppExport SEXP _arrow_DataType_children_pointer(SEXP typeSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< xptr_DataType >::type type(typeSEXP);
    rcpp_result_gen = Rcpp::wrap(DataType_children_pointer(type));
    return rcpp_result_gen;
END_RCPP
}
// schema_
xptr_Schema schema_(ListOf<xptr_Field> fields);
RcppExport SEXP _arrow_schema_(SEXP fieldsSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< ListOf<xptr_Field> >::type fields(fieldsSEXP);
    rcpp_result_gen = Rcpp::wrap(schema_(fields));
    return rcpp_result_gen;
END_RCPP
}
// Schema_ToString
std::string Schema_ToString(xptr_Schema type);
RcppExport SEXP _arrow_Schema_ToString(SEXP typeSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< xptr_Schema >::type type(typeSEXP);
    rcpp_result_gen = Rcpp::wrap(Schema_ToString(type));
    return rcpp_result_gen;
END_RCPP
}
// ListType_ToString
std::string ListType_ToString(xptr_ListType type);
RcppExport SEXP _arrow_ListType_ToString(SEXP typeSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< xptr_ListType >::type type(typeSEXP);
    rcpp_result_gen = Rcpp::wrap(ListType_ToString(type));
    return rcpp_result_gen;
END_RCPP
}
// FixedWidthType_bit_width
int FixedWidthType_bit_width(xptr_FixedWidthType type);
RcppExport SEXP _arrow_FixedWidthType_bit_width(SEXP typeSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< xptr_FixedWidthType >::type type(typeSEXP);
    rcpp_result_gen = Rcpp::wrap(FixedWidthType_bit_width(type));
    return rcpp_result_gen;
END_RCPP
}
// DateType_unit
arrow::DateUnit DateType_unit(xptr_DateType type);
RcppExport SEXP _arrow_DateType_unit(SEXP typeSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< xptr_DateType >::type type(typeSEXP);
    rcpp_result_gen = Rcpp::wrap(DateType_unit(type));
    return rcpp_result_gen;
END_RCPP
}
// DecimalType_precision
int32_t DecimalType_precision(xptr_DecimalType type);
RcppExport SEXP _arrow_DecimalType_precision(SEXP typeSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< xptr_DecimalType >::type type(typeSEXP);
    rcpp_result_gen = Rcpp::wrap(DecimalType_precision(type));
    return rcpp_result_gen;
END_RCPP
}
// DecimalType_scale
int32_t DecimalType_scale(xptr_DecimalType type);
RcppExport SEXP _arrow_DecimalType_scale(SEXP typeSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< xptr_DecimalType >::type type(typeSEXP);
    rcpp_result_gen = Rcpp::wrap(DecimalType_scale(type));
    return rcpp_result_gen;
END_RCPP
}

static const R_CallMethodDef CallEntries[] = {
    {"_arrow_ArrayBuilder", (DL_FUNC) &_arrow_ArrayBuilder, 1},
    {"_arrow_ArrayBuilder__num_children", (DL_FUNC) &_arrow_ArrayBuilder__num_children, 1},
    {"_arrow_field_pointer", (DL_FUNC) &_arrow_field_pointer, 3},
    {"_arrow_Field_ToString", (DL_FUNC) &_arrow_Field_ToString, 1},
    {"_arrow_Field_name", (DL_FUNC) &_arrow_Field_name, 1},
    {"_arrow_Field_nullable", (DL_FUNC) &_arrow_Field_nullable, 1},
    {"_arrow_Int8_initialize", (DL_FUNC) &_arrow_Int8_initialize, 0},
    {"_arrow_Int16_initialize", (DL_FUNC) &_arrow_Int16_initialize, 0},
    {"_arrow_Int32_initialize", (DL_FUNC) &_arrow_Int32_initialize, 0},
    {"_arrow_Int64_initialize", (DL_FUNC) &_arrow_Int64_initialize, 0},
    {"_arrow_UInt8_initialize", (DL_FUNC) &_arrow_UInt8_initialize, 0},
    {"_arrow_UInt16_initialize", (DL_FUNC) &_arrow_UInt16_initialize, 0},
    {"_arrow_UInt32_initialize", (DL_FUNC) &_arrow_UInt32_initialize, 0},
    {"_arrow_UInt64_initialize", (DL_FUNC) &_arrow_UInt64_initialize, 0},
    {"_arrow_Float16_initialize", (DL_FUNC) &_arrow_Float16_initialize, 0},
    {"_arrow_Float32_initialize", (DL_FUNC) &_arrow_Float32_initialize, 0},
    {"_arrow_Float64_initialize", (DL_FUNC) &_arrow_Float64_initialize, 0},
    {"_arrow_Boolean_initialize", (DL_FUNC) &_arrow_Boolean_initialize, 0},
    {"_arrow_Utf8_initialize", (DL_FUNC) &_arrow_Utf8_initialize, 0},
    {"_arrow_Date32_initialize", (DL_FUNC) &_arrow_Date32_initialize, 0},
    {"_arrow_Date64_initialize", (DL_FUNC) &_arrow_Date64_initialize, 0},
    {"_arrow_Null_initialize", (DL_FUNC) &_arrow_Null_initialize, 0},
    {"_arrow_Decimal128Type_initialize", (DL_FUNC) &_arrow_Decimal128Type_initialize, 2},
    {"_arrow_FixedSizeBinary_initialize", (DL_FUNC) &_arrow_FixedSizeBinary_initialize, 1},
    {"_arrow_Timestamp_initialize1", (DL_FUNC) &_arrow_Timestamp_initialize1, 1},
    {"_arrow_Timestamp_initialize2", (DL_FUNC) &_arrow_Timestamp_initialize2, 2},
    {"_arrow_Time32_initialize", (DL_FUNC) &_arrow_Time32_initialize, 1},
    {"_arrow_Time64_initialize", (DL_FUNC) &_arrow_Time64_initialize, 1},
    {"_arrow_list__", (DL_FUNC) &_arrow_list__, 1},
    {"_arrow_struct_", (DL_FUNC) &_arrow_struct_, 1},
    {"_arrow_DataType_ToString", (DL_FUNC) &_arrow_DataType_ToString, 1},
    {"_arrow_DataType_name", (DL_FUNC) &_arrow_DataType_name, 1},
    {"_arrow_DataType_Equals", (DL_FUNC) &_arrow_DataType_Equals, 2},
    {"_arrow_DataType_num_children", (DL_FUNC) &_arrow_DataType_num_children, 1},
    {"_arrow_DataType_children_pointer", (DL_FUNC) &_arrow_DataType_children_pointer, 1},
    {"_arrow_schema_", (DL_FUNC) &_arrow_schema_, 1},
    {"_arrow_Schema_ToString", (DL_FUNC) &_arrow_Schema_ToString, 1},
    {"_arrow_ListType_ToString", (DL_FUNC) &_arrow_ListType_ToString, 1},
    {"_arrow_FixedWidthType_bit_width", (DL_FUNC) &_arrow_FixedWidthType_bit_width, 1},
    {"_arrow_DateType_unit", (DL_FUNC) &_arrow_DateType_unit, 1},
    {"_arrow_DecimalType_precision", (DL_FUNC) &_arrow_DecimalType_precision, 1},
    {"_arrow_DecimalType_scale", (DL_FUNC) &_arrow_DecimalType_scale, 1},
    {NULL, NULL, 0}
};

RcppExport void R_init_arrow(DllInfo *dll) {
    R_registerRoutines(dll, NULL, CallEntries, NULL, NULL);
    R_useDynamicSymbols(dll, FALSE);
}
