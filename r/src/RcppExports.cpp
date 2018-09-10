// Generated by using Rcpp::compileAttributes() -> do not edit by hand
// Generator token: 10BE3573-1514-4C36-9D1C-5A225CD40393

#include "arrow_types.h"
#include <Rcpp.h>

using namespace Rcpp;

// ArrayData_get_type
std::shared_ptr<arrow::DataType> ArrayData_get_type(const std::shared_ptr<arrow::ArrayData>& x);
RcppExport SEXP _arrow_ArrayData_get_type(SEXP xSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< const std::shared_ptr<arrow::ArrayData>& >::type x(xSEXP);
    rcpp_result_gen = Rcpp::wrap(ArrayData_get_type(x));
    return rcpp_result_gen;
END_RCPP
}
// ArrayData_get_length
int ArrayData_get_length(const std::shared_ptr<arrow::ArrayData>& x);
RcppExport SEXP _arrow_ArrayData_get_length(SEXP xSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< const std::shared_ptr<arrow::ArrayData>& >::type x(xSEXP);
    rcpp_result_gen = Rcpp::wrap(ArrayData_get_length(x));
    return rcpp_result_gen;
END_RCPP
}
// ArrayData_get_null_count
int ArrayData_get_null_count(const std::shared_ptr<arrow::ArrayData>& x);
RcppExport SEXP _arrow_ArrayData_get_null_count(SEXP xSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< const std::shared_ptr<arrow::ArrayData>& >::type x(xSEXP);
    rcpp_result_gen = Rcpp::wrap(ArrayData_get_null_count(x));
    return rcpp_result_gen;
END_RCPP
}
// ArrayData_get_offset
int ArrayData_get_offset(const std::shared_ptr<arrow::ArrayData>& x);
RcppExport SEXP _arrow_ArrayData_get_offset(SEXP xSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< const std::shared_ptr<arrow::ArrayData>& >::type x(xSEXP);
    rcpp_result_gen = Rcpp::wrap(ArrayData_get_offset(x));
    return rcpp_result_gen;
END_RCPP
}
// Array_IsNull
bool Array_IsNull(const std::shared_ptr<arrow::Array>& x, int i);
RcppExport SEXP _arrow_Array_IsNull(SEXP xSEXP, SEXP iSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< const std::shared_ptr<arrow::Array>& >::type x(xSEXP);
    Rcpp::traits::input_parameter< int >::type i(iSEXP);
    rcpp_result_gen = Rcpp::wrap(Array_IsNull(x, i));
    return rcpp_result_gen;
END_RCPP
}
// Array_IsValid
bool Array_IsValid(const std::shared_ptr<arrow::Array>& x, int i);
RcppExport SEXP _arrow_Array_IsValid(SEXP xSEXP, SEXP iSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< const std::shared_ptr<arrow::Array>& >::type x(xSEXP);
    Rcpp::traits::input_parameter< int >::type i(iSEXP);
    rcpp_result_gen = Rcpp::wrap(Array_IsValid(x, i));
    return rcpp_result_gen;
END_RCPP
}
// Array_length
int Array_length(const std::shared_ptr<arrow::Array>& x);
RcppExport SEXP _arrow_Array_length(SEXP xSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< const std::shared_ptr<arrow::Array>& >::type x(xSEXP);
    rcpp_result_gen = Rcpp::wrap(Array_length(x));
    return rcpp_result_gen;
END_RCPP
}
// Array_offset
int Array_offset(const std::shared_ptr<arrow::Array>& x);
RcppExport SEXP _arrow_Array_offset(SEXP xSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< const std::shared_ptr<arrow::Array>& >::type x(xSEXP);
    rcpp_result_gen = Rcpp::wrap(Array_offset(x));
    return rcpp_result_gen;
END_RCPP
}
// Array_null_count
int Array_null_count(const std::shared_ptr<arrow::Array>& x);
RcppExport SEXP _arrow_Array_null_count(SEXP xSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< const std::shared_ptr<arrow::Array>& >::type x(xSEXP);
    rcpp_result_gen = Rcpp::wrap(Array_null_count(x));
    return rcpp_result_gen;
END_RCPP
}
// Array_type
std::shared_ptr<arrow::DataType> Array_type(const std::shared_ptr<arrow::Array>& x);
RcppExport SEXP _arrow_Array_type(SEXP xSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< const std::shared_ptr<arrow::Array>& >::type x(xSEXP);
    rcpp_result_gen = Rcpp::wrap(Array_type(x));
    return rcpp_result_gen;
END_RCPP
}
// Array_type_id
arrow::Type::type Array_type_id(const std::shared_ptr<arrow::Array>& x);
RcppExport SEXP _arrow_Array_type_id(SEXP xSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< const std::shared_ptr<arrow::Array>& >::type x(xSEXP);
    rcpp_result_gen = Rcpp::wrap(Array_type_id(x));
    return rcpp_result_gen;
END_RCPP
}
// Array_Equals
bool Array_Equals(const std::shared_ptr<arrow::Array>& lhs, const std::shared_ptr<arrow::Array>& rhs);
RcppExport SEXP _arrow_Array_Equals(SEXP lhsSEXP, SEXP rhsSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< const std::shared_ptr<arrow::Array>& >::type lhs(lhsSEXP);
    Rcpp::traits::input_parameter< const std::shared_ptr<arrow::Array>& >::type rhs(rhsSEXP);
    rcpp_result_gen = Rcpp::wrap(Array_Equals(lhs, rhs));
    return rcpp_result_gen;
END_RCPP
}
// Array_ApproxEquals
bool Array_ApproxEquals(const std::shared_ptr<arrow::Array>& lhs, const std::shared_ptr<arrow::Array>& rhs);
RcppExport SEXP _arrow_Array_ApproxEquals(SEXP lhsSEXP, SEXP rhsSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< const std::shared_ptr<arrow::Array>& >::type lhs(lhsSEXP);
    Rcpp::traits::input_parameter< const std::shared_ptr<arrow::Array>& >::type rhs(rhsSEXP);
    rcpp_result_gen = Rcpp::wrap(Array_ApproxEquals(lhs, rhs));
    return rcpp_result_gen;
END_RCPP
}
// Array_data
std::shared_ptr<arrow::ArrayData> Array_data(const std::shared_ptr<arrow::Array>& array);
RcppExport SEXP _arrow_Array_data(SEXP arraySEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< const std::shared_ptr<arrow::Array>& >::type array(arraySEXP);
    rcpp_result_gen = Rcpp::wrap(Array_data(array));
    return rcpp_result_gen;
END_RCPP
}
// rvector_to_Array
std::shared_ptr<arrow::Array> rvector_to_Array(SEXP x);
RcppExport SEXP _arrow_rvector_to_Array(SEXP xSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< SEXP >::type x(xSEXP);
    rcpp_result_gen = Rcpp::wrap(rvector_to_Array(x));
    return rcpp_result_gen;
END_RCPP
}
// dataframe_to_RecordBatch
std::shared_ptr<arrow::RecordBatch> dataframe_to_RecordBatch(DataFrame tbl);
RcppExport SEXP _arrow_dataframe_to_RecordBatch(SEXP tblSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< DataFrame >::type tbl(tblSEXP);
    rcpp_result_gen = Rcpp::wrap(dataframe_to_RecordBatch(tbl));
    return rcpp_result_gen;
END_RCPP
}
// RecordBatch_num_columns
int RecordBatch_num_columns(const std::shared_ptr<arrow::RecordBatch>& x);
RcppExport SEXP _arrow_RecordBatch_num_columns(SEXP xSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< const std::shared_ptr<arrow::RecordBatch>& >::type x(xSEXP);
    rcpp_result_gen = Rcpp::wrap(RecordBatch_num_columns(x));
    return rcpp_result_gen;
END_RCPP
}
// RecordBatch_num_rows
int RecordBatch_num_rows(const std::shared_ptr<arrow::RecordBatch>& x);
RcppExport SEXP _arrow_RecordBatch_num_rows(SEXP xSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< const std::shared_ptr<arrow::RecordBatch>& >::type x(xSEXP);
    rcpp_result_gen = Rcpp::wrap(RecordBatch_num_rows(x));
    return rcpp_result_gen;
END_RCPP
}
// RecordBatch_schema
std::shared_ptr<arrow::Schema> RecordBatch_schema(const std::shared_ptr<arrow::RecordBatch>& x);
RcppExport SEXP _arrow_RecordBatch_schema(SEXP xSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< const std::shared_ptr<arrow::RecordBatch>& >::type x(xSEXP);
    rcpp_result_gen = Rcpp::wrap(RecordBatch_schema(x));
    return rcpp_result_gen;
END_RCPP
}
// RecordBatch_column
std::shared_ptr<arrow::Array> RecordBatch_column(const std::shared_ptr<arrow::RecordBatch>& batch, int i);
RcppExport SEXP _arrow_RecordBatch_column(SEXP batchSEXP, SEXP iSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< const std::shared_ptr<arrow::RecordBatch>& >::type batch(batchSEXP);
    Rcpp::traits::input_parameter< int >::type i(iSEXP);
    rcpp_result_gen = Rcpp::wrap(RecordBatch_column(batch, i));
    return rcpp_result_gen;
END_RCPP
}
// RecordBatch_to_dataframe
List RecordBatch_to_dataframe(const std::shared_ptr<arrow::RecordBatch>& batch);
RcppExport SEXP _arrow_RecordBatch_to_dataframe(SEXP batchSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< const std::shared_ptr<arrow::RecordBatch>& >::type batch(batchSEXP);
    rcpp_result_gen = Rcpp::wrap(RecordBatch_to_dataframe(batch));
    return rcpp_result_gen;
END_RCPP
}
// dataframe_to_Table
std::shared_ptr<arrow::Table> dataframe_to_Table(DataFrame tbl);
RcppExport SEXP _arrow_dataframe_to_Table(SEXP tblSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< DataFrame >::type tbl(tblSEXP);
    rcpp_result_gen = Rcpp::wrap(dataframe_to_Table(tbl));
    return rcpp_result_gen;
END_RCPP
}
// Table_num_columns
int Table_num_columns(const std::shared_ptr<arrow::Table>& x);
RcppExport SEXP _arrow_Table_num_columns(SEXP xSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< const std::shared_ptr<arrow::Table>& >::type x(xSEXP);
    rcpp_result_gen = Rcpp::wrap(Table_num_columns(x));
    return rcpp_result_gen;
END_RCPP
}
// Table_num_rows
int Table_num_rows(const std::shared_ptr<arrow::Table>& x);
RcppExport SEXP _arrow_Table_num_rows(SEXP xSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< const std::shared_ptr<arrow::Table>& >::type x(xSEXP);
    rcpp_result_gen = Rcpp::wrap(Table_num_rows(x));
    return rcpp_result_gen;
END_RCPP
}
// Table_schema
std::shared_ptr<arrow::Schema> Table_schema(const std::shared_ptr<arrow::Table>& x);
RcppExport SEXP _arrow_Table_schema(SEXP xSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< const std::shared_ptr<arrow::Table>& >::type x(xSEXP);
    rcpp_result_gen = Rcpp::wrap(Table_schema(x));
    return rcpp_result_gen;
END_RCPP
}
// RecordBatch_to_file
int RecordBatch_to_file(const std::shared_ptr<arrow::RecordBatch>& batch, std::string path);
RcppExport SEXP _arrow_RecordBatch_to_file(SEXP batchSEXP, SEXP pathSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< const std::shared_ptr<arrow::RecordBatch>& >::type batch(batchSEXP);
    Rcpp::traits::input_parameter< std::string >::type path(pathSEXP);
    rcpp_result_gen = Rcpp::wrap(RecordBatch_to_file(batch, path));
    return rcpp_result_gen;
END_RCPP
}
// read_record_batch_
std::shared_ptr<arrow::RecordBatch> read_record_batch_(std::string path);
RcppExport SEXP _arrow_read_record_batch_(SEXP pathSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< std::string >::type path(pathSEXP);
    rcpp_result_gen = Rcpp::wrap(read_record_batch_(path));
    return rcpp_result_gen;
END_RCPP
}
// Field_initialize
std::shared_ptr<arrow::Field> Field_initialize(const std::string& name, const std::shared_ptr<arrow::DataType>& type, bool nullable);
RcppExport SEXP _arrow_Field_initialize(SEXP nameSEXP, SEXP typeSEXP, SEXP nullableSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< const std::string& >::type name(nameSEXP);
    Rcpp::traits::input_parameter< const std::shared_ptr<arrow::DataType>& >::type type(typeSEXP);
    Rcpp::traits::input_parameter< bool >::type nullable(nullableSEXP);
    rcpp_result_gen = Rcpp::wrap(Field_initialize(name, type, nullable));
    return rcpp_result_gen;
END_RCPP
}
// Field_ToString
std::string Field_ToString(const std::shared_ptr<arrow::Field>& type);
RcppExport SEXP _arrow_Field_ToString(SEXP typeSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< const std::shared_ptr<arrow::Field>& >::type type(typeSEXP);
    rcpp_result_gen = Rcpp::wrap(Field_ToString(type));
    return rcpp_result_gen;
END_RCPP
}
// Field_name
std::string Field_name(std::shared_ptr<arrow::Field> type);
RcppExport SEXP _arrow_Field_name(SEXP typeSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< std::shared_ptr<arrow::Field> >::type type(typeSEXP);
    rcpp_result_gen = Rcpp::wrap(Field_name(type));
    return rcpp_result_gen;
END_RCPP
}
// Field_nullable
bool Field_nullable(std::shared_ptr<arrow::Field> type);
RcppExport SEXP _arrow_Field_nullable(SEXP typeSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< std::shared_ptr<arrow::Field> >::type type(typeSEXP);
    rcpp_result_gen = Rcpp::wrap(Field_nullable(type));
    return rcpp_result_gen;
END_RCPP
}
// ping_arrow
std::string ping_arrow();
RcppExport SEXP _arrow_ping_arrow() {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    rcpp_result_gen = Rcpp::wrap(ping_arrow());
    return rcpp_result_gen;
END_RCPP
}
// MemoryPool_default
static_ptr<arrow::MemoryPool> MemoryPool_default();
RcppExport SEXP _arrow_MemoryPool_default() {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    rcpp_result_gen = Rcpp::wrap(MemoryPool_default());
    return rcpp_result_gen;
END_RCPP
}
// MemoryPool_bytes_allocated
int MemoryPool_bytes_allocated(static_ptr<arrow::MemoryPool> pool);
RcppExport SEXP _arrow_MemoryPool_bytes_allocated(SEXP poolSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< static_ptr<arrow::MemoryPool> >::type pool(poolSEXP);
    rcpp_result_gen = Rcpp::wrap(MemoryPool_bytes_allocated(pool));
    return rcpp_result_gen;
END_RCPP
}
// MemoryPool_max_memory
int MemoryPool_max_memory(static_ptr<arrow::MemoryPool> pool);
RcppExport SEXP _arrow_MemoryPool_max_memory(SEXP poolSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< static_ptr<arrow::MemoryPool> >::type pool(poolSEXP);
    rcpp_result_gen = Rcpp::wrap(MemoryPool_max_memory(pool));
    return rcpp_result_gen;
END_RCPP
}
// Int8_initialize
std::shared_ptr<arrow::DataType> Int8_initialize();
RcppExport SEXP _arrow_Int8_initialize() {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    rcpp_result_gen = Rcpp::wrap(Int8_initialize());
    return rcpp_result_gen;
END_RCPP
}
// Int16_initialize
std::shared_ptr<arrow::DataType> Int16_initialize();
RcppExport SEXP _arrow_Int16_initialize() {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    rcpp_result_gen = Rcpp::wrap(Int16_initialize());
    return rcpp_result_gen;
END_RCPP
}
// Int32_initialize
std::shared_ptr<arrow::DataType> Int32_initialize();
RcppExport SEXP _arrow_Int32_initialize() {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    rcpp_result_gen = Rcpp::wrap(Int32_initialize());
    return rcpp_result_gen;
END_RCPP
}
// Int64_initialize
std::shared_ptr<arrow::DataType> Int64_initialize();
RcppExport SEXP _arrow_Int64_initialize() {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    rcpp_result_gen = Rcpp::wrap(Int64_initialize());
    return rcpp_result_gen;
END_RCPP
}
// UInt8_initialize
std::shared_ptr<arrow::DataType> UInt8_initialize();
RcppExport SEXP _arrow_UInt8_initialize() {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    rcpp_result_gen = Rcpp::wrap(UInt8_initialize());
    return rcpp_result_gen;
END_RCPP
}
// UInt16_initialize
std::shared_ptr<arrow::DataType> UInt16_initialize();
RcppExport SEXP _arrow_UInt16_initialize() {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    rcpp_result_gen = Rcpp::wrap(UInt16_initialize());
    return rcpp_result_gen;
END_RCPP
}
// UInt32_initialize
std::shared_ptr<arrow::DataType> UInt32_initialize();
RcppExport SEXP _arrow_UInt32_initialize() {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    rcpp_result_gen = Rcpp::wrap(UInt32_initialize());
    return rcpp_result_gen;
END_RCPP
}
// UInt64_initialize
std::shared_ptr<arrow::DataType> UInt64_initialize();
RcppExport SEXP _arrow_UInt64_initialize() {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    rcpp_result_gen = Rcpp::wrap(UInt64_initialize());
    return rcpp_result_gen;
END_RCPP
}
// Float16_initialize
std::shared_ptr<arrow::DataType> Float16_initialize();
RcppExport SEXP _arrow_Float16_initialize() {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    rcpp_result_gen = Rcpp::wrap(Float16_initialize());
    return rcpp_result_gen;
END_RCPP
}
// Float32_initialize
std::shared_ptr<arrow::DataType> Float32_initialize();
RcppExport SEXP _arrow_Float32_initialize() {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    rcpp_result_gen = Rcpp::wrap(Float32_initialize());
    return rcpp_result_gen;
END_RCPP
}
// Float64_initialize
std::shared_ptr<arrow::DataType> Float64_initialize();
RcppExport SEXP _arrow_Float64_initialize() {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    rcpp_result_gen = Rcpp::wrap(Float64_initialize());
    return rcpp_result_gen;
END_RCPP
}
// Boolean_initialize
std::shared_ptr<arrow::DataType> Boolean_initialize();
RcppExport SEXP _arrow_Boolean_initialize() {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    rcpp_result_gen = Rcpp::wrap(Boolean_initialize());
    return rcpp_result_gen;
END_RCPP
}
// Utf8_initialize
std::shared_ptr<arrow::DataType> Utf8_initialize();
RcppExport SEXP _arrow_Utf8_initialize() {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    rcpp_result_gen = Rcpp::wrap(Utf8_initialize());
    return rcpp_result_gen;
END_RCPP
}
// Date32_initialize
std::shared_ptr<arrow::DataType> Date32_initialize();
RcppExport SEXP _arrow_Date32_initialize() {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    rcpp_result_gen = Rcpp::wrap(Date32_initialize());
    return rcpp_result_gen;
END_RCPP
}
// Date64_initialize
std::shared_ptr<arrow::DataType> Date64_initialize();
RcppExport SEXP _arrow_Date64_initialize() {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    rcpp_result_gen = Rcpp::wrap(Date64_initialize());
    return rcpp_result_gen;
END_RCPP
}
// Null_initialize
std::shared_ptr<arrow::DataType> Null_initialize();
RcppExport SEXP _arrow_Null_initialize() {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    rcpp_result_gen = Rcpp::wrap(Null_initialize());
    return rcpp_result_gen;
END_RCPP
}
// Decimal128Type_initialize
std::shared_ptr<arrow::DataType> Decimal128Type_initialize(int32_t precision, int32_t scale);
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
std::shared_ptr<arrow::DataType> FixedSizeBinary_initialize(int32_t byte_width);
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
std::shared_ptr<arrow::DataType> Timestamp_initialize1(arrow::TimeUnit::type unit);
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
std::shared_ptr<arrow::DataType> Timestamp_initialize2(arrow::TimeUnit::type unit, const std::string& timezone);
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
std::shared_ptr<arrow::DataType> Time32_initialize(arrow::TimeUnit::type unit);
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
std::shared_ptr<arrow::DataType> Time64_initialize(arrow::TimeUnit::type unit);
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
std::shared_ptr<arrow::DataType> struct_(List fields);
RcppExport SEXP _arrow_struct_(SEXP fieldsSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< List >::type fields(fieldsSEXP);
    rcpp_result_gen = Rcpp::wrap(struct_(fields));
    return rcpp_result_gen;
END_RCPP
}
// DataType_ToString
std::string DataType_ToString(const std::shared_ptr<arrow::DataType>& type);
RcppExport SEXP _arrow_DataType_ToString(SEXP typeSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< const std::shared_ptr<arrow::DataType>& >::type type(typeSEXP);
    rcpp_result_gen = Rcpp::wrap(DataType_ToString(type));
    return rcpp_result_gen;
END_RCPP
}
// DataType_name
std::string DataType_name(const std::shared_ptr<arrow::DataType>& type);
RcppExport SEXP _arrow_DataType_name(SEXP typeSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< const std::shared_ptr<arrow::DataType>& >::type type(typeSEXP);
    rcpp_result_gen = Rcpp::wrap(DataType_name(type));
    return rcpp_result_gen;
END_RCPP
}
// DataType_Equals
bool DataType_Equals(const std::shared_ptr<arrow::DataType>& lhs, const std::shared_ptr<arrow::DataType>& rhs);
RcppExport SEXP _arrow_DataType_Equals(SEXP lhsSEXP, SEXP rhsSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< const std::shared_ptr<arrow::DataType>& >::type lhs(lhsSEXP);
    Rcpp::traits::input_parameter< const std::shared_ptr<arrow::DataType>& >::type rhs(rhsSEXP);
    rcpp_result_gen = Rcpp::wrap(DataType_Equals(lhs, rhs));
    return rcpp_result_gen;
END_RCPP
}
// DataType_num_children
int DataType_num_children(const std::shared_ptr<arrow::DataType>& type);
RcppExport SEXP _arrow_DataType_num_children(SEXP typeSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< const std::shared_ptr<arrow::DataType>& >::type type(typeSEXP);
    rcpp_result_gen = Rcpp::wrap(DataType_num_children(type));
    return rcpp_result_gen;
END_RCPP
}
// DataType_children_pointer
List DataType_children_pointer(const std::shared_ptr<arrow::DataType>& type);
RcppExport SEXP _arrow_DataType_children_pointer(SEXP typeSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< const std::shared_ptr<arrow::DataType>& >::type type(typeSEXP);
    rcpp_result_gen = Rcpp::wrap(DataType_children_pointer(type));
    return rcpp_result_gen;
END_RCPP
}
// DataType_id
arrow::Type::type DataType_id(const std::shared_ptr<arrow::DataType>& type);
RcppExport SEXP _arrow_DataType_id(SEXP typeSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< const std::shared_ptr<arrow::DataType>& >::type type(typeSEXP);
    rcpp_result_gen = Rcpp::wrap(DataType_id(type));
    return rcpp_result_gen;
END_RCPP
}
// schema_
std::shared_ptr<arrow::Schema> schema_(List fields);
RcppExport SEXP _arrow_schema_(SEXP fieldsSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< List >::type fields(fieldsSEXP);
    rcpp_result_gen = Rcpp::wrap(schema_(fields));
    return rcpp_result_gen;
END_RCPP
}
// Schema_ToString
std::string Schema_ToString(const std::shared_ptr<arrow::Schema>& s);
RcppExport SEXP _arrow_Schema_ToString(SEXP sSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< const std::shared_ptr<arrow::Schema>& >::type s(sSEXP);
    rcpp_result_gen = Rcpp::wrap(Schema_ToString(s));
    return rcpp_result_gen;
END_RCPP
}
// ListType_ToString
std::string ListType_ToString(const std::shared_ptr<arrow::ListType>& type);
RcppExport SEXP _arrow_ListType_ToString(SEXP typeSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< const std::shared_ptr<arrow::ListType>& >::type type(typeSEXP);
    rcpp_result_gen = Rcpp::wrap(ListType_ToString(type));
    return rcpp_result_gen;
END_RCPP
}
// FixedWidthType_bit_width
int FixedWidthType_bit_width(const std::shared_ptr<arrow::FixedWidthType>& type);
RcppExport SEXP _arrow_FixedWidthType_bit_width(SEXP typeSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< const std::shared_ptr<arrow::FixedWidthType>& >::type type(typeSEXP);
    rcpp_result_gen = Rcpp::wrap(FixedWidthType_bit_width(type));
    return rcpp_result_gen;
END_RCPP
}
// DateType_unit
arrow::DateUnit DateType_unit(const std::shared_ptr<arrow::DateType>& type);
RcppExport SEXP _arrow_DateType_unit(SEXP typeSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< const std::shared_ptr<arrow::DateType>& >::type type(typeSEXP);
    rcpp_result_gen = Rcpp::wrap(DateType_unit(type));
    return rcpp_result_gen;
END_RCPP
}
// TimeType_unit
arrow::TimeUnit::type TimeType_unit(const std::shared_ptr<arrow::TimeType>& type);
RcppExport SEXP _arrow_TimeType_unit(SEXP typeSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< const std::shared_ptr<arrow::TimeType>& >::type type(typeSEXP);
    rcpp_result_gen = Rcpp::wrap(TimeType_unit(type));
    return rcpp_result_gen;
END_RCPP
}
// DecimalType_precision
int32_t DecimalType_precision(const std::shared_ptr<arrow::DecimalType>& type);
RcppExport SEXP _arrow_DecimalType_precision(SEXP typeSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< const std::shared_ptr<arrow::DecimalType>& >::type type(typeSEXP);
    rcpp_result_gen = Rcpp::wrap(DecimalType_precision(type));
    return rcpp_result_gen;
END_RCPP
}
// DecimalType_scale
int32_t DecimalType_scale(const std::shared_ptr<arrow::DecimalType>& type);
RcppExport SEXP _arrow_DecimalType_scale(SEXP typeSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< const std::shared_ptr<arrow::DecimalType>& >::type type(typeSEXP);
    rcpp_result_gen = Rcpp::wrap(DecimalType_scale(type));
    return rcpp_result_gen;
END_RCPP
}
// TimestampType_timezone
std::string TimestampType_timezone(const std::shared_ptr<arrow::TimestampType>& type);
RcppExport SEXP _arrow_TimestampType_timezone(SEXP typeSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< const std::shared_ptr<arrow::TimestampType>& >::type type(typeSEXP);
    rcpp_result_gen = Rcpp::wrap(TimestampType_timezone(type));
    return rcpp_result_gen;
END_RCPP
}
// TimestampType_unit
arrow::TimeUnit::type TimestampType_unit(const std::shared_ptr<arrow::TimestampType>& type);
RcppExport SEXP _arrow_TimestampType_unit(SEXP typeSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< const std::shared_ptr<arrow::TimestampType>& >::type type(typeSEXP);
    rcpp_result_gen = Rcpp::wrap(TimestampType_unit(type));
    return rcpp_result_gen;
END_RCPP
}
// Status_ToString
std::string Status_ToString(const std::shared_ptr<arrow::Status>& status);
RcppExport SEXP _arrow_Status_ToString(SEXP statusSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< const std::shared_ptr<arrow::Status>& >::type status(statusSEXP);
    rcpp_result_gen = Rcpp::wrap(Status_ToString(status));
    return rcpp_result_gen;
END_RCPP
}
// Status_CodeAsString
std::string Status_CodeAsString(const std::shared_ptr<arrow::Status>& status);
RcppExport SEXP _arrow_Status_CodeAsString(SEXP statusSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< const std::shared_ptr<arrow::Status>& >::type status(statusSEXP);
    rcpp_result_gen = Rcpp::wrap(Status_CodeAsString(status));
    return rcpp_result_gen;
END_RCPP
}
// Status_code
arrow::StatusCode Status_code(const std::shared_ptr<arrow::Status>& status);
RcppExport SEXP _arrow_Status_code(SEXP statusSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< const std::shared_ptr<arrow::Status>& >::type status(statusSEXP);
    rcpp_result_gen = Rcpp::wrap(Status_code(status));
    return rcpp_result_gen;
END_RCPP
}
// Status_message
std::string Status_message(const std::shared_ptr<arrow::Status>& status);
RcppExport SEXP _arrow_Status_message(SEXP statusSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< const std::shared_ptr<arrow::Status>& >::type status(statusSEXP);
    rcpp_result_gen = Rcpp::wrap(Status_message(status));
    return rcpp_result_gen;
END_RCPP
}

static const R_CallMethodDef CallEntries[] = {
    {"_arrow_ArrayData_get_type", (DL_FUNC) &_arrow_ArrayData_get_type, 1},
    {"_arrow_ArrayData_get_length", (DL_FUNC) &_arrow_ArrayData_get_length, 1},
    {"_arrow_ArrayData_get_null_count", (DL_FUNC) &_arrow_ArrayData_get_null_count, 1},
    {"_arrow_ArrayData_get_offset", (DL_FUNC) &_arrow_ArrayData_get_offset, 1},
    {"_arrow_Array_IsNull", (DL_FUNC) &_arrow_Array_IsNull, 2},
    {"_arrow_Array_IsValid", (DL_FUNC) &_arrow_Array_IsValid, 2},
    {"_arrow_Array_length", (DL_FUNC) &_arrow_Array_length, 1},
    {"_arrow_Array_offset", (DL_FUNC) &_arrow_Array_offset, 1},
    {"_arrow_Array_null_count", (DL_FUNC) &_arrow_Array_null_count, 1},
    {"_arrow_Array_type", (DL_FUNC) &_arrow_Array_type, 1},
    {"_arrow_Array_type_id", (DL_FUNC) &_arrow_Array_type_id, 1},
    {"_arrow_Array_Equals", (DL_FUNC) &_arrow_Array_Equals, 2},
    {"_arrow_Array_ApproxEquals", (DL_FUNC) &_arrow_Array_ApproxEquals, 2},
    {"_arrow_Array_data", (DL_FUNC) &_arrow_Array_data, 1},
    {"_arrow_rvector_to_Array", (DL_FUNC) &_arrow_rvector_to_Array, 1},
    {"_arrow_dataframe_to_RecordBatch", (DL_FUNC) &_arrow_dataframe_to_RecordBatch, 1},
    {"_arrow_RecordBatch_num_columns", (DL_FUNC) &_arrow_RecordBatch_num_columns, 1},
    {"_arrow_RecordBatch_num_rows", (DL_FUNC) &_arrow_RecordBatch_num_rows, 1},
    {"_arrow_RecordBatch_schema", (DL_FUNC) &_arrow_RecordBatch_schema, 1},
    {"_arrow_RecordBatch_column", (DL_FUNC) &_arrow_RecordBatch_column, 2},
    {"_arrow_RecordBatch_to_dataframe", (DL_FUNC) &_arrow_RecordBatch_to_dataframe, 1},
    {"_arrow_dataframe_to_Table", (DL_FUNC) &_arrow_dataframe_to_Table, 1},
    {"_arrow_Table_num_columns", (DL_FUNC) &_arrow_Table_num_columns, 1},
    {"_arrow_Table_num_rows", (DL_FUNC) &_arrow_Table_num_rows, 1},
    {"_arrow_Table_schema", (DL_FUNC) &_arrow_Table_schema, 1},
    {"_arrow_RecordBatch_to_file", (DL_FUNC) &_arrow_RecordBatch_to_file, 2},
    {"_arrow_read_record_batch_", (DL_FUNC) &_arrow_read_record_batch_, 1},
    {"_arrow_Field_initialize", (DL_FUNC) &_arrow_Field_initialize, 3},
    {"_arrow_Field_ToString", (DL_FUNC) &_arrow_Field_ToString, 1},
    {"_arrow_Field_name", (DL_FUNC) &_arrow_Field_name, 1},
    {"_arrow_Field_nullable", (DL_FUNC) &_arrow_Field_nullable, 1},
    {"_arrow_ping_arrow", (DL_FUNC) &_arrow_ping_arrow, 0},
    {"_arrow_MemoryPool_default", (DL_FUNC) &_arrow_MemoryPool_default, 0},
    {"_arrow_MemoryPool_bytes_allocated", (DL_FUNC) &_arrow_MemoryPool_bytes_allocated, 1},
    {"_arrow_MemoryPool_max_memory", (DL_FUNC) &_arrow_MemoryPool_max_memory, 1},
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
    {"_arrow_DataType_id", (DL_FUNC) &_arrow_DataType_id, 1},
    {"_arrow_schema_", (DL_FUNC) &_arrow_schema_, 1},
    {"_arrow_Schema_ToString", (DL_FUNC) &_arrow_Schema_ToString, 1},
    {"_arrow_ListType_ToString", (DL_FUNC) &_arrow_ListType_ToString, 1},
    {"_arrow_FixedWidthType_bit_width", (DL_FUNC) &_arrow_FixedWidthType_bit_width, 1},
    {"_arrow_DateType_unit", (DL_FUNC) &_arrow_DateType_unit, 1},
    {"_arrow_TimeType_unit", (DL_FUNC) &_arrow_TimeType_unit, 1},
    {"_arrow_DecimalType_precision", (DL_FUNC) &_arrow_DecimalType_precision, 1},
    {"_arrow_DecimalType_scale", (DL_FUNC) &_arrow_DecimalType_scale, 1},
    {"_arrow_TimestampType_timezone", (DL_FUNC) &_arrow_TimestampType_timezone, 1},
    {"_arrow_TimestampType_unit", (DL_FUNC) &_arrow_TimestampType_unit, 1},
    {"_arrow_Status_ToString", (DL_FUNC) &_arrow_Status_ToString, 1},
    {"_arrow_Status_CodeAsString", (DL_FUNC) &_arrow_Status_CodeAsString, 1},
    {"_arrow_Status_code", (DL_FUNC) &_arrow_Status_code, 1},
    {"_arrow_Status_message", (DL_FUNC) &_arrow_Status_message, 1},
    {NULL, NULL, 0}
};

RcppExport void R_init_arrow(DllInfo *dll) {
    R_registerRoutines(dll, NULL, CallEntries, NULL, NULL);
    R_useDynamicSymbols(dll, FALSE);
}
