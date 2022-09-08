// Code generated by "stringer -type=Type"; DO NOT EDIT.

package arrow

import "strconv"

func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	// Re-run the stringer command to generate them again.
	var x [1]struct{}
	_ = x[NULL-0]
	_ = x[BOOL-1]
	_ = x[UINT8-2]
	_ = x[INT8-3]
	_ = x[UINT16-4]
	_ = x[INT16-5]
	_ = x[UINT32-6]
	_ = x[INT32-7]
	_ = x[UINT64-8]
	_ = x[INT64-9]
	_ = x[FLOAT16-10]
	_ = x[FLOAT32-11]
	_ = x[FLOAT64-12]
	_ = x[STRING-13]
	_ = x[BINARY-14]
	_ = x[FIXED_SIZE_BINARY-15]
	_ = x[DATE32-16]
	_ = x[DATE64-17]
	_ = x[TIMESTAMP-18]
	_ = x[TIME32-19]
	_ = x[TIME64-20]
	_ = x[INTERVAL_MONTHS-21]
	_ = x[INTERVAL_DAY_TIME-22]
	_ = x[DECIMAL128-23]
	_ = x[DECIMAL256-24]
	_ = x[LIST-25]
	_ = x[STRUCT-26]
	_ = x[SPARSE_UNION-27]
	_ = x[DENSE_UNION-28]
	_ = x[DICTIONARY-29]
	_ = x[MAP-30]
	_ = x[EXTENSION-31]
	_ = x[FIXED_SIZE_LIST-32]
	_ = x[DURATION-33]
	_ = x[LARGE_STRING-34]
	_ = x[LARGE_BINARY-35]
	_ = x[LARGE_LIST-36]
	_ = x[INTERVAL_MONTH_DAY_NANO-37]
	_ = x[INTERVAL-38]
}

const _Type_name = "NULLBOOLUINT8INT8UINT16INT16UINT32INT32UINT64INT64FLOAT16FLOAT32FLOAT64STRINGBINARYFIXED_SIZE_BINARYDATE32DATE64TIMESTAMPTIME32TIME64INTERVAL_MONTHSINTERVAL_DAY_TIMEDECIMAL128DECIMAL256LISTSTRUCTSPARSE_UNIONDENSE_UNIONDICTIONARYMAPEXTENSIONFIXED_SIZE_LISTDURATIONLARGE_STRINGLARGE_BINARYLARGE_LISTINTERVAL_MONTH_DAY_NANOINTERVAL"

var _Type_index = [...]uint16{0, 4, 8, 13, 17, 23, 28, 34, 39, 45, 50, 57, 64, 71, 77, 83, 100, 106, 112, 121, 127, 133, 148, 165, 175, 185, 189, 195, 207, 218, 228, 231, 240, 255, 263, 275, 287, 297, 320, 328}

func (i Type) String() string {
	if i < 0 || i >= Type(len(_Type_index)-1) {
		return "Type(" + strconv.FormatInt(int64(i), 10) + ")"
	}
	return _Type_name[_Type_index[i]:_Type_index[i+1]]
}
