// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pqarrow_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/decimal128"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/apache/arrow/go/parquet/file"
	"github.com/apache/arrow/go/parquet/pqarrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func getDataDir() string {
	datadir := os.Getenv("PARQUET_TEST_DATA")
	if datadir == "" {
		panic("please point PARQUET_TEST_DATA env var to the test data directory")
	}
	return datadir
}

func TestArrowReaderAdHocReadDecimals(t *testing.T) {
	tests := []struct {
		file string
		typ  *arrow.Decimal128Type
	}{
		{"int32_decimal", &arrow.Decimal128Type{Precision: 4, Scale: 2}},
		{"int64_decimal", &arrow.Decimal128Type{Precision: 10, Scale: 2}},
		{"fixed_length_decimal", &arrow.Decimal128Type{Precision: 25, Scale: 2}},
		{"fixed_length_decimal_legacy", &arrow.Decimal128Type{Precision: 13, Scale: 2}},
		{"byte_array_decimal", &arrow.Decimal128Type{Precision: 4, Scale: 2}},
	}

	dataDir := getDataDir()
	for _, tt := range tests {
		t.Run(tt.file, func(t *testing.T) {
			filename := filepath.Join(dataDir, tt.file+".parquet")
			require.FileExists(t, filename)

			rdr, err := file.OpenParquetFile(filename, false)
			require.NoError(t, err)
			arrowRdr, err := pqarrow.NewFileReader(rdr, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
			require.NoError(t, err)

			tbl, err := arrowRdr.ReadTable(context.Background())
			require.NoError(t, err)

			assert.EqualValues(t, 1, tbl.NumCols())
			assert.Truef(t, arrow.TypeEqual(tbl.Schema().Field(0).Type, tt.typ), "expected: %s\ngot: %s", tbl.Schema().Field(0).Type, tt.typ)

			const expectedLen = 24
			valCol := tbl.Column(0)

			assert.EqualValues(t, expectedLen, valCol.Len())
			assert.Len(t, valCol.Data().Chunks(), 1)

			chunk := valCol.Data().Chunk(0)
			bldr := array.NewDecimal128Builder(memory.DefaultAllocator, tt.typ)
			defer bldr.Release()
			for i := 0; i < expectedLen; i++ {
				bldr.Append(decimal128.FromI64(int64((i + 1) * 100)))
			}

			expectedArr := bldr.NewDecimal128Array()
			defer expectedArr.Release()

			assert.Truef(t, array.ArrayEqual(expectedArr, chunk), "expected: %s\ngot: %s", expectedArr, chunk)
		})
	}
}

func TestRecordReaderParallel(t *testing.T) {
	tbl := makeDateTimeTypesTable(true, true)
	var buf bytes.Buffer
	require.NoError(t, pqarrow.WriteTable(tbl, &buf, tbl.NumRows(), nil, pqarrow.DefaultWriterProps(), memory.DefaultAllocator))

	pf, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	reader, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{BatchSize: 3, Parallel: true}, memory.DefaultAllocator)
	require.NoError(t, err)

	sc, err := reader.Schema()
	assert.NoError(t, err)
	assert.Truef(t, tbl.Schema().Equal(sc), "expected: %s\ngot: %s", tbl.Schema(), sc)

	rr, err := reader.GetRecordReader(context.Background(), nil, nil)
	assert.NoError(t, err)
	assert.NotNil(t, rr)
	defer rr.Release()

	records := make([]array.Record, 0)
	for rr.Next() {
		rec := rr.Record()
		defer rec.Release()

		assert.Truef(t, sc.Equal(rec.Schema()), "expected: %s\ngot: %s", sc, rec.Schema())
		rec.Retain()
		records = append(records, rec)
	}

	assert.False(t, rr.Next())

	tr := array.NewTableReader(tbl, 3)
	defer tr.Release()

	assert.True(t, tr.Next())
	assert.Truef(t, array.RecordEqual(tr.Record(), records[0]), "expected: %s\ngot: %s", tr.Record(), records[0])
	assert.True(t, tr.Next())
	assert.Truef(t, array.RecordEqual(tr.Record(), records[1]), "expected: %s\ngot: %s", tr.Record(), records[1])
}

func TestRecordReaderSerial(t *testing.T) {
	tbl := makeDateTimeTypesTable(true, true)
	var buf bytes.Buffer
	require.NoError(t, pqarrow.WriteTable(tbl, &buf, tbl.NumRows(), nil, pqarrow.DefaultWriterProps(), memory.DefaultAllocator))

	pf, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	reader, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{BatchSize: 2}, memory.DefaultAllocator)
	require.NoError(t, err)

	sc, err := reader.Schema()
	assert.NoError(t, err)
	assert.Truef(t, tbl.Schema().Equal(sc), "expected: %s\ngot: %s", tbl.Schema(), sc)

	rr, err := reader.GetRecordReader(context.Background(), nil, nil)
	assert.NoError(t, err)
	assert.NotNil(t, rr)
	defer rr.Release()

	tr := array.NewTableReader(tbl, 2)
	defer tr.Release()

	rec, err := rr.Read()
	assert.NoError(t, err)
	tr.Next()
	assert.Truef(t, array.RecordEqual(tr.Record(), rec), "expected: %s\ngot: %s", tr.Record(), rec)

	rec, err = rr.Read()
	assert.NoError(t, err)
	tr.Next()
	assert.Truef(t, array.RecordEqual(tr.Record(), rec), "expected: %s\ngot: %s", tr.Record(), rec)

	rec, err = rr.Read()
	assert.NoError(t, err)
	tr.Next()
	assert.Truef(t, array.RecordEqual(tr.Record(), rec), "expected: %s\ngot: %s", tr.Record(), rec)

	rec, err = rr.Read()
	assert.Same(t, io.EOF, err)
	assert.Nil(t, rec)
}
