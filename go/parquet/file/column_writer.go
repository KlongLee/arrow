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

package file

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/apache/arrow/go/v7/arrow"
	"github.com/apache/arrow/go/v7/arrow/array"
	"github.com/apache/arrow/go/v7/arrow/bitutil"
	"github.com/apache/arrow/go/v7/arrow/memory"
	"github.com/apache/arrow/go/v7/parquet"
	"github.com/apache/arrow/go/v7/parquet/internal/encoding"
	"github.com/apache/arrow/go/v7/parquet/metadata"
	"github.com/apache/arrow/go/v7/parquet/schema"
)

//go:generate go run ../../arrow/_tools/tmpl/main.go -i -data=../internal/encoding/physical_types.tmpldata column_writer_types.gen.go.tmpl

// ColumnChunkWriter is the base interface for all columnwriters. To directly write
// data to the column, you need to assert it to the correctly typed ColumnChunkWriter
// instance, such as Int32ColumnWriter.
type ColumnChunkWriter interface {
	// Close ends this column and returns the number of bytes written
	Close() error
	// Type returns the underlying physical parquet type for this column
	Type() parquet.Type
	// Descr returns the column information for this writer
	Descr() *schema.Column
	// RowsWritten returns the number of rows that have so far been written with this writer
	RowsWritten() int
	// TotalCompressedBytes returns the number of bytes, after compression, that have been written so far
	TotalCompressedBytes() int64
	// TotalBytesWritten includes the bytes for writing dictionary pages, while TotalCompressedBytes is
	// just the data and page headers
	TotalBytesWritten() int64
	// Properties returns the current WriterProperties in use for this writer
	Properties() *parquet.WriterProperties

	LevelInfo() LevelInfo
	SetBitsBuffer(*memory.Buffer)
}

func computeLevelInfo(descr *schema.Column) (info LevelInfo) {
	info.DefLevel = descr.MaxDefinitionLevel()
	info.RepLevel = descr.MaxRepetitionLevel()

	minSpacedDefLevel := descr.MaxDefinitionLevel()
	n := descr.SchemaNode()
	for n != nil && n.RepetitionType() != parquet.Repetitions.Repeated {
		if n.RepetitionType() == parquet.Repetitions.Optional {
			minSpacedDefLevel--
		}
		n = n.Parent()
	}
	info.RepeatedAncestorDefLevel = minSpacedDefLevel
	return
}

type columnWriter struct {
	metaData *metadata.ColumnChunkMetaDataBuilder
	descr    *schema.Column

	// scratch buffer if validity bits need to be recalculated
	bitsBuffer *memory.Buffer
	levelInfo  LevelInfo
	pager      PageWriter
	hasDict    bool
	encoding   parquet.Encoding
	props      *parquet.WriterProperties
	defEncoder encoding.LevelEncoder
	repEncoder encoding.LevelEncoder
	mem        memory.Allocator

	pageStatistics  metadata.TypedStatistics
	chunkStatistics metadata.TypedStatistics

	// total number of values stored in the current data page. this is the maximum
	// of the number of encoded def levels or encoded values. for
	// non-repeated, required columns, this is equal to the number of encoded
	// values. For repeated or optional values, there may be fewer data values
	// than levels, and this tells you how many encoded levels there are in that case
	numBufferedValues int64

	// the total number of stored values in the current page. for repeated or optional
	// values. this number may be lower than numBuffered
	numDataValues int64

	rowsWritten       int
	totalBytesWritten int64
	// records the current number of compressed bytes in a column
	totalCompressedBytes int64
	closed               bool
	fallbackToNonDict    bool

	pages []DataPage

	defLevelSink *encoding.PooledBufferWriter
	repLevelSink *encoding.PooledBufferWriter

	uncompressedData bytes.Buffer
	compressedTemp   *bytes.Buffer

	currentEncoder encoding.TypedEncoder
}

func newColumnWriterBase(metaData *metadata.ColumnChunkMetaDataBuilder, pager PageWriter, useDict bool, enc parquet.Encoding, props *parquet.WriterProperties) columnWriter {
	ret := columnWriter{
		metaData:     metaData,
		descr:        metaData.Descr(),
		levelInfo:    computeLevelInfo(metaData.Descr()),
		pager:        pager,
		hasDict:      useDict,
		encoding:     enc,
		props:        props,
		mem:          props.Allocator(),
		defLevelSink: encoding.NewPooledBufferWriter(0),
		repLevelSink: encoding.NewPooledBufferWriter(0),
	}
	if pager.HasCompressor() {
		ret.compressedTemp = new(bytes.Buffer)
	}
	if props.StatisticsEnabledFor(ret.descr.Path()) && ret.descr.SortOrder() != schema.SortUNKNOWN {
		ret.pageStatistics = metadata.NewStatistics(ret.descr, props.Allocator())
		ret.chunkStatistics = metadata.NewStatistics(ret.descr, props.Allocator())
	}

	ret.defEncoder.Init(parquet.Encodings.RLE, ret.descr.MaxDefinitionLevel(), ret.defLevelSink)
	ret.repEncoder.Init(parquet.Encodings.RLE, ret.descr.MaxRepetitionLevel(), ret.repLevelSink)

	ret.reset()

	return ret
}

func (w *columnWriter) SetBitsBuffer(buf *memory.Buffer) { w.bitsBuffer = buf }

func (w *columnWriter) LevelInfo() LevelInfo { return w.levelInfo }

func (w *columnWriter) Type() parquet.Type {
	return w.descr.PhysicalType()
}

func (w *columnWriter) Descr() *schema.Column {
	return w.descr
}

func (w *columnWriter) Properties() *parquet.WriterProperties {
	return w.props
}

func (w *columnWriter) TotalCompressedBytes() int64 {
	return w.totalCompressedBytes
}

func (w *columnWriter) TotalBytesWritten() int64 {
	return w.totalBytesWritten
}

func (w *columnWriter) RowsWritten() int {
	return w.rowsWritten
}

func (w *columnWriter) WriteDataPage(page DataPage) error {
	written, err := w.pager.WriteDataPage(page)
	w.totalBytesWritten += written
	return err
}

func (w *columnWriter) WriteDefinitionLevels(levels []int16) {
	w.defEncoder.EncodeNoFlush(levels)
}

func (w *columnWriter) WriteRepetitionLevels(levels []int16) {
	w.repEncoder.EncodeNoFlush(levels)
}

func (w *columnWriter) reset() {
	w.defLevelSink.Reset(0)
	w.repLevelSink.Reset(0)

	if w.props.DataPageVersion() == parquet.DataPageV1 {
		// offset the buffers to make room to record the number of levels at the
		// beginning of each after we've encoded them with RLE
		if w.descr.MaxDefinitionLevel() > 0 {
			w.defLevelSink.SetOffset(arrow.Uint32SizeBytes)
		}
		if w.descr.MaxRepetitionLevel() > 0 {
			w.repLevelSink.SetOffset(arrow.Uint32SizeBytes)
		}
	}

	w.defEncoder.Reset(w.descr.MaxDefinitionLevel())
	w.repEncoder.Reset(w.descr.MaxRepetitionLevel())
}

func (w *columnWriter) concatBuffers(defLevelsSize, repLevelsSize int64, values []byte, wr io.Writer) {
	wr.Write(w.repLevelSink.Bytes()[:repLevelsSize])
	wr.Write(w.defLevelSink.Bytes()[:defLevelsSize])
	wr.Write(values)
}

func (w *columnWriter) EstimatedBufferedValueBytes() int64 {
	return w.currentEncoder.EstimatedDataEncodedSize()
}

func (w *columnWriter) commitWriteAndCheckPageLimit(numLevels, numValues int64) error {
	w.numBufferedValues += numLevels
	w.numDataValues += numValues

	if w.currentEncoder.EstimatedDataEncodedSize() >= w.props.DataPageSize() {
		return w.FlushCurrentPage()
	}
	return nil
}

func (w *columnWriter) FlushCurrentPage() error {
	var (
		defLevelsRLESize int64 = 0
		repLevelsRLESize int64 = 0
	)

	values, err := w.currentEncoder.FlushValues()
	if err != nil {
		return err
	}
	defer values.Release()

	isV1DataPage := w.props.DataPageVersion() == parquet.DataPageV1
	if w.descr.MaxDefinitionLevel() > 0 {
		w.defEncoder.Flush()
		w.defLevelSink.SetOffset(0)
		sz := w.defEncoder.Len()
		if isV1DataPage {
			sz += arrow.Uint32SizeBytes
			binary.LittleEndian.PutUint32(w.defLevelSink.Bytes(), uint32(w.defEncoder.Len()))
		}
		defLevelsRLESize = int64(sz)
	}

	if w.descr.MaxRepetitionLevel() > 0 {
		w.repEncoder.Flush()
		w.repLevelSink.SetOffset(0)
		if isV1DataPage {
			binary.LittleEndian.PutUint32(w.repLevelSink.Bytes(), uint32(w.repEncoder.Len()))
		}
		repLevelsRLESize = int64(w.repLevelSink.Len())
	}

	uncompressed := defLevelsRLESize + repLevelsRLESize + int64(values.Len())
	if isV1DataPage {
		w.buildDataPageV1(defLevelsRLESize, repLevelsRLESize, uncompressed, values.Bytes())
	} else {
		w.buildDataPageV2(defLevelsRLESize, repLevelsRLESize, uncompressed, values.Bytes())
	}

	w.reset()
	w.numBufferedValues, w.numDataValues = 0, 0
	return nil
}

func (w *columnWriter) buildDataPageV1(defLevelsRLESize, repLevelsRLESize, uncompressed int64, values []byte) error {
	w.uncompressedData.Reset()
	w.uncompressedData.Grow(int(uncompressed))
	w.concatBuffers(defLevelsRLESize, repLevelsRLESize, values, &w.uncompressedData)

	pageStats, err := w.getPageStatistics()
	if err != nil {
		return err
	}
	pageStats.ApplyStatSizeLimits(int(w.props.MaxStatsSizeFor(w.descr.Path())))
	pageStats.Signed = schema.SortSIGNED == w.descr.SortOrder()
	w.resetPageStatistics()

	var data []byte
	if w.pager.HasCompressor() {
		w.compressedTemp.Reset()
		data = w.pager.Compress(w.compressedTemp, w.uncompressedData.Bytes())
	} else {
		data = w.uncompressedData.Bytes()
	}

	// write the page to sink eagerly if there's no dictionary or if dictionary encoding has fallen back
	if w.hasDict && !w.fallbackToNonDict {
		pageSlice := make([]byte, len(data))
		copy(pageSlice, data)
		page := NewDataPageV1WithStats(memory.NewBufferBytes(pageSlice), int32(w.numBufferedValues), w.encoding, parquet.Encodings.RLE, parquet.Encodings.RLE, uncompressed, pageStats)
		w.totalCompressedBytes += int64(page.buf.Len()) // + size of Pageheader
		w.pages = append(w.pages, page)
	} else {
		w.totalCompressedBytes += int64(len(data))
		dp := NewDataPageV1WithStats(memory.NewBufferBytes(data), int32(w.numBufferedValues), w.encoding, parquet.Encodings.RLE, parquet.Encodings.RLE, uncompressed, pageStats)
		defer dp.Release()
		w.WriteDataPage(dp)
	}
	return nil
}

func (w *columnWriter) buildDataPageV2(defLevelsRLESize, repLevelsRLESize, uncompressed int64, values []byte) error {
	var data []byte
	if w.pager.HasCompressor() {
		w.compressedTemp.Reset()
		data = w.pager.Compress(w.compressedTemp, values)
		// data = w.compressedTemp.Bytes()
	} else {
		data = values
	}

	// concatenate uncompressed levels and the possibly compressed values
	var combined bytes.Buffer
	combined.Grow(int(defLevelsRLESize + repLevelsRLESize + int64(len(data))))
	w.concatBuffers(defLevelsRLESize, repLevelsRLESize, data, &combined)

	pageStats, err := w.getPageStatistics()
	if err != nil {
		return err
	}
	pageStats.ApplyStatSizeLimits(int(w.props.MaxStatsSizeFor(w.descr.Path())))
	pageStats.Signed = schema.SortSIGNED == w.descr.SortOrder()
	w.resetPageStatistics()

	numValues := int32(w.numBufferedValues)
	nullCount := int32(pageStats.NullCount)
	defLevelsByteLen := int32(defLevelsRLESize)
	repLevelsByteLen := int32(repLevelsRLESize)

	page := NewDataPageV2WithStats(memory.NewBufferBytes(combined.Bytes()), numValues, nullCount, numValues, w.encoding,
		defLevelsByteLen, repLevelsByteLen, uncompressed, w.pager.HasCompressor(), pageStats)
	if w.hasDict && !w.fallbackToNonDict {
		w.totalCompressedBytes += int64(page.buf.Len()) // + sizeof pageheader
		w.pages = append(w.pages, page)
	} else {
		w.totalCompressedBytes += int64(combined.Len())
		defer page.Release()
		w.WriteDataPage(page)
	}
	return nil
}

func (w *columnWriter) FlushBufferedDataPages() {
	if w.numBufferedValues > 0 {
		w.FlushCurrentPage()
	}

	for _, p := range w.pages {
		defer p.Release()
		w.WriteDataPage(p)
	}
	w.pages = w.pages[:0]
	w.totalCompressedBytes = 0
}

func (w *columnWriter) writeLevels(numValues int64, defLevels, repLevels []int16) int64 {
	toWrite := int64(0)
	// if the field is required and non-repeated, no definition levels
	if defLevels != nil && w.descr.MaxDefinitionLevel() > 0 {
		for _, v := range defLevels {
			if v == w.descr.MaxDefinitionLevel() {
				toWrite++
			}
		}
		w.WriteDefinitionLevels(defLevels[:numValues])
	} else {
		toWrite = numValues
	}

	if repLevels != nil && w.descr.MaxRepetitionLevel() > 0 {
		// a row could include more than one value
		//count the occasions where we start a new row
		for _, v := range repLevels {
			if v == 0 {
				w.rowsWritten++
			}
		}

		w.WriteRepetitionLevels(repLevels[:numValues])
	} else {
		// each value is exactly 1 row
		w.rowsWritten += int(numValues)
	}
	return toWrite
}

func (w *columnWriter) writeLevelsSpaced(numLevels int64, defLevels, repLevels []int16) {
	if w.descr.MaxDefinitionLevel() > 0 {
		w.WriteDefinitionLevels(defLevels[:numLevels])
	}

	if w.descr.MaxRepetitionLevel() > 0 {
		for _, v := range repLevels {
			if v == 0 {
				w.rowsWritten++
			}
		}
		w.WriteRepetitionLevels(repLevels[:numLevels])
	} else {
		w.rowsWritten += int(numLevels)
	}
}

func (w *columnWriter) WriteDictionaryPage() error {
	dictEncoder := w.currentEncoder.(encoding.DictEncoder)
	buffer := memory.NewResizableBuffer(w.mem)
	buffer.Resize(dictEncoder.DictEncodedSize())
	dictEncoder.WriteDict(buffer.Bytes())
	defer buffer.Release()

	page := NewDictionaryPage(buffer, int32(dictEncoder.NumEntries()), w.props.DictionaryPageEncoding())
	written, err := w.pager.WriteDictionaryPage(page)
	w.totalBytesWritten += written
	return err
}

type batchWriteInfo struct {
	batchNum  int64
	nullCount int64
}

func (b batchWriteInfo) numSpaced() int64 { return b.batchNum + b.nullCount }

// this will always update the three otuput params
// outValsToWrite, outSpacedValsToWrite, and NullCount. Additionally
// it will update the validity bitmap if required (i.e. if at least one
// level of nullable structs directly preceed the leaf node)
func (w *columnWriter) maybeCalculateValidityBits(defLevels []int16, batchSize int64) (out batchWriteInfo) {
	if w.bitsBuffer == nil {
		if w.levelInfo.DefLevel == 0 {
			// in this case def levels should be null and we only
			// need to output counts which will always be equal to
			// the batch size passed in (max def level == 0 indicates
			// there cannot be repeated or null fields)
			out.batchNum = batchSize
			out.nullCount = 0
		} else {
			var (
				toWrite       int64
				spacedToWrite int64
			)
			for i := int64(0); i < batchSize; i++ {
				if defLevels[i] == w.levelInfo.DefLevel {
					toWrite++
				}
				if defLevels[i] >= w.levelInfo.RepeatedAncestorDefLevel {
					spacedToWrite++
				}
			}
			out.batchNum += toWrite
			out.nullCount = spacedToWrite - toWrite
		}
		return
	}

	// shrink to fit possible causes another allocation
	newBitmapSize := bitutil.BytesForBits(batchSize)
	if newBitmapSize != int64(w.bitsBuffer.Len()) {
		w.bitsBuffer.ResizeNoShrink(int(newBitmapSize))
	}

	io := ValidityBitmapInputOutput{
		ValidBits:      w.bitsBuffer.Bytes(),
		ReadUpperBound: batchSize,
	}
	DefLevelsToBitmap(defLevels[:batchSize], w.levelInfo, &io)
	out.batchNum = io.Read - io.NullCount
	out.nullCount = io.NullCount
	return
}

func (w *columnWriter) getPageStatistics() (enc metadata.EncodedStatistics, err error) {
	if w.pageStatistics != nil {
		enc, err = w.pageStatistics.Encode()
	}
	return
}

func (w *columnWriter) getChunkStatistics() (enc metadata.EncodedStatistics, err error) {
	if w.chunkStatistics != nil {
		enc, err = w.chunkStatistics.Encode()
	}
	return
}

func (w *columnWriter) resetPageStatistics() {
	if w.chunkStatistics != nil {
		w.chunkStatistics.Merge(w.pageStatistics)
		w.pageStatistics.Reset()
	}
}

func (w *columnWriter) Close() (err error) {
	if !w.closed {
		w.closed = true
		if w.hasDict && !w.fallbackToNonDict {
			w.WriteDictionaryPage()
		}

		w.FlushBufferedDataPages()

		var chunkStats metadata.EncodedStatistics
		chunkStats, err = w.getChunkStatistics()
		if err != nil {
			return err
		}

		chunkStats.ApplyStatSizeLimits(int(w.props.MaxStatsSizeFor(w.descr.Path())))
		chunkStats.Signed = schema.SortSIGNED == w.descr.SortOrder()

		if w.rowsWritten > 0 && chunkStats.IsSet() {
			w.metaData.SetStats(chunkStats)
		}
		err = w.pager.Close(w.hasDict, w.fallbackToNonDict)

		w.defLevelSink.Reset(0)
		w.repLevelSink.Reset(0)
	}
	return err
}

func doBatches(total, batchSize int64, action func(offset, batch int64)) {
	numBatches := total / batchSize
	for i := int64(0); i < numBatches; i++ {
		action(i*batchSize, batchSize)
	}
	if total%batchSize > 0 {
		action(numBatches*batchSize, total%batchSize)
	}
}

func levelSliceOrNil(rep []int16, offset, batch int64) []int16 {
	if rep == nil {
		return nil
	}
	return rep[offset : batch+offset]
}

func (w *ByteArrayColumnChunkWriter) maybeReplaceValidity(values array.Interface, newNullCount int64) array.Interface {
	if w.bitsBuffer == nil {
		return values
	}

	buffers := values.Data().Buffers()
	if len(buffers) == 0 {
		return values
	}
	// bitsBuffer should already be the offset slice of the validity bits
	// we want so we don't need to manually slice the validity buffer
	buffers[0] = w.bitsBuffer

	if values.Data().Offset() > 0 {
		data := values.Data()
		buffers[1] = memory.NewBufferBytes(data.Buffers()[1].Bytes()[data.Offset()*arrow.Int32SizeBytes : data.Len()*arrow.Int32SizeBytes])
	}
	return array.MakeFromData(array.NewData(values.DataType(), values.Len(), buffers, nil, int(newNullCount), 0))
}
