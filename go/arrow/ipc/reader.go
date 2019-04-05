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

package ipc // import "github.com/apache/arrow/go/arrow/ipc"

import (
	"bytes"
	"io"
	"sync/atomic"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/internal/debug"
	"github.com/apache/arrow/go/arrow/internal/flatbuf"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/pkg/errors"
)

// Reader reads records from an io.Reader.
// Reader expects a schema (plus any dictionaries) as the first messages
// in the stream, followed by records.
type Reader struct {
	r      *MessageReader
	schema *arrow.Schema

	refCount int64
	rec      array.Record
	err      error

	types dictTypeMap
	memo  dictMemo

	mem memory.Allocator

	done bool
}

// NewReader returns a reader that reads records from an input stream.
func NewReader(r io.Reader, opts ...Option) (*Reader, error) {
	msg, err := NewMessageReader(r)
	if err != nil {
		return nil, errors.Wrap(err, "arrow/ipc: could create message reader")
	}

	cfg := newConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	rr := &Reader{
		r:     msg,
		types: make(dictTypeMap),
		memo:  newMemo(),
		mem:   cfg.alloc,
	}

	err = rr.readSchema()
	if err != nil {
		return nil, errors.Wrap(err, "arrow/ipc: could not read schema from stream")
	}

	return rr, nil
}

// Err returns the last error encountered during the iteration over the
// underlying stream.
func (r *Reader) Err() error { return r.err }

func (r *Reader) Schema() *arrow.Schema { return r.schema }

func (r *Reader) readSchema() error {
	msg, err := r.r.Message()
	if err != nil {
		return errors.Wrap(err, "arrow/ipc: could not read message schema")
	}

	if msg.Type() != MessageSchema {
		return errors.Errorf("arrow/ipc: invalid message type (got=%v, want=%v)", msg.Type(), MessageSchema)
	}

	// FIXME(sbinet) refactor msg-header handling.
	var schemaFB flatbuf.Schema
	initFB(&schemaFB, msg.msg.Header)

	r.types, err = dictTypesFromFB(&schemaFB)
	if err != nil {
		return errors.Wrap(err, "arrow/ipc: could read dictionary types from message schema")
	}

	// TODO(sbinet): in the future, we may want to reconcile IDs in the stream with
	// those found in the schema.
	for range r.types {
		panic("not implemented") // FIXME(sbinet): ReadNextDictionary
	}

	r.schema, err = schemaFromFB(&schemaFB, &r.memo)
	if err != nil {
		return errors.Wrap(err, "arrow/ipc: could not decode schema from message schema")
	}

	return nil
}

// Retain increases the reference count by 1.
// Retain may be called simultaneously from multiple goroutines.
func (r *Reader) Retain() {
	atomic.AddInt64(&r.refCount, 1)
}

// Release decreases the reference count by 1.
// When the reference count goes to zero, the memory is freed.
// Release may be called simultaneously from multiple goroutines.
func (r *Reader) Release() {
	debug.Assert(atomic.LoadInt64(&r.refCount) > 0, "too many releases")

	if atomic.AddInt64(&r.refCount, -1) == 0 {
		if r.rec != nil {
			r.rec.Release()
			r.rec = nil
		}
		if r.r != nil {
			r.r.Release()
			r.r = nil
		}
	}
}

// Next returns whether a Record could be extracted from the underlying stream.
func (r *Reader) Next() bool {
	if r.rec != nil {
		r.rec.Release()
		r.rec = nil
	}

	if r.err != nil || r.done {
		return false
	}

	return r.next()
}

func (r *Reader) next() bool {
	var msg *Message
	msg, r.err = r.r.Message()
	if r.err != nil {
		r.done = true
		if r.err == io.EOF {
			r.err = nil
		}
		return false
	}

	if got, want := msg.Type(), MessageRecordBatch; got != want {
		r.err = errors.Errorf("arrow/ipc: invalid message type (got=%v, want=%v", got, want)
		return false
	}

	r.rec = newRecord(r.schema, msg.meta, bytes.NewReader(msg.body.Bytes()))
	return true
}

// Record returns the current record that has been extracted from the
// underlying stream.
// It is valid until the next call to Next.
func (r *Reader) Record() array.Record {
	return r.rec
}

var (
	_ array.RecordReader = (*Reader)(nil)
)
