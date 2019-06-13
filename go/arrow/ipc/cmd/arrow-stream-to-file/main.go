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

package main // import "github.com/apache/arrow/go/arrow/ipc/cmd/arrow-stream-to-file"

import (
	"flag"
	"io"
	"log"
	"os"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/arrio"
	"github.com/apache/arrow/go/arrow/ipc"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/pkg/errors"
)

func main() {
	log.SetPrefix("arrow-stream-to-file: ")
	log.SetFlags(0)

	flag.Parse()

	err := processStream(os.Stdout, os.Stdin)
	if err != nil {
		log.Fatal(err)
	}
}

func processStream(w *os.File, r io.Reader) error {
	mem := memory.NewGoAllocator()

	rr, err := ipc.NewReader(r, ipc.WithAllocator(mem))
	if err != nil {
		if errors.Cause(err) == io.EOF {
			return nil
		}
		return err
	}

	ww, err := ipc.NewFileWriter(w, ipc.WithAllocator(mem), ipc.WithSchema(rr.Schema()))
	if err != nil {
		return errors.Wrap(err, "could not create ARROW file writer")
	}
	defer ww.Close()

	_, err = arrio.Copy(ww, rr)
	if err != nil {
		return errors.Wrap(err, "could not copy ARROW stream")
	}

	err = ww.Close()
	if err != nil {
		return errors.Wrap(err, "could not close output ARROW file")
	}

	return nil
}

func write(w *ipc.FileWriter, rec array.Record) error {
	defer rec.Release()
	return w.Write(rec)
}
