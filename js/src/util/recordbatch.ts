// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import { Column } from '../column';
import { Vector } from '../vector';
import { DataType } from '../type';
import { Data, Buffers } from '../data';
import { Schema, Field } from '../schema';
import { RecordBatch } from '../recordbatch';

const noopBuf = new Uint8Array(0);
const nullBufs = (bitmapLength: number) => <unknown> [
    noopBuf, noopBuf, new Uint8Array(bitmapLength), noopBuf
] as Buffers<any>;

/** @ignore */
export function distributeColumnsIntoRecordBatches<T extends { [key: string]: DataType } = any>(columns: Column<T[keyof T]>[]): [Schema<T>, RecordBatch<T>[]] {
    return distributeVectorsIntoRecordBatches<T>(new Schema<T>(columns.map(({ field }) => field)), columns);
}

/** @ignore */
export function distributeVectorsIntoRecordBatches<T extends { [key: string]: DataType } = any>(schema: Schema<T>, vecs: (Vector<T[keyof T]> | Chunked<T[keyof T]>)[]): [Schema<T>, RecordBatch<T>[]] {
    return uniformlyDistributeChunksAcrossRecordBatches<T>(schema, vecs.map((v) => v.chunks));
}

/** @ignore */
function uniformlyDistributeChunksAcrossRecordBatches<T extends { [key: string]: DataType } = any>(schema: Schema<T>, columns: Data<T[keyof T]>[][]): [Schema<T>, RecordBatch<T>[]] {

    const fields = [...schema.fields];
    const batchArgs = [] as [number, Data<T[keyof T]>[]][];
    const memo = { numBatches: columns.reduce((n, c) => Math.max(n, c.length), 0) };

    let numBatches = 0, batchLength = 0;
    let i = -1;
    const numColumns = columns.length;
    let child: Data<T[keyof T]>, children: Data<T[keyof T]>[] = [];

    while (memo.numBatches-- > 0) {

        for (batchLength = Number.POSITIVE_INFINITY, i = -1; ++i < numColumns;) {
            children[i] = child = columns[i].shift()!;
            batchLength = Math.min(batchLength, child ? child.length : batchLength);
        }

        if (isFinite(batchLength)) {
            children = distributechildren(fields, batchLength, children, columns, memo);
            if (batchLength > 0) {
                batchArgs[numBatches++] = [batchLength, children.slice()];
            }
        }
    }
    return [
        schema = new Schema<T>(fields, schema.metadata),
        batchArgs.map((xs) => new RecordBatch(schema, ...xs))
    ];
}

/** @ignore */
function distributechildren<T extends { [key: string]: DataType } = any>(fields: Field<T[keyof T]>[], batchLength: number, children: Data<T[keyof T]>[], columns: Data<T[keyof T]>[][], memo: { numBatches: number }) {
    let data: Data<T[keyof T]>;
    let field: Field<T[keyof T]>;
    let length = 0, i = -1;
    const n = columns.length;
    const bitmapLength = ((batchLength + 63) & ~63) >> 3;
    while (++i < n) {
        if ((data = children[i]) && ((length = data.length) >= batchLength)) {
            if (length === batchLength) {
                children[i] = data;
            } else {
                children[i] = data.slice(0, batchLength);
                data = data.slice(batchLength, length - batchLength);
                memo.numBatches = Math.max(memo.numBatches, columns[i].unshift(data));
            }
        } else {
            (field = fields[i]).nullable || (fields[i] = field.clone({ nullable: true }) as Field<T[keyof T]>);
            children[i] = data ? data._changeLengthAndBackfillNullBitmap(batchLength)
                : Data.new(field.type, 0, batchLength, batchLength, nullBufs(bitmapLength)) as Data<T[keyof T]>;
        }
    }
    return children;
}
