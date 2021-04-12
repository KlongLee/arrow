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

import { MapRow } from './row';
import { Field } from '../schema';
import { Vector } from '../vector';
import { BaseVector } from './base';
import { DataType, Map_, Struct, List } from '../type';

/** @ignore */
export class MapVector<K extends DataType = any, V extends DataType = any> extends BaseVector<Map_<K, V>> {
    public asList() {
        const child = this.type.children[0] as Field<Struct<{ key: K, value: V }>>;
        return Vector.new(this.data.clone(new List<Struct<{ key: K, value: V }>>(child)));
    }
    public bind(index: number): Map_<K, V>['TValue'] {
        const child = this.getChildAt<Struct<{ key: K, value: V }>>(0)!;
        const { [index]: begin, [index + 1]: end } = this.valueOffsets;
        return new MapRow(child.slice(begin, end));
    }
}
