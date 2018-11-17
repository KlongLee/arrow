﻿// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Collections.Generic;
using System.Linq;

namespace Apache.Arrow
{
    public partial class Schema
    {
        public IReadOnlyDictionary<string, Field> Fields { get; }
        public IReadOnlyDictionary<string, string> Metadata { get; }

        public bool HasMetadata =>
            Metadata != null && Metadata.Count > 0;

        private readonly IList<Field> _fields;

        public Schema(
            IEnumerable<Field> fields,
            IEnumerable<KeyValuePair<string, string>> metadata)
        {
            if (fields == null)
            {
                throw new ArgumentNullException(nameof(fields));
            }

            _fields = fields.ToList();

            Fields = fields.ToDictionary(
                field => field.Name, field => field,
                StringComparer.OrdinalIgnoreCase);

            Metadata = metadata?.ToDictionary(kv => kv.Key, kv => kv.Value);
        }

        public Field GetFieldByIndex(int i)
        {
            return _fields[i];
        }

        public Field GetFieldByName(string name) =>
            Fields.TryGetValue(name, out var field) ? field : null;

        public int GetFieldIndex(string name, StringComparer comparer = default)
        {
            if (comparer == null)
                comparer = StringComparer.CurrentCulture;

            return _fields.IndexOf(
                _fields.Single(x => comparer.Compare(x.Name, name) == 0));
        }
    }
}
