# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import shutil
import tempfile

import numpy as np
import pandas as pd

import pyarrow as pa
try:
    import pyarrow.parquet as pq
except ImportError:
    pq = None


class ParquetManifestCreation(object):
    """Benchmark creating a parquet manifest."""

    size = 10 ** 6
    tmpdir = None

    param_names = ('num_partitions', 'num_threads')
    params = [(10, 100, 1000), (1, 8)]

    def setup(self, num_partitions, num_threads):
        if pq is None:
            raise NotImplementedError("Parquet support not enabled")

        self.tmpdir = tempfile.mkdtemp('benchmark_parquet')
        rnd = np.random.RandomState(42)
        num1 = rnd.randint(0, num_partitions, size=self.size)
        num2 = rnd.randint(0, 1000, size=self.size)
        output_df = pd.DataFrame({'num1': num1, 'num2': num2})
        output_table = pa.Table.from_pandas(output_df)
        pq.write_to_dataset(output_table, self.tmpdir, ['num1'])

    def teardown(self, num_partitions, num_threads):
        if self.tmpdir is not None:
            shutil.rmtree(self.tmpdir)

    def time_manifest_creation(self, num_partitions, num_threads):
        pq.ParquetManifest(self.tmpdir, metadata_nthreads=num_threads)


class ParquetWriteBinary(object):

    def setup(self):
        nuniques = 100000
        value_size = 50
        length = 1000000
        num_cols = 10

        unique_values = np.array([pd.util.testing.rands(value_size) for
                                  i in range(nuniques)], dtype='O')
        values = unique_values[np.random.randint(0, nuniques, size=length)]
        self.table = pa.table([pa.array(values) for i in range(num_cols)],
                              names=['f{}'.format(i) for i in range(num_cols)])
        self.table_df = self.table.to_pandas()

    def time_write_binary_table(self):
        out = pa.BufferOutputStream()
        pq.write_table(self.table, out)

    def time_write_binary_table_uncompressed(self):
        out = pa.BufferOutputStream()
        pq.write_table(self.table, out, compression='none')

    def time_write_binary_table_no_dictionary(self):
        out = pa.BufferOutputStream()
        pq.write_table(self.table, out, use_dictionary=False)

    def time_convert_pandas_and_write_binary_table(self):
        out = pa.BufferOutputStream()
        pq.write_table(pa.table(self.table_df), out)
