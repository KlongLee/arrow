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

import io
import os

import pytest

import pyarrow as pa

try:
    import pyarrow.parquet as pq
    from pyarrow.tests.parquet.common import _write_table
except ImportError:
    pq = None

try:
    import pandas as pd
    import pandas.testing as tm

    from pyarrow.tests.parquet.common import alltypes_sample
except ImportError:
    pd = tm = None


@pytest.mark.pandas
def test_pass_separate_metadata():
    # ARROW-471
    df = alltypes_sample(size=10000)

    a_table = pa.Table.from_pandas(df)

    buf = io.BytesIO()
    _write_table(a_table, buf, compression='snappy', version='2.0')

    buf.seek(0)
    metadata = pq.read_metadata(buf)

    buf.seek(0)

    fileh = pq.ParquetFile(buf, metadata=metadata)

    tm.assert_frame_equal(df, fileh.read().to_pandas())


@pytest.mark.pandas
def test_read_single_row_group():
    # ARROW-471
    N, K = 10000, 4
    df = alltypes_sample(size=N)

    a_table = pa.Table.from_pandas(df)

    buf = io.BytesIO()
    _write_table(a_table, buf, row_group_size=N / K,
                 compression='snappy', version='2.0')

    buf.seek(0)

    pf = pq.ParquetFile(buf)

    assert pf.num_row_groups == K

    row_groups = [pf.read_row_group(i) for i in range(K)]
    result = pa.concat_tables(row_groups)
    tm.assert_frame_equal(df, result.to_pandas())


@pytest.mark.pandas
def test_read_single_row_group_with_column_subset():
    N, K = 10000, 4
    df = alltypes_sample(size=N)
    a_table = pa.Table.from_pandas(df)

    buf = io.BytesIO()
    _write_table(a_table, buf, row_group_size=N / K,
                 compression='snappy', version='2.0')

    buf.seek(0)
    pf = pq.ParquetFile(buf)

    cols = list(df.columns[:2])
    row_groups = [pf.read_row_group(i, columns=cols) for i in range(K)]
    result = pa.concat_tables(row_groups)
    tm.assert_frame_equal(df[cols], result.to_pandas())

    # ARROW-4267: Selection of duplicate columns still leads to these columns
    # being read uniquely.
    row_groups = [pf.read_row_group(i, columns=cols + cols) for i in range(K)]
    result = pa.concat_tables(row_groups)
    tm.assert_frame_equal(df[cols], result.to_pandas())


@pytest.mark.pandas
def test_read_multiple_row_groups():
    N, K = 10000, 4
    df = alltypes_sample(size=N)

    a_table = pa.Table.from_pandas(df)

    buf = io.BytesIO()
    _write_table(a_table, buf, row_group_size=N / K,
                 compression='snappy', version='2.0')

    buf.seek(0)

    pf = pq.ParquetFile(buf)

    assert pf.num_row_groups == K

    result = pf.read_row_groups(range(K))
    tm.assert_frame_equal(df, result.to_pandas())


@pytest.mark.pandas
def test_read_multiple_row_groups_with_column_subset():
    N, K = 10000, 4
    df = alltypes_sample(size=N)
    a_table = pa.Table.from_pandas(df)

    buf = io.BytesIO()
    _write_table(a_table, buf, row_group_size=N / K,
                 compression='snappy', version='2.0')

    buf.seek(0)
    pf = pq.ParquetFile(buf)

    cols = list(df.columns[:2])
    result = pf.read_row_groups(range(K), columns=cols)
    tm.assert_frame_equal(df[cols], result.to_pandas())

    # ARROW-4267: Selection of duplicate columns still leads to these columns
    # being read uniquely.
    result = pf.read_row_groups(range(K), columns=cols + cols)
    tm.assert_frame_equal(df[cols], result.to_pandas())


@pytest.mark.pandas
def test_scan_contents():
    N, K = 10000, 4
    df = alltypes_sample(size=N)
    a_table = pa.Table.from_pandas(df)

    buf = io.BytesIO()
    _write_table(a_table, buf, row_group_size=N / K,
                 compression='snappy', version='2.0')

    buf.seek(0)
    pf = pq.ParquetFile(buf)

    assert pf.scan_contents() == 10000
    assert pf.scan_contents(df.columns[:4]) == 10000


def test_parquet_file_pass_directory_instead_of_file(tempdir):
    # ARROW-7208
    path = tempdir / 'directory'
    os.mkdir(str(path))

    with pytest.raises(IOError, match="Expected file path"):
        pq.ParquetFile(path)


def test_read_column_invalid_index():
    table = pa.table([pa.array([4, 5]), pa.array(["foo", "bar"])],
                     names=['ints', 'strs'])
    bio = pa.BufferOutputStream()
    pq.write_table(table, bio)
    f = pq.ParquetFile(bio.getvalue())
    assert f.reader.read_column(0).to_pylist() == [4, 5]
    assert f.reader.read_column(1).to_pylist() == ["foo", "bar"]
    for index in (-1, 2):
        with pytest.raises((ValueError, IndexError)):
            f.reader.read_column(index)


@pytest.mark.pandas
@pytest.mark.parametrize('batch_size', [300, 1000, 1300])
def test_iter_batches_columns_reader(tempdir, batch_size):
    total_size = 3000
    chunk_size = 1000
    # TODO: Add categorical support
    df = alltypes_sample(size=total_size)

    filename = tempdir / 'pandas_roundtrip.parquet'
    arrow_table = pa.Table.from_pandas(df)
    _write_table(arrow_table, filename, version="2.0",
                 coerce_timestamps='ms', chunk_size=chunk_size)

    file_ = pq.ParquetFile(filename)
    for columns in [df.columns[:10], df.columns[10:]]:
        batches = file_.iter_batches(batch_size=batch_size, columns=columns)
        batch_starts = range(0, total_size+batch_size, batch_size)
        for batch, start in zip(batches, batch_starts):
            end = min(total_size, start + batch_size)
            tm.assert_frame_equal(
                batch.to_pandas(),
                df.iloc[start:end, :].loc[:, columns].reset_index(drop=True)
            )


@pytest.mark.pandas
@pytest.mark.parametrize('chunk_size', [1000])
def test_iter_batches_reader(tempdir, chunk_size):
    df = alltypes_sample(size=10000, categorical=True)

    filename = tempdir / 'pandas_roundtrip.parquet'
    arrow_table = pa.Table.from_pandas(df)
    assert arrow_table.schema.pandas_metadata is not None

    _write_table(arrow_table, filename, version="2.0",
                 coerce_timestamps='ms', chunk_size=chunk_size)

    file_ = pq.ParquetFile(filename)

    def get_all_batches(f):
        for row_group in range(f.num_row_groups):
            batches = f.iter_batches(
                batch_size=900,
                row_groups=[row_group],
            )

            for batch in batches:
                yield batch

    batches = list(get_all_batches(file_))
    batch_no = 0

    for i in range(file_.num_row_groups):
        tm.assert_frame_equal(
            batches[batch_no].to_pandas(),
            file_.read_row_groups([i]).to_pandas().head(900)
        )

        batch_no += 1

        tm.assert_frame_equal(
            batches[batch_no].to_pandas().reset_index(drop=True),
            file_.read_row_groups([i]).to_pandas().iloc[900:].reset_index(
                drop=True
            )
        )

        batch_no += 1


def test_read_encrypted_footer_key_only():
    """
    It's possible to decrypt Parquet files when only the footer key was set.
    """
    footer_key = b"foot!abcd\xff\x0012356"

    table = pa.table([pa.array([4, 5]), pa.array(["foo", "bar"])],
                     names=['ints', 'strs'])
    bio = pa.BufferOutputStream()

    # The encryption APIs are private for now, exposed only to allow testing.
    from pyarrow import _parquet
    encryption_props = _parquet.LowLevelEncryptionProperties(footer_key)

    pq.write_table(table, bio, lowlevel_encryption_properties=encryption_props)

    # Without key, decryption fails:
    with pytest.raises(IOError, match="no decryption found"):
        f = pq.ParquetFile(bio.getvalue())

    # With wrong key, decryption fails:
    decryption_props = pq.LowLevelDecryptionProperties(b"a" * 16)
    with pytest.raises(IOError, match="Failed decryption"):
        f = pq.ParquetFile(
            bio.getvalue(), low_level_decryption=decryption_props
        )

    # With key, decryption succeeds:
    decryption_props = pq.LowLevelDecryptionProperties(footer_key)
    f = pq.ParquetFile(bio.getvalue(), low_level_decryption=decryption_props)
    assert f.reader.read_column(0).to_pylist() == [4, 5]
    assert f.reader.read_column(1).to_pylist() == ["foo", "bar"]


def test_read_encrypted_column_keys():
    """
    It's possible to decrypt Parquet files with different column keys per
    column.
    """
    footer_key = b"foot!abcd\xff\x0012356"
    column_key1 = b"col1!abcd\xff\x0012356"
    column_key2 = b"col2!abcd\xff\x0012356"
    table = pa.table([pa.array([4, 5]), pa.array(["foo", "bar"])],
                     names=['ints', 'strs'])
    bio = pa.BufferOutputStream()

    # The encryption APIs are private for now, exposed only to allow testing.
    from pyarrow import _parquet
    encryption_props = _parquet.LowLevelEncryptionProperties(
        footer_key, {"ints": column_key1, "strs": column_key2})

    pq.write_table(table, bio, lowlevel_encryption_properties=encryption_props)

    # Without column key, decryption fails:
    decryption_props = pq.LowLevelDecryptionProperties(footer_key)
    f = pq.ParquetFile(
        bio.getvalue(), low_level_decryption=decryption_props
    )
    with pytest.raises(IOError, match="HiddenColumnException"):
        f.reader.read_column(0)

    # With wrong key, decryption fails:
    decryption_props = pq.LowLevelDecryptionProperties(
        footer_key, {"ints": b"a" * 16})
    f = pq.ParquetFile(
        bio.getvalue(), low_level_decryption=decryption_props
    )
    with pytest.raises(IOError, match="Failed decryption"):
        f.reader.read_column(0)

    # With correct keys, decryption succeeds:
    decryption_props = pq.LowLevelDecryptionProperties(
        footer_key, {"ints": column_key1, "strs": column_key2}
    )
    f = pq.ParquetFile(bio.getvalue(), low_level_decryption=decryption_props)
    assert f.reader.read_column(0).to_pylist() == [4, 5]
    assert f.reader.read_column(1).to_pylist() == ["foo", "bar"]


def test_read_encrypted_with_plaintext_file():
    """
    Plain text files are optionally supported when specifying decryption
    properties.
    """
    table = pa.table([pa.array([4, 5]), pa.array(["foo", "bar"])],
                     names=['ints', 'strs'])
    bio = pa.BufferOutputStream()
    pq.write_table(table, bio)

    footer_key = b"foot!abcd\xff\x0012356"

    # Without allowing plain text files, they don't work.
    decryption_props = pq.LowLevelDecryptionProperties(footer_key)
    with pytest.raises(
            IOError, match="Applying decryption properties on plaintext file"):
        f = pq.ParquetFile(
            bio.getvalue(), low_level_decryption=decryption_props)

    # When allowed, we can read it:
    decryption_props = pq.LowLevelDecryptionProperties(
        footer_key, plaintext_files_allowed=True)
    f = pq.ParquetFile(bio.getvalue(), low_level_decryption=decryption_props)
    assert f.reader.read_column(0).to_pylist() == [4, 5]


def test_read_encrypted_disable_footer_signature_verification():
    """
    It's possible to disable footer signature verification when reading
    encrypted files.
    """
    footer_key = b"foot!abcd\xff\x0012356"
    column_key1 = b"col1!abcd\xff\x0012356"
    column_key2 = b"col2!abcd\xff\x0012356"
    table = pa.table([pa.array([4, 5]), pa.array(["foo", "bar"])],
                     names=['ints', 'strs'])
    bio = pa.BufferOutputStream()

    # The encryption APIs are private for now, exposed only to allow testing.
    from pyarrow import _parquet
    encryption_props = _parquet.LowLevelEncryptionProperties(
        footer_key, {"ints": column_key1, "strs": column_key2},
        plaintext_footer=True)

    pq.write_table(table, bio, lowlevel_encryption_properties=encryption_props)

    # Footer is plaintext, Parquet complains:
    decryption_props = pq.LowLevelDecryptionProperties(
        None, {"ints": column_key1, "strs": column_key2},
        retrieve_key=lambda *args: b""
    )
    with pytest.raises(IOError, match="No footer key or key metadata"):
        pq.ParquetFile(
            bio.getvalue(), low_level_decryption=decryption_props)

    # But if we tell it it's OK for footer to be unverified, that's OK:
    decryption_props = pq.LowLevelDecryptionProperties(
        None, {"ints": column_key1, "strs": column_key2},
        disable_footer_signature_verification=True,
        retrieve_key=lambda *args: b""
    )
    pq.ParquetFile(bio.getvalue(), low_level_decryption=decryption_props)


def test_decrypt_with_key_retriever():
    """A key retriever function can be used to retrieve keys.

    We put null bytes in the key metadata to ensure that doesn't break
    anything; the key metadata can be arbitrary byte sequences.
    """
    key1 = b"foot!abcd\xff\x0012356"
    key2 = b"col1!abcd\xff\x0012356"
    table = pa.table([pa.array([4, 5]), pa.array(["foo", "bar"])],
                     names=['ints', 'strs'])
    bio = pa.BufferOutputStream()

    # The encryption APIs are private for now, exposed only to allow testing.
    from pyarrow import _parquet
    encryption_props = _parquet.LowLevelEncryptionProperties(
        key1, {"ints": key1, "strs": key2},
        column_keys_metadata={"ints": b"KEY\x001", "strs": b"KEY\x002"},
        footer_key_metadata=b"KEY\x001",
    )

    pq.write_table(table, bio, lowlevel_encryption_properties=encryption_props)

    # Use wrong key retriever to retrieve keys:
    def retrieve_key(metadata):
        return {b"KEY\x001": key2, b"KEY\x002": key1}[metadata]

    decryption_props = pq.LowLevelDecryptionProperties(
        retrieve_key=retrieve_key
    )
    with pytest.raises(IOError, match="Failed decryption"):
        pq.ParquetFile(bio.getvalue(), low_level_decryption=decryption_props)

    # Use correct key retriever to retrieve keys:
    def retrieve_key(metadata):
        return {b"KEY\x001": key1, b"KEY\x002": key2}[metadata]

    decryption_props = pq.LowLevelDecryptionProperties(
        retrieve_key=retrieve_key
    )
    f = pq.ParquetFile(bio.getvalue(), low_level_decryption=decryption_props)
    assert f.reader.read_column(0).to_pylist() == [4, 5]
    assert f.reader.read_column(1).to_pylist() == ["foo", "bar"]
