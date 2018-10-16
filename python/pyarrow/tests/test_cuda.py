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

"""
UNTESTED:
read_message
"""


import pytest
import pyarrow as pa
import numpy as np
import sysconfig

cuda = pytest.importorskip("pyarrow.cuda")

platform = sysconfig.get_platform()
# TODO: enable ppc64 when Arrow C++ supports IPC in ppc64 systems:
has_ipc_support = platform == 'linux-x86_64'  # or 'ppc64' in platform

cuda_ipc = pytest.mark.skipif(
    not has_ipc_support,
    reason='CUDA IPC not supported in platform `%s`' % (platform))

global_context = None  # for flake8


def setup_module(module):
    cuda.Context.use_numba_context(True)
    module.global_context = cuda.Context(0)


def teardown_module(module):
    del module.global_context


def test_Context():
    assert cuda.Context.get_num_devices() > 0
    assert global_context.device_number == 0

    with pytest.raises(ValueError):
        try:
            cuda.Context(cuda.Context.get_num_devices())
        except Exception as e_info:
            assert str(e_info).startswith(
                "device_number argument must be non-negative less than")
            raise


def test_manage_allocate_free_host():
    size = 1024

    buf = cuda.new_host_buffer(size)
    arr = np.frombuffer(buf, dtype=np.uint8)
    arr[size//4:3*size//4] = 1
    arr_cp = arr.copy()
    arr2 = np.frombuffer(buf, dtype=np.uint8)
    np.testing.assert_equal(arr2, arr_cp)
    assert buf.size == size


def test_context_allocate_del():
    bytes_allocated = global_context.bytes_allocated
    cudabuf = global_context.new_buffer(128)
    assert global_context.bytes_allocated == bytes_allocated + 128
    del cudabuf
    assert global_context.bytes_allocated == bytes_allocated


def make_random_buffer(size, target='host'):
    """Return a host or device buffer with random data.
    """
    if target == 'host':
        assert size >= 0
        buf = pa.allocate_buffer(size)
        assert buf.size == size
        arr = np.frombuffer(buf, dtype=np.uint8)
        assert arr.size == size
        arr[:] = np.random.randint(low=0, high=255, size=size, dtype=np.uint8)
        assert arr.sum() > 0
        arr_ = np.frombuffer(buf, dtype=np.uint8)
        np.testing.assert_equal(arr, arr_)
        return arr, buf
    elif target == 'device':
        arr, buf = make_random_buffer(size, target='host')
        dbuf = global_context.new_buffer(size)
        assert dbuf.size == size
        dbuf.copy_from_host(buf, position=0, nbytes=size)
        return arr, dbuf
    raise ValueError('invalid target value')


def test_context_device_buffer():
    # Creating device buffer from host buffer;
    size = 8
    arr, buf = make_random_buffer(size)
    cudabuf = global_context.buffer_from_data(buf)
    assert cudabuf.size == size
    arr2 = np.frombuffer(cudabuf.copy_to_host(), dtype=np.uint8)
    np.testing.assert_equal(arr, arr2)

    # CudaBuffer does not support buffer protocol
    with pytest.raises(BufferError):
        try:
            np.frombuffer(cudabuf, dtype=np.uint8)
        except Exception as e_info:
            assert str(e_info).startswith(
                "buffer protocol for device buffer not supported")
            raise

    # Creating device buffer from array:
    cudabuf = global_context.buffer_from_data(arr)
    assert cudabuf.size == size
    arr2 = np.frombuffer(cudabuf.copy_to_host(), dtype=np.uint8)
    np.testing.assert_equal(arr, arr2)

    # Creating device buffer from bytes:
    cudabuf = global_context.buffer_from_data(arr.tobytes())
    assert cudabuf.size == size
    arr2 = np.frombuffer(cudabuf.copy_to_host(), dtype=np.uint8)
    np.testing.assert_equal(arr, arr2)

    # Creating device buffer from another device buffer:
    # cudabuf2 = global_context.buffer_from_data(cudabuf) #  TODO: copy
    cudabuf2 = cudabuf.slice(0, cudabuf.size)  # view
    assert cudabuf2.size == size
    arr2 = np.frombuffer(cudabuf2.copy_to_host(), dtype=np.uint8)
    np.testing.assert_equal(arr, arr2)

    # Creating a device buffer from a slice of host buffer
    soffset = size//4
    ssize = 2*size//4
    cudabuf = global_context.buffer_from_data(buf, offset=soffset,
                                              size=ssize)
    assert cudabuf.size == ssize
    arr2 = np.frombuffer(cudabuf.copy_to_host(), dtype=np.uint8)
    np.testing.assert_equal(arr[soffset:soffset + ssize], arr2)

    cudabuf = global_context.buffer_from_data(buf.slice(offset=soffset,
                                                        length=ssize))
    assert cudabuf.size == ssize
    arr2 = np.frombuffer(cudabuf.copy_to_host(), dtype=np.uint8)
    np.testing.assert_equal(arr[soffset:soffset + ssize], arr2)

    # Creating a device buffer from a slice of an array
    cudabuf = global_context.buffer_from_data(arr, offset=soffset, size=ssize)
    assert cudabuf.size == ssize
    arr2 = np.frombuffer(cudabuf.copy_to_host(), dtype=np.uint8)
    np.testing.assert_equal(arr[soffset:soffset + ssize], arr2)

    cudabuf = global_context.buffer_from_data(arr[soffset:soffset+ssize])
    assert cudabuf.size == ssize
    arr2 = np.frombuffer(cudabuf.copy_to_host(), dtype=np.uint8)
    np.testing.assert_equal(arr[soffset:soffset + ssize], arr2)

    # Creating a device buffer from a slice of bytes
    cudabuf = global_context.buffer_from_data(arr.tobytes(),
                                              offset=soffset,
                                              size=ssize)
    assert cudabuf.size == ssize
    arr2 = np.frombuffer(cudabuf.copy_to_host(), dtype=np.uint8)
    np.testing.assert_equal(arr[soffset:soffset + ssize], arr2)

    # Creating a device buffer from size
    cudabuf = global_context.new_buffer(size)
    assert cudabuf.size == size

    # Creating device buffer from a slice of another device buffer:
    cudabuf = global_context.buffer_from_data(arr)
    cudabuf2 = cudabuf.slice(soffset, ssize)
    assert cudabuf2.size == ssize
    arr2 = np.frombuffer(cudabuf2.copy_to_host(), dtype=np.uint8)
    np.testing.assert_equal(arr[soffset:soffset+ssize], arr2)

    # Creating device buffer from HostBuffer

    buf = cuda.new_host_buffer(size)
    arr_ = np.frombuffer(buf, dtype=np.uint8)
    arr_[:] = arr
    cudabuf = global_context.buffer_from_data(buf)
    assert cudabuf.size == size
    arr2 = np.frombuffer(cudabuf.copy_to_host(), dtype=np.uint8)
    np.testing.assert_equal(arr, arr2)

    # Creating device buffer from HostBuffer slice

    cudabuf = global_context.buffer_from_data(buf, offset=soffset, size=ssize)
    assert cudabuf.size == ssize
    arr2 = np.frombuffer(cudabuf.copy_to_host(), dtype=np.uint8)
    np.testing.assert_equal(arr[soffset:soffset+ssize], arr2)

    cudabuf = global_context.buffer_from_data(
        buf.slice(offset=soffset, length=ssize))
    assert cudabuf.size == ssize
    arr2 = np.frombuffer(cudabuf.copy_to_host(), dtype=np.uint8)
    np.testing.assert_equal(arr[soffset:soffset+ssize], arr2)


def test_CudaBuffer():
    size = 8
    arr, buf = make_random_buffer(size)
    assert arr.tobytes() == buf.to_pybytes()
    cbuf = global_context.buffer_from_data(buf)
    assert cbuf.size == size
    assert arr.tobytes() == cbuf.to_pybytes()

    for i in range(size):
        assert cbuf[i] == arr[i]

    for s in [
            slice(None),
            slice(size//4, size//2),
    ]:
        assert cbuf[s].to_pybytes() == arr[s].tobytes()

    sbuf = cbuf.slice(size//4, size//2)
    assert sbuf.parent == cbuf

    with pytest.raises(TypeError):
        try:
            cuda.CudaBuffer()
        except Exception as e_info:
            assert str(e_info).startswith(
                "Do not call CudaBuffer's constructor directly")
            raise


def test_HostBuffer():
    size = 8
    arr, buf = make_random_buffer(size)
    assert arr.tobytes() == buf.to_pybytes()
    hbuf = cuda.new_host_buffer(size)
    np.frombuffer(hbuf, dtype=np.uint8)[:] = arr
    assert hbuf.size == size
    assert arr.tobytes() == hbuf.to_pybytes()
    for i in range(size):
        assert hbuf[i] == arr[i]
    for s in [
            slice(None),
            slice(size//4, size//2),
    ]:
        assert hbuf[s].to_pybytes() == arr[s].tobytes()

    sbuf = hbuf.slice(size//4, size//2)
    assert sbuf.parent == hbuf

    del hbuf

    with pytest.raises(TypeError):
        try:
            cuda.HostBuffer()
        except Exception as e_info:
            assert str(e_info).startswith(
                "Do not call HostBuffer's constructor directly")
            raise


def test_copy_from_to_host():
    size = 1024

    # Create a buffer in host containing range(size)
    buf = pa.allocate_buffer(size, resizable=True)  # in host
    assert isinstance(buf, pa.Buffer)
    assert not isinstance(buf, cuda.CudaBuffer)
    arr = np.frombuffer(buf, dtype=np.uint8)
    assert arr.size == size
    arr[:] = range(size)
    arr_ = np.frombuffer(buf, dtype=np.uint8)
    np.testing.assert_equal(arr, arr_)

    device_buffer = global_context.new_buffer(size)
    assert isinstance(device_buffer, cuda.CudaBuffer)
    assert isinstance(device_buffer, pa.Buffer)
    assert device_buffer.size == size

    device_buffer.copy_from_host(buf, position=0, nbytes=size)

    buf2 = device_buffer.copy_to_host(position=0, nbytes=size)
    arr2 = np.frombuffer(buf2, dtype=np.uint8)
    np.testing.assert_equal(arr, arr2)


def test_copy_to_host():
    size = 1024
    arr, dbuf = make_random_buffer(size, target='device')

    buf = dbuf.copy_to_host()
    np.testing.assert_equal(arr, np.frombuffer(buf, dtype=np.uint8))

    buf = dbuf.copy_to_host(position=size//4)
    np.testing.assert_equal(arr[size//4:], np.frombuffer(buf, dtype=np.uint8))

    buf = dbuf.copy_to_host(position=size//4, nbytes=size//8)
    np.testing.assert_equal(arr[size//4:size//4+size//8],
                            np.frombuffer(buf, dtype=np.uint8))

    buf = dbuf.copy_to_host(position=size//4, nbytes=0)
    assert buf.size == 0

    for (position, nbytes) in [
        (size+2, -1), (-2, -1), (size+1, 0), (-3, 0),
    ]:
        with pytest.raises(ValueError):
            try:
                dbuf.copy_to_host(position=position, nbytes=nbytes)
            except Exception as e_info:
                assert str(e_info).startswith(
                    'position argument is out-of-range')
                raise

    for (position, nbytes) in [
        (0, size+1), (size//2, size//2+1), (size, 1)
    ]:
        with pytest.raises(ValueError):
            try:
                dbuf.copy_to_host(position=position, nbytes=nbytes)
            except Exception as e_info:
                assert str(e_info).startswith(
                    'requested more to copy than available from device buffer')
                raise

    buf = pa.allocate_buffer(size//4)
    dbuf.copy_to_host(buf=buf)
    np.testing.assert_equal(arr[:size//4], np.frombuffer(buf, dtype=np.uint8))

    dbuf.copy_to_host(buf=buf, position=12)
    np.testing.assert_equal(arr[12:12+size//4],
                            np.frombuffer(buf, dtype=np.uint8))

    dbuf.copy_to_host(buf=buf, nbytes=12)
    np.testing.assert_equal(arr[:12], np.frombuffer(buf, dtype=np.uint8)[:12])

    dbuf.copy_to_host(buf=buf, nbytes=12, position=6)
    np.testing.assert_equal(arr[6:6+12],
                            np.frombuffer(buf, dtype=np.uint8)[:12])

    for (position, nbytes) in [
            (0, size+10), (10, size-5),
            (0, size//2), (size//4, size//4+1)
    ]:
        with pytest.raises(ValueError):
            try:
                dbuf.copy_to_host(buf=buf, position=position, nbytes=nbytes)
                print('dbuf.size={}, buf.size={}, position={}, nbytes={}'
                      .format(dbuf.size, buf.size, position, nbytes))
            except Exception as e_info:
                assert str(e_info).startswith(
                    'requested copy does not fit into host buffer')
                raise


def test_copy_from_host():
    size = 1024
    arr, buf = make_random_buffer(size=size, target='host')
    lst = arr.tolist()
    dbuf = global_context.new_buffer(size)

    def put(*args, **kwargs):
        dbuf.copy_from_host(buf, *args, **kwargs)
        rbuf = dbuf.copy_to_host()
        return np.frombuffer(rbuf, dtype=np.uint8).tolist()
    assert put() == lst
    assert put(position=size//4) == lst[:size//4]+lst[:-size//4]
    assert put() == lst
    assert put(position=1, nbytes=size//2) == \
        lst[:1] + lst[:size//2] + lst[-(size-size//2-1):]

    for (position, nbytes) in [
            (size+2, -1), (-2, -1), (size+1, 0), (-3, 0),
    ]:
        with pytest.raises(ValueError):
            try:
                put(position=position, nbytes=nbytes)
                print('dbuf.size={}, buf.size={}, position={}, nbytes={}'
                      .format(dbuf.size, buf.size, position, nbytes))
            except Exception as e_info:
                assert str(e_info).startswith(
                    'position argument is out-of-range')
                raise

    for (position, nbytes) in [
        (0, size+1),
    ]:
        with pytest.raises(ValueError):
            try:
                put(position=position, nbytes=nbytes)
                print('dbuf.size={}, buf.size={}, position={}, nbytes={}'
                      .format(dbuf.size, buf.size, position, nbytes))
            except Exception as e_info:
                assert str(e_info).startswith(
                    'requested more to copy than available from host buffer')
                raise

    for (position, nbytes) in [
        (size//2, size//2+1)
    ]:
        with pytest.raises(ValueError):
            try:
                put(position=position, nbytes=nbytes)
                print('dbuf.size={}, buf.size={}, position={}, nbytes={}'
                      .format(dbuf.size, buf.size, position, nbytes))
            except Exception as e_info:
                assert str(e_info).startswith(
                    'requested more to copy than available in device buffer')
                raise


def test_BufferWriter():
    def allocate(size):
        cbuf = global_context.new_buffer(size)
        writer = cuda.BufferWriter(cbuf)
        return cbuf, writer

    def test_writes(total_size, chunksize, buffer_size=0):
        cbuf, writer = allocate(total_size)
        arr, buf = make_random_buffer(size=total_size, target='host')

        if buffer_size > 0:
            writer.buffer_size = buffer_size

        position = writer.tell()
        assert position == 0
        writer.write(buf.slice(length=chunksize))
        assert writer.tell() == chunksize
        writer.seek(0)
        position = writer.tell()
        assert position == 0

        while position < total_size:
            bytes_to_write = min(chunksize, total_size - position)
            writer.write(buf.slice(offset=position, length=bytes_to_write))
            position += bytes_to_write

        writer.flush()
        assert cbuf.size == total_size
        buf2 = cbuf.copy_to_host()
        assert buf2.size == total_size
        arr2 = np.frombuffer(buf2, dtype=np.uint8)
        np.testing.assert_equal(arr, arr2)

    total_size, chunk_size = 1 << 16, 1000
    test_writes(total_size, chunk_size)
    test_writes(total_size, chunk_size, total_size // 16)

    cbuf, writer = allocate(100)
    writer.write(np.arange(100, dtype=np.uint8))
    writer.writeat(50, np.arange(25, dtype=np.uint8))
    writer.write(np.arange(25, dtype=np.uint8))
    writer.flush()

    arr = np.frombuffer(cbuf.copy_to_host(), np.uint8)
    np.testing.assert_equal(arr[:50], np.arange(50, dtype=np.uint8))
    np.testing.assert_equal(arr[50:75], np.arange(25, dtype=np.uint8))
    np.testing.assert_equal(arr[75:], np.arange(25, dtype=np.uint8))


def test_BufferWriter_edge_cases():
    # edge cases, see cuda-test.cc for more information:
    size = 1000
    cbuf = global_context.new_buffer(size)
    writer = cuda.BufferWriter(cbuf)
    arr, buf = make_random_buffer(size=size, target='host')

    assert writer.buffer_size == 0
    writer.buffer_size = 100
    assert writer.buffer_size == 100

    writer.write(buf.slice(length=0))
    assert writer.tell() == 0

    writer.write(buf.slice(length=10))
    writer.buffer_size = 200
    assert writer.buffer_size == 200
    assert writer.num_bytes_buffered == 0

    writer.write(buf.slice(offset=10, length=300))
    assert writer.num_bytes_buffered == 0

    writer.write(buf.slice(offset=310, length=200))
    assert writer.num_bytes_buffered == 0

    writer.write(buf.slice(offset=510, length=390))
    writer.write(buf.slice(offset=900, length=100))

    writer.flush()

    buf2 = cbuf.copy_to_host()
    assert buf2.size == size
    arr2 = np.frombuffer(buf2, dtype=np.uint8)
    np.testing.assert_equal(arr, arr2)


def test_BufferReader():
    size = 1000
    arr, cbuf = make_random_buffer(size=size, target='device')

    reader = cuda.BufferReader(cbuf)
    reader.seek(950)
    assert reader.tell() == 950

    data = reader.read(100)
    assert len(data) == 50
    assert reader.tell() == 1000

    reader.seek(925)
    arr2 = np.zeros(100, dtype=np.uint8)
    n = reader.readinto(arr2)
    assert n == 75
    assert reader.tell() == 1000
    np.testing.assert_equal(arr[925:], arr2[:75])

    reader.seek(0)
    assert reader.tell() == 0
    buf2 = reader.read_buffer()
    arr2 = np.frombuffer(buf2.copy_to_host(), dtype=np.uint8)
    np.testing.assert_equal(arr, arr2)


def make_recordbatch(length):
    schema = pa.schema([pa.field('f0', pa.int16()),
                        pa.field('f1', pa.int16())])
    a0 = pa.array(np.random.randint(0, 255, size=length, dtype=np.int16))
    a1 = pa.array(np.random.randint(0, 255, size=length, dtype=np.int16))
    batch = pa.RecordBatch.from_arrays([a0, a1], schema)
    return batch


def test_batch_serialize():
    batch = make_recordbatch(10)
    hbuf = batch.serialize()
    cbuf = cuda.serialize_record_batch(batch, global_context)
    # test that read_record_batch works properly:
    cuda.read_record_batch(cbuf, batch.schema)
    buf = cbuf.copy_to_host()
    assert hbuf.equals(buf)
    batch2 = pa.read_record_batch(buf, batch.schema)
    assert hbuf.equals(batch2.serialize())
    assert batch.num_columns == batch2.num_columns
    assert batch.num_rows == batch2.num_rows
    assert batch.column(0).equals(batch2.column(0))
    assert batch.equals(batch2)


def other_process_for_test_IPC(handle_buffer, expected_arr):
    other_context = pa.cuda.Context(0)
    ipc_handle = pa.cuda.IpcMemHandle.from_buffer(handle_buffer)
    ipc_buf = other_context.open_ipc_buffer(ipc_handle)
    buf = ipc_buf.copy_to_host()
    assert buf.size == expected_arr.size, repr((buf.size, expected_arr.size))
    arr = np.frombuffer(buf, dtype=expected_arr.dtype)
    np.testing.assert_equal(arr, expected_arr)


@cuda_ipc
def test_IPC():
    import multiprocessing
    ctx = multiprocessing.get_context('spawn')
    size = 1000
    arr, cbuf = make_random_buffer(size=size, target='device')
    ipc_handle = cbuf.export_for_ipc()
    handle_buffer = ipc_handle.serialize()
    p = ctx.Process(target=other_process_for_test_IPC,
                    args=(handle_buffer, arr))
    p.start()
    p.join()
    assert p.exitcode == 0
