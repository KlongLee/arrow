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

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import pytest
import random
import signal
import sys

import numpy as np
import pyarrow as pa
import pandas as pd


DEFAULT_PLASMA_STORE_MEMORY = 10 ** 8
USE_VALGRIND = os.getenv("PLASMA_VALGRIND") == "1"


def random_name():
    return str(random.randint(0, 99999999))


def random_object_id():
    import pyarrow.plasma as plasma
    return plasma.ObjectID(np.random.bytes(20))


def generate_metadata(length):
    metadata = bytearray(length)
    if length > 0:
        metadata[0] = random.randint(0, 255)
        metadata[-1] = random.randint(0, 255)
        for _ in range(100):
            metadata[random.randint(0, length - 1)] = random.randint(0, 255)
    return metadata


def write_to_data_buffer(buff, length):
    array = np.frombuffer(buff, dtype="uint8")
    if length > 0:
        array[0] = random.randint(0, 255)
        array[-1] = random.randint(0, 255)
        for _ in range(100):
            array[random.randint(0, length - 1)] = random.randint(0, 255)


def create_object_with_id(client, object_id, data_size, metadata_size,
                          seal=True):
    metadata = generate_metadata(metadata_size)
    memory_buffer = client.create(object_id, data_size, metadata)
    write_to_data_buffer(memory_buffer, data_size)
    if seal:
        client.seal(object_id)
    return memory_buffer, metadata


def create_object(client, data_size, metadata_size=0, seal=True):
    object_id = random_object_id()
    memory_buffer, metadata = create_object_with_id(client, object_id,
                                                    data_size, metadata_size,
                                                    seal=seal)
    return object_id, memory_buffer, metadata


def assert_get_object_equal(unit_test, client1, client2, object_id,
                            memory_buffer=None, metadata=None):
    import pyarrow.plasma as plasma
    client1_buff = client1.get_buffers([object_id])[0]
    client2_buff = client2.get_buffers([object_id])[0]
    client1_metadata = client1.get_metadata([object_id])[0]
    client2_metadata = client2.get_metadata([object_id])[0]
    assert len(client1_buff) == len(client2_buff)
    assert len(client1_metadata) == len(client2_metadata)
    # Check that the buffers from the two clients are the same.
    assert plasma.buffers_equal(client1_buff, client2_buff)
    # Check that the metadata buffers from the two clients are the same.
    assert plasma.buffers_equal(client1_metadata, client2_metadata)
    # If a reference buffer was provided, check that it is the same as well.
    if memory_buffer is not None:
        assert plasma.buffers_equal(memory_buffer, client1_buff)
    # If reference metadata was provided, check that it is the same as well.
    if metadata is not None:
        assert plasma.buffers_equal(metadata, client1_metadata)


@pytest.mark.plasma
class TestPlasmaClient(object):

    def setup_method(self, test_method):
        use_one_memory_mapped_file = (test_method ==
                                      self.test_use_one_memory_mapped_file)

        import pyarrow.plasma as plasma
        # Start Plasma store.
        self.plasma_store_ctx = plasma.start_plasma_store(
            plasma_store_memory=DEFAULT_PLASMA_STORE_MEMORY,
            use_valgrind=USE_VALGRIND,
            use_one_memory_mapped_file=use_one_memory_mapped_file)
        plasma_store_name, self.p = self.plasma_store_ctx.__enter__()
        # Connect to Plasma.
        self.plasma_client = plasma.connect(plasma_store_name, "", 64)
        self.plasma_client2 = plasma.connect(plasma_store_name, "", 0)

    def teardown_method(self, test_method):
        try:
            # Check that the Plasma store is still alive.
            assert self.p.poll() is None
            # Ensure Valgrind and/or coverage have a clean exit
            self.p.send_signal(signal.SIGTERM)
            if sys.version_info >= (3, 3):
                self.p.wait(timeout=5)
            else:
                self.p.wait()
            assert self.p.returncode == 0
        finally:
            self.plasma_store_ctx.__exit__(None, None, None)

    def test_connection_failure_raises_exception(self):
        import pyarrow.plasma as plasma
        # ARROW-1264
        with pytest.raises(IOError):
            plasma.connect('unknown-store-name', '', 0, 1)

    def test_create(self):
        # Create an object id string.
        object_id = random_object_id()
        # Create a new buffer and write to it.
        length = 50
        memory_buffer = np.frombuffer(self.plasma_client.create(object_id,
                                                                length),
                                      dtype="uint8")
        for i in range(length):
            memory_buffer[i] = i % 256
        # Seal the object.
        self.plasma_client.seal(object_id)
        # Get the object.
        memory_buffer = np.frombuffer(
            self.plasma_client.get_buffers([object_id])[0], dtype="uint8")
        for i in range(length):
            assert memory_buffer[i] == i % 256

    def test_create_with_metadata(self):
        for length in range(0, 1000, 3):
            # Create an object id string.
            object_id = random_object_id()
            # Create a random metadata string.
            metadata = generate_metadata(length)
            # Create a new buffer and write to it.
            memory_buffer = np.frombuffer(self.plasma_client.create(object_id,
                                                                    length,
                                                                    metadata),
                                          dtype="uint8")
            for i in range(length):
                memory_buffer[i] = i % 256
            # Seal the object.
            self.plasma_client.seal(object_id)
            # Get the object.
            memory_buffer = np.frombuffer(
                self.plasma_client.get_buffers([object_id])[0], dtype="uint8")
            for i in range(length):
                assert memory_buffer[i] == i % 256
            # Get the metadata.
            metadata_buffer = np.frombuffer(
                self.plasma_client.get_metadata([object_id])[0], dtype="uint8")
            assert len(metadata) == len(metadata_buffer)
            for i in range(len(metadata)):
                assert metadata[i] == metadata_buffer[i]

    def test_create_existing(self):
        # This test is partially used to test the code path in which we create
        # an object with an ID that already exists
        length = 100
        for _ in range(1000):
            object_id = random_object_id()
            self.plasma_client.create(object_id, length,
                                      generate_metadata(length))
            try:
                self.plasma_client.create(object_id, length,
                                          generate_metadata(length))
            # TODO(pcm): Introduce a more specific error type here.
            except pa.lib.ArrowException:
                pass
            else:
                assert False

    def test_get(self):
        num_object_ids = 60
        # Test timing out of get with various timeouts.
        for timeout in [0, 10, 100, 1000]:
            object_ids = [random_object_id() for _ in range(num_object_ids)]
            results = self.plasma_client.get_buffers(object_ids,
                                                     timeout_ms=timeout)
            assert results == num_object_ids * [None]

        data_buffers = []
        metadata_buffers = []
        for i in range(num_object_ids):
            if i % 2 == 0:
                data_buffer, metadata_buffer = create_object_with_id(
                    self.plasma_client, object_ids[i], 2000, 2000)
                data_buffers.append(data_buffer)
                metadata_buffers.append(metadata_buffer)

        # Test timing out from some but not all get calls with various
        # timeouts.
        for timeout in [0, 10, 100, 1000]:
            data_results = self.plasma_client.get_buffers(object_ids,
                                                          timeout_ms=timeout)
            # metadata_results = self.plasma_client.get_metadata(
            #     object_ids, timeout_ms=timeout)
            for i in range(num_object_ids):
                if i % 2 == 0:
                    array1 = np.frombuffer(data_buffers[i // 2], dtype="uint8")
                    array2 = np.frombuffer(data_results[i], dtype="uint8")
                    np.testing.assert_equal(array1, array2)
                    # TODO(rkn): We should compare the metadata as well. But
                    # currently the types are different (e.g., memoryview
                    # versus bytearray).
                    # assert plasma.buffers_equal(
                    #     metadata_buffers[i // 2], metadata_results[i])
                else:
                    assert results[i] is None

    def test_buffer_lifetime(self):
        # ARROW-2195
        arr = pa.array([1, 12, 23, 3, 34], pa.int32())
        batch = pa.RecordBatch.from_arrays([arr], ['field1'])

        # Serialize RecordBatch into Plasma store
        sink = pa.MockOutputStream()
        writer = pa.RecordBatchStreamWriter(sink, batch.schema)
        writer.write_batch(batch)
        writer.close()

        object_id = random_object_id()
        data_buffer = self.plasma_client.create(object_id, sink.size())
        stream = pa.FixedSizeBufferWriter(data_buffer)
        writer = pa.RecordBatchStreamWriter(stream, batch.schema)
        writer.write_batch(batch)
        writer.close()
        self.plasma_client.seal(object_id)
        del data_buffer

        # Unserialize RecordBatch from Plasma store
        [data_buffer] = self.plasma_client2.get_buffers([object_id])
        reader = pa.RecordBatchStreamReader(data_buffer)
        read_batch = reader.read_next_batch()
        # Lose reference to returned buffer.  The RecordBatch must still
        # be backed by valid memory.
        del data_buffer, reader

        assert read_batch.equals(batch)

    def test_put_and_get(self):
        for value in [["hello", "world", 3, 1.0], None, "hello"]:
            object_id = self.plasma_client.put(value)
            [result] = self.plasma_client.get([object_id])
            assert result == value

            result = self.plasma_client.get(object_id)
            assert result == value

            object_id = pa.plasma.ObjectID.from_random()
            [result] = self.plasma_client.get([object_id], timeout_ms=0)
            assert result == pa.plasma.ObjectNotAvailable

    def test_put_and_get_serialization_context(self):

        class CustomType(object):
            def __init__(self, val):
                self.val = val

        val = CustomType(42)

        with pytest.raises(pa.ArrowSerializationError):
            self.plasma_client.put(val)

        serialization_context = pa.SerializationContext()
        serialization_context.register_type(CustomType, 20*b"\x00")

        object_id = self.plasma_client.put(
            val, None, serialization_context=serialization_context)

        with pytest.raises(pa.ArrowSerializationError):
            result = self.plasma_client.get(object_id)

        result = self.plasma_client.get(
            object_id, -1, serialization_context=serialization_context)
        assert result.val == val.val

    def test_store_arrow_objects(self):
        data = np.random.randn(10, 4)
        # Write an arrow object.
        object_id = random_object_id()
        tensor = pa.Tensor.from_numpy(data)
        data_size = pa.get_tensor_size(tensor)
        buf = self.plasma_client.create(object_id, data_size)
        stream = pa.FixedSizeBufferWriter(buf)
        pa.write_tensor(tensor, stream)
        self.plasma_client.seal(object_id)
        # Read the arrow object.
        [tensor] = self.plasma_client.get_buffers([object_id])
        reader = pa.BufferReader(tensor)
        array = pa.read_tensor(reader).to_numpy()
        # Assert that they are equal.
        np.testing.assert_equal(data, array)

    def test_store_pandas_dataframe(self):
        import pyarrow.plasma as plasma
        d = {'one': pd.Series([1., 2., 3.], index=['a', 'b', 'c']),
             'two': pd.Series([1., 2., 3., 4.], index=['a', 'b', 'c', 'd'])}
        df = pd.DataFrame(d)

        # Write the DataFrame.
        record_batch = pa.RecordBatch.from_pandas(df)
        # Determine the size.
        s = pa.MockOutputStream()
        stream_writer = pa.RecordBatchStreamWriter(s, record_batch.schema)
        stream_writer.write_batch(record_batch)
        data_size = s.size()
        object_id = plasma.ObjectID(np.random.bytes(20))

        buf = self.plasma_client.create(object_id, data_size)
        stream = pa.FixedSizeBufferWriter(buf)
        stream_writer = pa.RecordBatchStreamWriter(stream, record_batch.schema)
        stream_writer.write_batch(record_batch)

        self.plasma_client.seal(object_id)

        # Read the DataFrame.
        [data] = self.plasma_client.get_buffers([object_id])
        reader = pa.RecordBatchStreamReader(pa.BufferReader(data))
        result = reader.read_next_batch().to_pandas()

        pd.util.testing.assert_frame_equal(df, result)

    def test_pickle_object_ids(self):
        # This can be used for sharing object IDs between processes.
        import pickle
        object_id = random_object_id()
        data = pickle.dumps(object_id)
        object_id2 = pickle.loads(data)
        assert object_id == object_id2

    def test_store_full(self):
        # The store is started with 1GB, so make sure that create throws an
        # exception when it is full.
        def assert_create_raises_plasma_full(unit_test, size):
            partial_size = np.random.randint(size)
            try:
                _, memory_buffer, _ = create_object(unit_test.plasma_client,
                                                    partial_size,
                                                    size - partial_size)
            # TODO(pcm): More specific error here.
            except pa.lib.ArrowException:
                pass
            else:
                # For some reason the above didn't throw an exception, so fail.
                assert False

        PERCENT = DEFAULT_PLASMA_STORE_MEMORY // 100

        # Create a list to keep some of the buffers in scope.
        memory_buffers = []
        _, memory_buffer, _ = create_object(self.plasma_client, 50 * PERCENT)
        memory_buffers.append(memory_buffer)
        # Remaining space is 50%. Make sure that we can't create an
        # object of size 50% + 1, but we can create one of size 20%.
        assert_create_raises_plasma_full(self, 50 * PERCENT + 1)
        _, memory_buffer, _ = create_object(self.plasma_client, 20 * PERCENT)
        del memory_buffer
        _, memory_buffer, _ = create_object(self.plasma_client, 20 * PERCENT)
        del memory_buffer
        assert_create_raises_plasma_full(self, 50 * PERCENT + 1)

        _, memory_buffer, _ = create_object(self.plasma_client, 20 * PERCENT)
        memory_buffers.append(memory_buffer)
        # Remaining space is 30%.
        assert_create_raises_plasma_full(self, 30 * PERCENT + 1)

        _, memory_buffer, _ = create_object(self.plasma_client, 10 * PERCENT)
        memory_buffers.append(memory_buffer)
        # Remaining space is 20%.
        assert_create_raises_plasma_full(self, 20 * PERCENT + 1)

    def test_contains(self):
        fake_object_ids = [random_object_id() for _ in range(100)]
        real_object_ids = [random_object_id() for _ in range(100)]
        for object_id in real_object_ids:
            assert self.plasma_client.contains(object_id) is False
            self.plasma_client.create(object_id, 100)
            self.plasma_client.seal(object_id)
            assert self.plasma_client.contains(object_id)
        for object_id in fake_object_ids:
            assert not self.plasma_client.contains(object_id)
        for object_id in real_object_ids:
            assert self.plasma_client.contains(object_id)

    def test_hash(self):
        # Check the hash of an object that doesn't exist.
        object_id1 = random_object_id()
        try:
            self.plasma_client.hash(object_id1)
            # TODO(pcm): Introduce a more specific error type here
        except pa.lib.ArrowException:
            pass
        else:
            assert False

        length = 1000
        # Create a random object, and check that the hash function always
        # returns the same value.
        metadata = generate_metadata(length)
        memory_buffer = np.frombuffer(self.plasma_client.create(object_id1,
                                                                length,
                                                                metadata),
                                      dtype="uint8")
        for i in range(length):
            memory_buffer[i] = i % 256
        self.plasma_client.seal(object_id1)
        assert (self.plasma_client.hash(object_id1) ==
                self.plasma_client.hash(object_id1))

        # Create a second object with the same value as the first, and check
        # that their hashes are equal.
        object_id2 = random_object_id()
        memory_buffer = np.frombuffer(self.plasma_client.create(object_id2,
                                                                length,
                                                                metadata),
                                      dtype="uint8")
        for i in range(length):
            memory_buffer[i] = i % 256
        self.plasma_client.seal(object_id2)
        assert (self.plasma_client.hash(object_id1) ==
                self.plasma_client.hash(object_id2))

        # Create a third object with a different value from the first two, and
        # check that its hash is different.
        object_id3 = random_object_id()
        metadata = generate_metadata(length)
        memory_buffer = np.frombuffer(self.plasma_client.create(object_id3,
                                                                length,
                                                                metadata),
                                      dtype="uint8")
        for i in range(length):
            memory_buffer[i] = (i + 1) % 256
        self.plasma_client.seal(object_id3)
        assert (self.plasma_client.hash(object_id1) !=
                self.plasma_client.hash(object_id3))

        # Create a fourth object with the same value as the third, but
        # different metadata. Check that its hash is different from any of the
        # previous three.
        object_id4 = random_object_id()
        metadata4 = generate_metadata(length)
        memory_buffer = np.frombuffer(self.plasma_client.create(object_id4,
                                                                length,
                                                                metadata4),
                                      dtype="uint8")
        for i in range(length):
            memory_buffer[i] = (i + 1) % 256
        self.plasma_client.seal(object_id4)
        assert (self.plasma_client.hash(object_id1) !=
                self.plasma_client.hash(object_id4))
        assert (self.plasma_client.hash(object_id3) !=
                self.plasma_client.hash(object_id4))

    def test_many_hashes(self):
        hashes = []
        length = 2 ** 10

        for i in range(256):
            object_id = random_object_id()
            memory_buffer = np.frombuffer(self.plasma_client.create(object_id,
                                                                    length),
                                          dtype="uint8")
            for j in range(length):
                memory_buffer[j] = i
            self.plasma_client.seal(object_id)
            hashes.append(self.plasma_client.hash(object_id))

        # Create objects of varying length. Each pair has two bits different.
        for i in range(length):
            object_id = random_object_id()
            memory_buffer = np.frombuffer(self.plasma_client.create(object_id,
                                                                    length),
                                          dtype="uint8")
            for j in range(length):
                memory_buffer[j] = 0
            memory_buffer[i] = 1
            self.plasma_client.seal(object_id)
            hashes.append(self.plasma_client.hash(object_id))

        # Create objects of varying length, all with value 0.
        for i in range(length):
            object_id = random_object_id()
            memory_buffer = np.frombuffer(self.plasma_client.create(object_id,
                                                                    i),
                                          dtype="uint8")
            for j in range(i):
                memory_buffer[j] = 0
            self.plasma_client.seal(object_id)
            hashes.append(self.plasma_client.hash(object_id))

        # Check that all hashes were unique.
        assert len(set(hashes)) == 256 + length + length

    # def test_individual_delete(self):
    #     length = 100
    #     # Create an object id string.
    #     object_id = random_object_id()
    #     # Create a random metadata string.
    #     metadata = generate_metadata(100)
    #     # Create a new buffer and write to it.
    #     memory_buffer = self.plasma_client.create(object_id, length,
    #                                               metadata)
    #     for i in range(length):
    #         memory_buffer[i] = chr(i % 256)
    #     # Seal the object.
    #     self.plasma_client.seal(object_id)
    #     # Check that the object is present.
    #     assert self.plasma_client.contains(object_id)
    #     # Delete the object.
    #     self.plasma_client.delete(object_id)
    #     # Make sure the object is no longer present.
    #     self.assertFalse(self.plasma_client.contains(object_id))
    #
    # def test_delete(self):
    #     # Create some objects.
    #     object_ids = [random_object_id() for _ in range(100)]
    #     for object_id in object_ids:
    #         length = 100
    #         # Create a random metadata string.
    #         metadata = generate_metadata(100)
    #         # Create a new buffer and write to it.
    #         memory_buffer = self.plasma_client.create(object_id, length,
    #                                                   metadata)
    #         for i in range(length):
    #             memory_buffer[i] = chr(i % 256)
    #         # Seal the object.
    #         self.plasma_client.seal(object_id)
    #         # Check that the object is present.
    #         assert self.plasma_client.contains(object_id)
    #
    #     # Delete the objects and make sure they are no longer present.
    #     for object_id in object_ids:
    #         # Delete the object.
    #         self.plasma_client.delete(object_id)
    #         # Make sure the object is no longer present.
    #         self.assertFalse(self.plasma_client.contains(object_id))

    def test_illegal_functionality(self):
        # Create an object id string.
        object_id = random_object_id()
        # Create a new buffer and write to it.
        length = 1000
        memory_buffer = self.plasma_client.create(object_id, length)
        # Make sure we cannot access memory out of bounds.
        with pytest.raises(Exception):
            memory_buffer[length]
        # Seal the object.
        self.plasma_client.seal(object_id)
        # This test is commented out because it currently fails.
        # # Make sure the object is ready only now.
        # def illegal_assignment():
        #     memory_buffer[0] = chr(0)
        # with pytest.raises(Exception):
        # illegal_assignment()
        # Get the object.
        memory_buffer = self.plasma_client.get_buffers([object_id])[0]

        # Make sure the object is read only.
        def illegal_assignment():
            memory_buffer[0] = chr(0)
        with pytest.raises(Exception):
            illegal_assignment()

    def test_evict(self):
        client = self.plasma_client2
        object_id1 = random_object_id()
        b1 = client.create(object_id1, 1000)
        client.seal(object_id1)
        del b1
        assert client.evict(1) == 1000

        object_id2 = random_object_id()
        object_id3 = random_object_id()
        b2 = client.create(object_id2, 999)
        b3 = client.create(object_id3, 998)
        client.seal(object_id3)
        del b3
        assert client.evict(1000) == 998

        object_id4 = random_object_id()
        b4 = client.create(object_id4, 997)
        client.seal(object_id4)
        del b4
        client.seal(object_id2)
        del b2
        assert client.evict(1) == 997
        assert client.evict(1) == 999

        object_id5 = random_object_id()
        object_id6 = random_object_id()
        object_id7 = random_object_id()
        b5 = client.create(object_id5, 996)
        b6 = client.create(object_id6, 995)
        b7 = client.create(object_id7, 994)
        client.seal(object_id5)
        client.seal(object_id6)
        client.seal(object_id7)
        del b5
        del b6
        del b7
        assert client.evict(2000) == 996 + 995 + 994

    # Mitigate valgrind-induced slowness
    SUBSCRIBE_TEST_SIZES = ([1, 10, 100, 1000] if USE_VALGRIND
                            else [1, 10, 100, 1000, 10000])

    def test_subscribe(self):
        # Subscribe to notifications from the Plasma Store.
        self.plasma_client.subscribe()
        for i in self.SUBSCRIBE_TEST_SIZES:
            object_ids = [random_object_id() for _ in range(i)]
            metadata_sizes = [np.random.randint(1000) for _ in range(i)]
            data_sizes = [np.random.randint(1000) for _ in range(i)]
            for j in range(i):
                self.plasma_client.create(
                    object_ids[j], data_sizes[j],
                    metadata=bytearray(np.random.bytes(metadata_sizes[j])))
                self.plasma_client.seal(object_ids[j])
            # Check that we received notifications for all of the objects.
            for j in range(i):
                notification_info = self.plasma_client.get_next_notification()
                recv_objid, recv_dsize, recv_msize = notification_info
                assert object_ids[j] == recv_objid
                assert data_sizes[j] == recv_dsize
                assert metadata_sizes[j] == recv_msize

    def test_subscribe_deletions(self):
        # Subscribe to notifications from the Plasma Store. We use
        # plasma_client2 to make sure that all used objects will get evicted
        # properly.
        self.plasma_client2.subscribe()
        for i in self.SUBSCRIBE_TEST_SIZES:
            object_ids = [random_object_id() for _ in range(i)]
            # Add 1 to the sizes to make sure we have nonzero object sizes.
            metadata_sizes = [np.random.randint(1000) + 1 for _ in range(i)]
            data_sizes = [np.random.randint(1000) + 1 for _ in range(i)]
            for j in range(i):
                x = self.plasma_client2.create(
                        object_ids[j], data_sizes[j],
                        metadata=bytearray(np.random.bytes(metadata_sizes[j])))
                self.plasma_client2.seal(object_ids[j])
            del x
            # Check that we received notifications for creating all of the
            # objects.
            for j in range(i):
                notification_info = self.plasma_client2.get_next_notification()
                recv_objid, recv_dsize, recv_msize = notification_info
                assert object_ids[j] == recv_objid
                assert data_sizes[j] == recv_dsize
                assert metadata_sizes[j] == recv_msize

            # Check that we receive notifications for deleting all objects, as
            # we evict them.
            for j in range(i):
                assert (self.plasma_client2.evict(1) ==
                        data_sizes[j] + metadata_sizes[j])
                notification_info = self.plasma_client2.get_next_notification()
                recv_objid, recv_dsize, recv_msize = notification_info
                assert object_ids[j] == recv_objid
                assert -1 == recv_dsize
                assert -1 == recv_msize

        # Test multiple deletion notifications. The first 9 object IDs have
        # size 0, and the last has a nonzero size. When Plasma evicts 1 byte,
        # it will evict all objects, so we should receive deletion
        # notifications for each.
        num_object_ids = 10
        object_ids = [random_object_id() for _ in range(num_object_ids)]
        metadata_sizes = [0] * (num_object_ids - 1)
        data_sizes = [0] * (num_object_ids - 1)
        metadata_sizes.append(np.random.randint(1000))
        data_sizes.append(np.random.randint(1000))
        for i in range(num_object_ids):
            x = self.plasma_client2.create(
                    object_ids[i], data_sizes[i],
                    metadata=bytearray(np.random.bytes(metadata_sizes[i])))
            self.plasma_client2.seal(object_ids[i])
        del x
        for i in range(num_object_ids):
            notification_info = self.plasma_client2.get_next_notification()
            recv_objid, recv_dsize, recv_msize = notification_info
            assert object_ids[i] == recv_objid
            assert data_sizes[i] == recv_dsize
            assert metadata_sizes[i] == recv_msize
        assert (self.plasma_client2.evict(1) ==
                data_sizes[-1] + metadata_sizes[-1])
        for i in range(num_object_ids):
            notification_info = self.plasma_client2.get_next_notification()
            recv_objid, recv_dsize, recv_msize = notification_info
            assert object_ids[i] == recv_objid
            assert -1 == recv_dsize
            assert -1 == recv_msize

    def test_use_one_memory_mapped_file(self):
        # Fill the object store up with a large number of small objects and let
        # them go out of scope.
        for _ in range(100):
            create_object(
                self.plasma_client2,
                np.random.randint(1, DEFAULT_PLASMA_STORE_MEMORY // 20), 0)
        # Create large objects that require the full object store size, and
        # verify that they fit.
        for _ in range(2):
            create_object(self.plasma_client2, DEFAULT_PLASMA_STORE_MEMORY, 0)
        # Verify that an object that is too large does not fit.
        with pytest.raises(pa.lib.PlasmaStoreFull):
            create_object(self.plasma_client2, DEFAULT_PLASMA_STORE_MEMORY + 1,
                          0)


@pytest.mark.plasma
def test_object_id_size():
    import pyarrow.plasma as plasma
    with pytest.raises(ValueError):
        plasma.ObjectID("hello")
    plasma.ObjectID(20 * b"0")


@pytest.mark.plasma
def test_object_id_equality_operators():
    import pyarrow.plasma as plasma

    oid1 = plasma.ObjectID(20 * b'0')
    oid2 = plasma.ObjectID(20 * b'0')
    oid3 = plasma.ObjectID(19 * b'0' + b'1')

    assert oid1 == oid2
    assert oid2 != oid3
    assert oid1 != 'foo'


@pytest.mark.xfail(reason="often fails on travis")
@pytest.mark.skipif(not os.path.exists("/mnt/hugepages"),
                    reason="requires hugepage support")
def test_use_huge_pages():
    import pyarrow.plasma as plasma
    with plasma.start_plasma_store(
            plasma_store_memory=2*10**9,
            plasma_directory="/mnt/hugepages",
            use_hugepages=True) as (plasma_store_name, p):
        plasma_client = plasma.connect(plasma_store_name, "", 64)
        create_object(plasma_client, 10**8)


# This is checking to make sure plasma_clients cannot be destroyed
# before all the PlasmaBuffers that have handles to them are
# destroyed, see ARROW-2448.
@pytest.mark.plasma
def test_plasma_client_sharing():
    import pyarrow.plasma as plasma

    with plasma.start_plasma_store(
            plasma_store_memory=DEFAULT_PLASMA_STORE_MEMORY) \
            as (plasma_store_name, p):
        plasma_client = plasma.connect(plasma_store_name, "", 64)
        object_id = plasma_client.put(np.zeros(3))
        buf = plasma_client.get(object_id)
        del plasma_client
        assert (buf == np.zeros(3)).all()
        del buf  # This segfaulted pre ARROW-2448.


@pytest.mark.plasma
def test_plasma_list():
    import pyarrow.plasma as plasma

    with plasma.start_plasma_store(
            plasma_store_memory=DEFAULT_PLASMA_STORE_MEMORY) \
            as (plasma_store_name, p):
        plasma_client = plasma.connect(plasma_store_name, "", 0)

        # Test sizes
        x, _, _ = create_object(plasma_client, 11, metadata_size=7, seal=False)
        l1 = plasma_client.list()
        assert l1[x]["data_size"] == 11
        assert l1[x]["metadata_size"] == 7

        # Test ref_count
        y = plasma_client.put(np.zeros(3))
        l2 = plasma_client.list()
        # Ref count has already been released
        assert l2[y]["ref_count"] == 0
        a = plasma_client.get(y)
        l3 = plasma_client.list()
        assert l3[y]["ref_count"] == 1
        del a

        # Test state
        z, _, _ = create_object(plasma_client, 3, metadata_size=0, seal=False)
        l4 = plasma_client.list()
        assert l4[z]["state"] == "created"
        plasma_client.seal(z)
        l5 = plasma_client.list()
        assert l5[z]["state"] == "sealed"
