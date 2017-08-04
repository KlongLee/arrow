from multiprocessing import Pool
import numpy as np
import os
import pandas as pd
import pyarrow as pa
import pyarrow.plasma as plasma
import subprocess
import time

import multimerge

client = None
object_store_size = 2000000000 # 2 GB
num_cores = 8
num_rows = 200000
num_cols = 2
column_names = [str(i) for i in range(num_cols)]
column_to_sort = column_names[0]


# Connect to clients
def connect():
    global client
    client = plasma.connect('/tmp/store', '', 0)
    np.random.seed(int(time.time() * 10e7) % 10000000)


def put_df(df):
    record_batch = pa.RecordBatch.from_pandas(df)

    # Get size of record batch and schema
    mock_sink = pa.MockOutputStream()
    stream_writer = pa.RecordBatchStreamWriter(mock_sink, record_batch.schema)
    stream_writer.write_batch(record_batch)
    data_size = mock_sink.size()

    # Generate an ID and allocate a buffer in the object store for the
    # serialized DataFrame
    object_id = plasma.ObjectID(np.random.bytes(20))
    buf = client.create(object_id, data_size)

    # Write the serialized DataFrame to the object store
    sink = pa.FixedSizeBufferOutputStream(buf)
    stream_writer = pa.RecordBatchStreamWriter(sink, record_batch.schema)
    stream_writer.write_batch(record_batch)

    # Seal the object
    client.seal(object_id)

    return object_id


def get_df(object_id):
    """Retrieve a dataframe from the object store given its object ID."""
    [buf] = client.get([object_id])
    reader = pa.RecordBatchStreamReader(buf)
    record_batch = reader.read_next_batch()
    return record_batch.to_pandas()


def local_sort(object_id):
    """Sort a partition of a dataframe."""
    # Get the dataframe from the object store.
    df = get_df(object_id)
    # Sort the dataframe.
    sorted_df = df.sort_values(by=column_to_sort)
    # Get evenly spaced values from the dataframe.
    indices = np.linspace(0, len(df) - 1, num=num_cores, dtype=np.int64)
    # Put the sorted dataframe in the object store and return the corresponding
    # object ID as well as the sampled values.
    return put_df(sorted_df), sorted_df.as_matrix().take(indices)


def local_partitions(object_id_and_pivots):
    """Take a sorted partition of a dataframe and split it into more pieces."""
    object_id, pivots = object_id_and_pivots
    df = get_df(object_id)
    split_at = df[column_to_sort].searchsorted(pivots)
    split_at = [0] + list(split_at) + [len(df)]
    # Partition the sorted dataframe and put each partition into the object
    # store.
    return [put_df(df[i:j]) for i, j in zip(split_at[:-1], split_at[1:])]


def merge(object_ids):
    """Merge a number of sorted dataframes into a single sorted dataframe."""
    # We could change this to do only one IPC roundtrip to the object store if
    # we want to.
    dfs = [get_df(object_id) for object_id in object_ids]

    # In order to use our multimerge code, we have to convert the arrays from
    # the Fortran format to the C format.
    arrays = [np.ascontiguousarray(df.as_matrix()) for df in dfs]
    for a in arrays:
        assert a.dtype == np.float64
        assert not np.isfortran(a)

    # Filter out empty arrays.
    arrays = [a for a in arrays if a.shape[0] > 0]

    if len(arrays) == 0:
        return None

    resulting_array = multimerge.multimerge2d(*arrays)
    merged_df2 = pd.DataFrame(resulting_array, columns=column_names)

    return put_df(merged_df2)


if __name__ == '__main__':
    # Start the plasma store.
    p = subprocess.Popen(["plasma_store",
                          '-s', '/tmp/store',
                          '-m', str(object_store_size)])

    # Connect to the plasma store.
    connect()

    # Connect the processes in the pool.
    pool = Pool(initializer=connect, initargs=(), processes=num_cores)

    # Create a DataFrame from a numpy array.
    df = pd.DataFrame(np.random.randn(num_rows, num_cols),
                      columns=column_names)

    partition_ids = [put_df(partition) for partition
                     in np.split(df, num_cores)]

    # Begin timing the parallel sort example.
    parallel_sort_start = time.time()

    # Sort each partition and subsample them. The subsampled values will be
    # used to create buckets.
    sorted_df_ids, pivot_groups = list(zip(*pool.map(local_sort,
                                                     partition_ids)))

    # Choose the pivots.
    all_pivots = np.concatenate(pivot_groups)
    indices = np.linspace(0, len(all_pivots) - 1, num=num_cores,
                          dtype=np.int64)
    pivots = np.take(np.sort(all_pivots), indices)

    # Break all of the sorted partitions into even smaller partitions. Group
    # the object IDs from each bucket together.
    results = list(zip(*pool.map(local_partitions,
                                 zip(sorted_df_ids,
                                     len(sorted_df_ids) * [pivots]))))

    # Merge each of the buckets and store the results in the object store.
    object_ids = pool.map(merge, results)

    resulting_ids = [object_id for object_id in object_ids
                     if object_id is not None]

    # Stop timing the paralle sort example.
    parallel_sort_end = time.time()

    print('Parallel sort took {} seconds.'
          .format(parallel_sort_end - parallel_sort_start))

    serial_sort_start = time.time()

    original_sorted_df = df.sort_values(by=column_to_sort)

    serial_sort_end = time.time()

    # Check that we sorted the DataFrame properly.

    sorted_dfs = [get_df(object_id) for object_id in resulting_ids]
    sorted_df = pd.concat(sorted_dfs)

    print('Serial sort took {} seconds.'
          .format(serial_sort_end - serial_sort_start))

    assert np.allclose(sorted_df.values, original_sorted_df.values)

    # Kill the object store.
    p.kill()
