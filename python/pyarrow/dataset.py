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

"""Dataset is currently unstable. APIs subject to change without notice."""

from __future__ import absolute_import

import sys

import pyarrow as pa
from pyarrow.util import _stringify_path, _is_path_like


if sys.version_info < (3,):
    raise ImportError("Python Dataset bindings require Python 3")

from pyarrow._dataset import (  # noqa
    AndExpression,
    CastExpression,
    CompareOperator,
    ComparisonExpression,
    Dataset,
    DefaultPartitioning,
    DirectoryPartitioning,
    Expression,
    FieldExpression,
    FileFormat,
    FileSystemSource,
    FileSystemSourceFactory,
    FileSystemFactoryOptions,
    HivePartitioning,
    InExpression,
    IsValidExpression,
    NotExpression,
    OrExpression,
    ParquetFileFormat,
    Partitioning,
    PartitioningFactory,
    ScalarExpression,
    Scanner,
    ScannerBuilder,
    ScanTask,
    Source,
    TreeSource,
)


def partitioning(field_names=None, flavor=None):
    """
    Specify a partitioning scheme.

    The supported schemes include:

    - "DirectoryPartitioning": this scheme expects one segment in the file path
      for each field in the specified schema (all fields are required to be
      present). For example given schema<year:int16, month:int8> the path
      "/2009/11" would be parsed to ("year"_ == 2009 and "month"_ == 11).
    - "HivePartitioning": a scheme for "/$key=$value/" nested directories as
      found in Apache Hive. This is a multi-level, directory based partitioning
      scheme. Data is partitioned by static values of a particular column in
      the schema. Partition keys are represented in the form $key=$value in
      directory names. Field order is ignored, as are missing or unrecognized
      field names.
      For example, given schema<year:int16, month:int8, day:int8>, a possible
      path would be "/year=2009/month=11/day=15" (but the field order does not
      need to match).

    Parameters
    ----------
    field_names : pyarrow.Schema or list of str
        The schema that describes the partitions present in the file path. If
        a list of strings (field names) is passed, the schema's types are
        inferred from the file paths (only valid for DirectoryPartitioning).
    flavor : str, default None
        The default is DirectoryPartitioning. Specify ``flavor="hive"`` for
        a HivePartitioning.

    Returns
    -------
    Partitioning or PartitioningFactory

    Examples
    --------

    Specify the Schema for paths like "/2009/June":

    >>> partitioning(pa.schema([("year", pa.int16()), ("month", pa.string())]))

    or let the types be inferred by only specifying the field names:

    >>> partitioning(["year", "month"])

    For paths like "/2009/June", the year will be inferred as int32 while month
    will be inferred as string.

    Create a Hive scheme for a path like "/year=2009/month=11":

    >>> partitioning(
    ...     pa.schema([("year", pa.int16()), ("month", pa.int8())]),
    ...     flavor="hive")

    A Hive scheme can also be discovered from the directory structure (and
    types will be inferred):

    >>> partitioning(flavor="hive")

    """
    if flavor is None:
        # default flavor
        if isinstance(field_names, pa.Schema):
            return DirectoryPartitioning(field_names)
        elif isinstance(field_names, list):
            return DirectoryPartitioning.discover(field_names)
        elif field_names is None:
            raise ValueError(
                "For the default directory flavor, need to specify "
                "'field_names' as Schema or list of field names")
        else:
            raise ValueError(
                "Expected Schema or list of field names, got {0}".format(
                    type(field_names)))
    elif flavor == 'hive':
        if isinstance(field_names, pa.Schema):
            return HivePartitioning(field_names)
        elif field_names is None:
            return HivePartitioning.discover()
        else:
            raise ValueError(
                "Expected Schema or None for 'field_names', got {0}".format(
                    type(field_names)))
    else:
        raise ValueError("Unsupported flavor")


def _ensure_fs_and_paths(path_or_paths, filesystem=None):
    # Validate and convert the path-likes and filesystem.
    # Returns filesystem and list of string paths or FileSelector
    from pyarrow.fs import FileSystem, FileType, FileSelector

    if isinstance(path_or_paths, list):
        paths_or_selector = [_stringify_path(path) for path in path_or_paths]
        if filesystem is None:
            # infer from first path
            filesystem, _ = FileSystem.from_uri(paths_or_selector[0])
    else:
        path = _stringify_path(path_or_paths)
        if filesystem is None:
            filesystem, path = FileSystem.from_uri(path)

        stats = filesystem.get_target_stats([path])[0]
        if stats.type == FileType.Directory:
            # for directory, pass a selector
            paths_or_selector = FileSelector(path, recursive=True)
        elif stats.type == FileType.File:
            # for a single file path, pass it as a list
            paths_or_selector = [path]
        else:
            raise FileNotFoundError(path)

    return filesystem, paths_or_selector


def source(path_or_paths, filesystem=None, partition_scheme=None,
           format=None):
    """
    Open a (multi-file) data source.

    Parameters
    ----------
    path_or_paths : str, pathlib.Path, or list of those
        Path to a file or to a directory containing the data files, or
        a list of paths.
    filesystem : FileSystem, default None
        By default will be inferred from the path.
    partition_scheme : Partitioning(Factory) or str
        The partitioning scheme specified with the ``partitioning()``
        function. A flavor string can be used as shortcut.
    format : str
        Currently only "parquet" is supported.

    Returns
    -------
    DataSource of DataSourceDiscovery

    """
    filesystem, paths_or_selector = _ensure_fs_and_paths(
        path_or_paths, filesystem)

    format = format or "parquet"
    if format == "parquet":
        format = ParquetFileFormat()
    elif not isinstance(format, FileFormat):
        raise ValueError("format '{0}' is not supported".format(format))

    # TODO pass through options
    options = FileSystemFactoryOptions()

    if partition_scheme is not None:
        if isinstance(partition_scheme, str):
            partition_scheme = partitioning(flavor=partition_scheme)
        if isinstance(partition_scheme, PartitioningFactory):
            options.partitioning_factory = partition_scheme
        elif isinstance(partition_scheme, Partitioning):
            options.partitioning = partition_scheme
        else:
            ValueError(
                "Expected Partitioning or PartitioningFactory, got "
                "{0}".format(type(partition_scheme)))

    discovery = FileSystemSourceFactory(
        filesystem, paths_or_selector, format, options)

    # TODO return Source if a specific schema was passed?

    # need to return SourceFactory since `dataset` might need to
    # finish the factory with a unified schema
    return discovery


def _ensure_source(src, filesystem=None, partition_scheme=None, format=None):
    if _is_path_like(src):
        src = source(src, filesystem=filesystem,
                     partition_scheme=partition_scheme, format=format)
    # TODO also accept Source?
    elif isinstance(src, FileSystemSourceFactory):
        # when passing a SourceFactory, the arguments cannot be specified
        if any(kwarg is not None
               for kwarg in [filesystem, partition_scheme, format]):
            raise ValueError(
                "When passing a Source(Factory), you cannot pass any "
                "additional arguments")
    else:
        raise ValueError("Expected a path-like or Source, got {0}".format(
            type(src)))
    return src


def dataset(sources, filesystem=None, partition_scheme=None, format=None):
    """
    Open a (multi-source) dataset.

    Parameters
    ----------
    sources : path or list of paths or sources
        Path to a file or to a directory containing the data files, or a list
        of paths for a multi-source dataset. To have more control, a list of
        sources can be passed, created with the ``source()`` function (in this
        case, the additional keywords will be ignored).
    filesystem : FileSystem, default None
        By default will be inferred from the path.
    partition_scheme : Partitioning(Factory) or str
        The partitioning scheme specified with the ``partitioning()``
        function. A flavor string can be used as shortcut.
    format : str
        Currently only "parquet" is supported.

    Returns
    -------
    Dataset

    Examples
    --------
    Opening a dataset for a single directory:

    >>> dataset("path/to/nyc-taxi/", format="parquet")

    Combining different sources:

    >>> dataset([
    ...     source("s3://old-taxi-data", format="parquet"),
    ...     source("local/path/to/new/data", format="csv")
    ... ])

    """
    if not isinstance(sources, list):
        sources = [sources]
    sources = [
        _ensure_source(src, filesystem=filesystem,
                       partition_scheme=partition_scheme, format=format)
        for src in sources
    ]

    # TEMP: for now only deal with a single source

    if len(sources) > 1:
        raise NotImplementedError("only a single source is supported for now")

    discovery = sources[0]
    inspected_schema = discovery.inspect()
    return Dataset([discovery.finish()], inspected_schema)
