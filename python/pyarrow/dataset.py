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

import pyarrow as pa
from pyarrow.util import _stringify_path, _is_path_like

from pyarrow.fs import (
    _normalize_path,
    FileSelector,
    FileSystem,
    FileType,
    LocalFileSystem,
    SubTreeFileSystem,
    _MockFileSystem
)

from pyarrow._dataset import (  # noqa
    AndExpression,
    CastExpression,
    CompareOperator,
    ComparisonExpression,
    Dataset,
    DatasetFactory,
    DirectoryPartitioning,
    Expression,
    FieldExpression,
    FileFormat,
    FileFragment,
    FileSystemDataset,
    FileSystemDatasetFactory,
    FileSystemFactoryOptions,
    Fragment,
    HivePartitioning,
    InExpression,
    IpcFileFormat,
    IsValidExpression,
    NotExpression,
    OrExpression,
    ParquetFileFormat,
    ParquetFileFragment,
    ParquetReadOptions,
    Partitioning,
    PartitioningFactory,
    ScalarExpression,
    Scanner,
    ScanTask,
    UnionDataset,
    UnionDatasetFactory
)


def field(name):
    """References a named column of the dataset.

    Stores only the field's name. Type and other information is known only when
    the expression is bound to a dataset having an explicit scheme.

    Parameters
    ----------
    name : string
        The name of the field the expression references to.

    Returns
    -------
    field_expr : FieldExpression
    """
    return FieldExpression(name)


def scalar(value):
    """Expression representing a scalar value.

    Parameters
    ----------
    value : bool, int, float or string
        Python value of the scalar. Note that only a subset of types are
        currently supported.

    Returns
    -------
    scalar_expr : ScalarExpression
    """
    return ScalarExpression(value)


def partitioning(schema=None, field_names=None, flavor=None):
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
    schema : pyarrow.Schema, default None
        The schema that describes the partitions present in the file path.
        If not specified, and `field_names` and/or `flavor` are specified,
        the schema will be inferred from the file path (and a
        PartitioningFactory is returned).
    field_names :  list of str, default None
        A list of strings (field names). If specified, the schema's types are
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

    >>> partitioning(field_names=["year", "month"])

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
        if schema is not None:
            if field_names is not None:
                raise ValueError(
                    "Cannot specify both 'schema' and 'field_names'")
            return DirectoryPartitioning(schema)
        elif field_names is not None:
            if isinstance(field_names, list):
                return DirectoryPartitioning.discover(field_names)
            else:
                raise ValueError(
                    "Expected list of field names, got {}".format(
                        type(field_names)))
        else:
            raise ValueError(
                "For the default directory flavor, need to specify "
                "a Schema or a list of field names")
    elif flavor == 'hive':
        if field_names is not None:
            raise ValueError("Cannot specify 'field_names' for flavor 'hive'")
        elif schema is not None:
            if isinstance(schema, pa.Schema):
                return HivePartitioning(schema)
            else:
                raise ValueError(
                    "Expected Schema for 'schema', got {}".format(
                        type(schema)))
        else:
            return HivePartitioning.discover()
    else:
        raise ValueError("Unsupported flavor")


def _ensure_partitioning(scheme):
    """
    Validate input and return a Partitioning(Factory).

    It passes None through if no partitioning scheme is defiend.
    """
    if scheme is None:
        pass
    elif isinstance(scheme, str):
        scheme = partitioning(flavor=scheme)
    elif isinstance(scheme, list):
        scheme = partitioning(field_names=scheme)
    elif isinstance(scheme, (Partitioning, PartitioningFactory)):
        pass
    else:
        ValueError("Expected Partitioning or PartitioningFactory, got {}"
                   .format(type(scheme)))
    return scheme


def _ensure_format(obj):
    if isinstance(obj, FileFormat):
        return obj
    elif obj == "parquet":
        return ParquetFileFormat()
    elif obj in {"ipc", "arrow", "feather"}:
        return IpcFileFormat()
    else:
        raise ValueError("format '{}' is not supported".format(obj))


def _ensure_multiple_sources(paths, filesystem=None):
    """
    Treat a list of paths as files belonging to a single file system

    If the file system is local then also validates that all paths
    are referencing existing *files* otherwise any non-file paths will be
    silently skipped (for example on a remote filesystem).

    Parameters
    ----------
    paths : list of path-like
        Note that URIs are not allowed.
    filesystem : FileSystem or str, optional
        If an URI is passed, then its path component will act as a prefix for
        the file paths.

    Returns
    -------
    (FileSystem, list of str)
        File system object and a list of normalized paths.

    Raises
    ------
    TypeError
        If the passed filesystem has wrong type.
    FileNotFoundError
        If the file system is local and a referenced path is not available.
    ValueError
        If the file system is local and a path references a directory or its
        type cannot be determined.
    """
    if filesystem is None:
        # fall back to local file system as the default
        filesystem = LocalFileSystem()
        paths_are_local = True
    elif isinstance(filesystem, str):
        # instantiate the file system from an uri, if the uri has a path
        # component then it will be treated as a path prefix
        filesystem, prefix = FileSystem.from_uri(filesystem)
        paths_are_local = isinstance(filesystem, LocalFileSystem)
        prefix = _normalize_path(filesystem, prefix)
        if prefix:
            filesystem = SubTreeFileSystem(prefix, filesystem)
    elif isinstance(filesystem, (LocalFileSystem, _MockFileSystem)):
        paths_are_local = True
    elif not isinstance(filesystem, FileSystem):
        raise TypeError(
            '`filesystem` argument must be a FileSystem instance or a valid '
            'file system URI'
        )
    else:
        paths_are_local = False

    # allow normalizing irregular paths such as Windows local paths
    paths = [_normalize_path(filesystem, _stringify_path(p)) for p in paths]

    # validate that all of the paths are pointing to existing *files*
    # possible improvement is to group the file_infos by type and raise for
    # multiple paths per error category
    if paths_are_local:
        for info in filesystem.get_file_info(paths):
            file_type = info.type
            if file_type == FileType.File:
                continue
            elif file_type == FileType.NotFound:
                raise FileNotFoundError(info.path)
            elif file_type == FileType.Directory:
                raise ValueError(
                    'Path {} points to a directory, but only file paths are '
                    'supported. To construct a nested or union dataset pass '
                    'a list of dataset objects instead.'.format(info.path)
                )
            else:
                raise ValueError(
                    'Path {} exists but its type is unknown (could be a '
                    'special file such as a Unix socket or character device, '
                    'or Windows NUL / CON / ...'.format(info.path)
                )

    return filesystem, paths


def _ensure_single_source(path, filesystem=None):
    """
    Treat path as either a recursively traversable directory or a single file.

    Parameters
    ----------
    path : path-like
    filesystem : FileSystem or str, optional
        If an URI is passed, then its path component will act as a prefix for
        the file paths.

   Returns
    -------
    (FileSystem, list of str or fs.Selector)
        File system object and either a single item list pointing to a file or
        an fs.Selector object pointing to a directory.

    Raises
    ------
    TypeError
        If the passed filesystem has wrong type.
    FileNotFoundError
        If the referenced file or directory doesn't exist.
    """
    path = _stringify_path(path)

    # if filesystem is not given try to automatically determine one
    # first check if the file exists as a local (relative) file path
    # if not then try to parse the path as an URI
    file_info = None
    if filesystem is None:
        filesystem = LocalFileSystem()
        try:
            file_info = filesystem.get_file_info([path])[0]
        except OSError:
            file_info = None
            exists_locally = False
        else:
            exists_locally = (file_info.type != FileType.NotFound)

        # if the file or directory doesn't exists locally, then assume that
        # the path is an URI describing the file system as well
        if not exists_locally:
            try:
                filesystem, path = FileSystem.from_uri(path)
            except ValueError as e:
                # ARROW-8213: neither an URI nor a locally existing path,
                # so assume that local path was given and propagate a nicer
                # file not found error instead of a more confusing scheme
                # parsing error
                if "empty scheme" not in str(e):
                    raise
            else:
                # unset file_info to query it again from the new filesystem
                file_info = None
    elif isinstance(filesystem, str):
        # instantiate the file system from an uri, if the uri has a path
        # component then it will be treated as a path prefix
        filesystem, prefix = FileSystem.from_uri(filesystem)
        prefix = _normalize_path(filesystem, prefix)
        if prefix:
            filesystem = SubTreeFileSystem(prefix, filesystem)
    elif not isinstance(filesystem, FileSystem):
        raise TypeError(
            '`filesystem` argument must be a FileSystem instance or a valid '
            'file system URI'
        )

    # ensure that the path is normalized before passing to dataset discovery
    path = _normalize_path(filesystem, path)

    # retrieve the file descriptor if it is available already
    if file_info is None:
        file_info = filesystem.get_file_info([path])[0]

    # depending on the path type either return with a recursive
    # directory selector or as a list containing a single file
    if file_info.type == FileType.Directory:
        paths_or_selector = FileSelector(path, recursive=True)
    elif file_info.type == FileType.File:
        paths_or_selector = [path]
    else:
        raise FileNotFoundError(path)

    return filesystem, paths_or_selector


def _filesystem_dataset(source, schema=None, filesystem=None,
                        partitioning=None, format=None,
                        partition_base_dir=None, exclude_invalid_files=None,
                        ignore_prefixes=None):
    """
    Create a FileSystemDataset which can be used to build a Dataset.

    Parameters are documented in the dataset function.

    Returns
    -------
    FileSystemDataset
    """
    format = _ensure_format(format or 'parquet')
    partitioning = _ensure_partitioning(partitioning)

    if isinstance(source, (list, tuple)):
        fs, paths_or_selector = _ensure_multiple_sources(source, filesystem)
    else:
        fs, paths_or_selector = _ensure_single_source(source, filesystem)

    options = FileSystemFactoryOptions(
        partitioning=partitioning,
        partition_base_dir=partition_base_dir,
        exclude_invalid_files=exclude_invalid_files,
        ignore_prefixes=ignore_prefixes
    )
    factory = FileSystemDatasetFactory(fs, paths_or_selector, format, options)

    return factory.finish(schema)


def _union_dataset(source, schema=None, **kwargs):
    if any(v is not None for v in kwargs.values()):
        raise ValueError(
            "When passing a list of Datasets, you cannot pass any additional "
            "arguments"
        )

    if schema is None:
        # unify the children datasets' schemas
        schema = pa.unify_schemas([ds.schema for ds in source])

    # create datasets with the requested schema
    children = [ds.replace_schema(schema) for ds in source]

    return UnionDataset(schema, children)


def dataset(source, schema=None, format=None, filesystem=None,
            partitioning=None, partition_base_dir=None,
            exclude_invalid_files=None, ignore_prefixes=None):
    """
    Open a dataset.

    Parameters
    ----------
    source : path, list of paths, dataset or list of datasets
        Path to a file or to a directory containing the data files, or a list
        of paths for a multi-directory dataset. To have more control, a list of
        factories can be passed, created with the ``factory()`` function (in
        this case, the additional keywords will be ignored).
    schema : Schema, optional
        Optionally provide the Schema for the Dataset, in which case it will
        not be inferred from the source.
    format : FileFormat or str
        Currently "parquet" and "ipc"/"arrow"/"feather" are supported. For
        Feather, only version 2 files are supported.
    filesystem : FileSystem, default None
        By default will be inferred from the path.
    partitioning : Partitioning, PartitioningFactory, str, list of str
        The partitioning scheme specified with the ``partitioning()``
        function. A flavor string can be used as shortcut, and with a list of
        field names a DirectionaryPartitioning will be inferred.

    partition_base_dir : str, optional
        For the purposes of applying the partitioning, paths will be
        stripped of the partition_base_dir. Files not matching the
        partition_base_dir prefix will be skipped for partitioning discovery.
        The ignored files will still be part of the Dataset, but will not
        have partition information.
    exclude_invalid_files : bool, optional (default True)
        If True, invalid files will be excluded (file format specific check).
        This will incur IO for each files in a serial and single threaded
        fashion. Disabling this feature will skip the IO, but unsupported
        files may be present in the Dataset (resulting in an error at scan
        time).
    ignore_prefixes : list, optional
        Files matching one of those prefixes will be ignored by the
        discovery process. This is matched to the basename of a path.
        By default this is ['.', '_'].

    Returns
    -------
    Dataset

    Examples
    --------
    Opening a dataset for a single directory:

    >>> dataset("path/to/nyc-taxi/", format="parquet")

    Construction from multiple factories:

    >>> dataset([
    ...     dataset("s3://old-taxi-data", format="parquet"),
    ...     dataset("local/path/to/new/data", format="ipc")
    ... ])

    """
    # collect the keyword arguments for later reuse
    kwargs = dict(
        schema=schema,
        filesystem=filesystem,
        partitioning=partitioning,
        format=format,
        partition_base_dir=partition_base_dir,
        exclude_invalid_files=exclude_invalid_files,
        ignore_prefixes=ignore_prefixes
    )

    # TODO(kszucs): support InMemoryDataset for a table input
    if _is_path_like(source):
        return _filesystem_dataset(source, **kwargs)
    elif isinstance(source, (tuple, list)):
        if all(_is_path_like(elem) for elem in source):
            return _filesystem_dataset(source, **kwargs)
        elif all(isinstance(elem, Dataset) for elem in source):
            return _union_dataset(source, **kwargs)
        else:
            unique_types = set(type(elem).__name__ for elem in source)
            type_names = ', '.join('{}'.format(t) for t in unique_types)
            raise TypeError(
                'Expected a list of path-like or dataset objects. The given '
                'list contains the following types: {}'.format(type_names)
            )
    else:
        raise TypeError(
            'Expected a path-like, list of path-likes or a list of Datasets '
            'instead of the given type: {}'.format(type(source).__name__)
        )
