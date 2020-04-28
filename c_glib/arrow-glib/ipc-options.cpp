/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#ifdef HAVE_CONFIG_H
#  include <config.h>
#endif

#include <arrow-glib/enums.h>
#include <arrow-glib/ipc-options.h>

#include <arrow/ipc/api.h>

G_BEGIN_DECLS

/**
 * SECTION: ipc-options
 * @section_id: ipc-options-classes
 * @title: IPC options classes
 * @include: arrow-glib/arrow-glib.h
 *
 * #GArrowReadOptions provides options for reading data.
 *
 * #GArrowWriteOptions provides options for writing data.
 */

typedef struct GArrowReadOptionsPrivate_ {
  arrow::ipc::IpcReadOptions options;
} GArrowReadOptionsPrivate;

enum {
  PROP_READ_OPTIONS_MAX_RECURSION_DEPTH = 1,
  PROP_READ_OPTIONS_USE_THREADS,
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowReadOptions,
                           garrow_read_options,
                           G_TYPE_OBJECT);

#define GARROW_READ_OPTIONS_GET_PRIVATE(obj)                \
  static_cast<GArrowReadOptionsPrivate *>(                  \
    garrow_read_options_get_instance_private(               \
      GARROW_READ_OPTIONS(obj)))

static void
garrow_read_options_finalize(GObject *object)
{
  auto priv = GARROW_READ_OPTIONS_GET_PRIVATE(object);

  priv->options.~IpcReadOptions();

  G_OBJECT_CLASS(garrow_read_options_parent_class)->finalize(object);
}

static void
garrow_read_options_set_property(GObject *object,
                                 guint prop_id,
                                 const GValue *value,
                                 GParamSpec *pspec)
{
  auto priv = GARROW_READ_OPTIONS_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_READ_OPTIONS_MAX_RECURSION_DEPTH:
    priv->options.max_recursion_depth = g_value_get_int(value);
    break;
  case PROP_READ_OPTIONS_USE_THREADS:
    priv->options.use_threads = g_value_get_boolean(value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_read_options_get_property(GObject *object,
                                 guint prop_id,
                                 GValue *value,
                                 GParamSpec *pspec)
{
  auto priv = GARROW_READ_OPTIONS_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_READ_OPTIONS_MAX_RECURSION_DEPTH:
    g_value_set_int(value, priv->options.max_recursion_depth);
    break;
  case PROP_READ_OPTIONS_USE_THREADS:
    g_value_set_boolean(value, priv->options.use_threads);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_read_options_init(GArrowReadOptions *object)
{
  auto priv = GARROW_READ_OPTIONS_GET_PRIVATE(object);
  new(&priv->options) arrow::ipc::IpcReadOptions;
  priv->options = arrow::ipc::IpcReadOptions::Defaults();
}

static void
garrow_read_options_class_init(GArrowReadOptionsClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = garrow_read_options_finalize;
  gobject_class->set_property = garrow_read_options_set_property;
  gobject_class->get_property = garrow_read_options_get_property;

  auto options = arrow::ipc::IpcReadOptions::Defaults();

  GParamSpec *spec;

  /**
   * GArrowReadOptions:max-recursion-depth:
   *
   * The maximum permitted schema nesting depth.
   *
   * Since: 1.0.0
   */
  spec = g_param_spec_int("max-recursion-depth",
                          "Max recursion depth",
                          "The maximum permitted schema nesting depth",
                          0,
                          arrow::ipc::kMaxNestingDepth,
                          options.max_recursion_depth,
                          static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class,
                                  PROP_READ_OPTIONS_MAX_RECURSION_DEPTH,
                                  spec);

  /**
   * GArrowReadOptions:use-threads:
   *
   * Whether to use the global CPU thread pool.
   *
   * Since: 1.0.0
   */
  spec = g_param_spec_boolean("use-threads",
                              "Use threads",
                              "Whether to use the global CPU thread pool",
                              options.use_threads,
                              static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class,
                                  PROP_READ_OPTIONS_USE_THREADS,
                                  spec);
}

/**
 * garrow_read_options_new:
 *
 * Returns: A newly created #GArrowReadOptions.
 *
 * Since: 1.0.0
 */
GArrowReadOptions *
garrow_read_options_new(void)
{
  auto options = g_object_new(GARROW_TYPE_READ_OPTIONS, NULL);
  return GARROW_READ_OPTIONS(options);
}

/**
 * garrow_read_options_get_included_fields:
 * @options: A #GArrowReadOptions.
 * @n_fields: (out): The number of included fields.
 *
 * Returns: (array length=n_fields) (transfer full):
 *   Top-level schema fields to include when deserializing
 *   RecordBatch. If empty, return all deserialized fields.
 *
 *   It should be freed with g_free() when no longer needed.
 *
 * Since: 1.0.0
 */
int *
garrow_read_options_get_included_fields(GArrowReadOptions *options,
                                        gsize *n_fields)
{
  auto priv = GARROW_READ_OPTIONS_GET_PRIVATE(options);
  if (priv->options.included_fields.empty()) {
    if (n_fields) {
      *n_fields = 0;
    }
    return NULL;
  }

  auto n = priv->options.included_fields.size();
  auto fields = g_new(int, n);
  if (n_fields) {
    *n_fields = n;
  }
  for (size_t i = 0; i < n; ++i) {
    fields[i] = priv->options.included_fields[i];
  }
  return fields;
}

/**
 * garrow_read_options_set_included_fields:
 * @options: A #GArrowReadOptions.
 * @fields: (array length=n_fields): Top-level schema fields to
 *   include when deserializing RecordBatch. If empty, return all
 *   deserialized fields.
 * @n_fields: The number of included fields.
 *
 * Since: 1.0.0
 */
void
garrow_read_options_set_included_fields(GArrowReadOptions *options,
                                        int *fields,
                                        gsize n_fields)
{
  auto priv = GARROW_READ_OPTIONS_GET_PRIVATE(options);

  priv->options.included_fields.resize(n_fields);
  for (gsize i = 0; i < n_fields; ++i) {
    priv->options.included_fields[i] = fields[i];
  }
}


typedef struct GArrowWriteOptionsPrivate_ {
  arrow::ipc::IpcWriteOptions options;
} GArrowWriteOptionsPrivate;

enum {
  PROP_WRITE_OPTIONS_ALLOW_64BIT = 1,
  PROP_WRITE_OPTIONS_MAX_RECURSION_DEPTH,
  PROP_WRITE_OPTIONS_ALIGNMENT,
  PROP_WRITE_OPTIONS_WRITE_LEGACY_IPC_FORMAT,
  PROP_WRITE_OPTIONS_COMPRESSION,
  PROP_WRITE_OPTIONS_COMPRESSION_LEVEL,
  PROP_WRITE_OPTIONS_USE_THREADS,
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowWriteOptions,
                           garrow_write_options,
                           G_TYPE_OBJECT);

#define GARROW_WRITE_OPTIONS_GET_PRIVATE(obj)                \
  static_cast<GArrowWriteOptionsPrivate *>(                  \
    garrow_write_options_get_instance_private(               \
      GARROW_WRITE_OPTIONS(obj)))

static void
garrow_write_options_finalize(GObject *object)
{
  auto priv = GARROW_WRITE_OPTIONS_GET_PRIVATE(object);

  priv->options.~IpcWriteOptions();

  G_OBJECT_CLASS(garrow_write_options_parent_class)->finalize(object);
}

static void
garrow_write_options_set_property(GObject *object,
                                  guint prop_id,
                                  const GValue *value,
                                  GParamSpec *pspec)
{
  auto priv = GARROW_WRITE_OPTIONS_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_WRITE_OPTIONS_ALLOW_64BIT:
    priv->options.allow_64bit = g_value_get_boolean(value);
    break;
  case PROP_WRITE_OPTIONS_MAX_RECURSION_DEPTH:
    priv->options.max_recursion_depth = g_value_get_int(value);
    break;
  case PROP_WRITE_OPTIONS_ALIGNMENT:
    priv->options.alignment = g_value_get_int(value);
    break;
  case PROP_WRITE_OPTIONS_WRITE_LEGACY_IPC_FORMAT:
    priv->options.write_legacy_ipc_format = g_value_get_boolean(value);
    break;
  case PROP_WRITE_OPTIONS_COMPRESSION:
    priv->options.compression =
      static_cast<arrow::Compression::type>(g_value_get_enum(value));
    break;
  case PROP_WRITE_OPTIONS_COMPRESSION_LEVEL:
    priv->options.compression_level = g_value_get_int(value);
    break;
  case PROP_WRITE_OPTIONS_USE_THREADS:
    priv->options.use_threads = g_value_get_boolean(value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_write_options_get_property(GObject *object,
                                  guint prop_id,
                                  GValue *value,
                                  GParamSpec *pspec)
{
  auto priv = GARROW_WRITE_OPTIONS_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_WRITE_OPTIONS_ALLOW_64BIT:
    g_value_set_boolean(value, priv->options.allow_64bit);
    break;
  case PROP_WRITE_OPTIONS_MAX_RECURSION_DEPTH:
    g_value_set_int(value, priv->options.max_recursion_depth);
    break;
  case PROP_WRITE_OPTIONS_ALIGNMENT:
    g_value_set_int(value, priv->options.alignment);
    break;
  case PROP_WRITE_OPTIONS_WRITE_LEGACY_IPC_FORMAT:
    g_value_set_boolean(value, priv->options.write_legacy_ipc_format);
    break;
  case PROP_WRITE_OPTIONS_COMPRESSION:
    g_value_set_enum(value, priv->options.compression);
    break;
  case PROP_WRITE_OPTIONS_COMPRESSION_LEVEL:
    g_value_set_int(value, priv->options.compression_level);
    break;
  case PROP_WRITE_OPTIONS_USE_THREADS:
    g_value_set_boolean(value, priv->options.use_threads);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_write_options_init(GArrowWriteOptions *object)
{
  auto priv = GARROW_WRITE_OPTIONS_GET_PRIVATE(object);
  new(&priv->options) arrow::ipc::IpcWriteOptions;
  priv->options = arrow::ipc::IpcWriteOptions::Defaults();
}

static void
garrow_write_options_class_init(GArrowWriteOptionsClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = garrow_write_options_finalize;
  gobject_class->set_property = garrow_write_options_set_property;
  gobject_class->get_property = garrow_write_options_get_property;

  auto options = arrow::ipc::IpcWriteOptions::Defaults();

  GParamSpec *spec;

  /**
   * GArrowWriteOptions:allow-64bit:
   *
   * Whether to allow field lengths that don't fit in a signed 32-bit
   * int. Some implementations may not be able to parse such streams.
   *
   * Since: 1.0.0
   */
  spec = g_param_spec_boolean("allow-64bit",
                              "Allow 64bit",
                              "Whether to allow signed 64-bit int "
                              "for field length",
                              options.allow_64bit,
                              static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class,
                                  PROP_WRITE_OPTIONS_ALLOW_64BIT,
                                  spec);

  /**
   * GArrowWriteOptions:max-recursion-depth:
   *
   * The maximum permitted schema nesting depth.
   *
   * Since: 1.0.0
   */
  spec = g_param_spec_int("max-recursion-depth",
                          "Max recursion depth",
                          "The maximum permitted schema nesting depth",
                          0,
                          arrow::ipc::kMaxNestingDepth,
                          options.max_recursion_depth,
                          static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class,
                                  PROP_WRITE_OPTIONS_MAX_RECURSION_DEPTH,
                                  spec);

  /**
   * GArrowWriteOptions:alignment:
   *
   * Write padding after memory buffers to this multiple of
   * bytes. Generally 8 or 64.
   *
   * Since: 1.0.0
   */
  spec = g_param_spec_int("alignment",
                          "Alignment",
                          "Write padding "
                          "after memory buffers to this multiple of bytes",
                          0,
                          G_MAXINT,
                          options.alignment,
                          static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class,
                                  PROP_WRITE_OPTIONS_ALIGNMENT,
                                  spec);

  /**
   * GArrowWriteOptions:write-legacy-ipc-format:
   *
   * Whether to write the pre-0.15.0 encapsulated IPC message format
   * consisting of a 4-byte prefix instead of 8 byte.
   *
   * Since: 1.0.0
   */
  spec = g_param_spec_boolean("write-legacy-ipc-format",
                              "Write legacy IPC format",
                              "Whether to write legacy IPC format",
                              options.write_legacy_ipc_format,
                              static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class,
                                  PROP_WRITE_OPTIONS_WRITE_LEGACY_IPC_FORMAT,
                                  spec);

  /**
   * GArrowWriteOptions:compression:
   *
   * Codec to use for compressing and decompressing record batch body
   * buffers. This is not part of the Arrow IPC protocol and only for
   * internal use (e.g. Feather files). May only be LZ4_FRAME and
   * ZSTD.
   *
   * Since: 1.0.0
   */
  spec = g_param_spec_enum("compression",
                           "Compression",
                           "Codec to use for "
                           "compressing record batch body buffers.",
                           GARROW_TYPE_COMPRESSION_TYPE,
                           options.compression,
                           static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class,
                                  PROP_WRITE_OPTIONS_COMPRESSION,
                                  spec);

  /**
   * GArrowWriteOptions:compression-level:
   *
   * The level for compression.
   *
   * Since: 1.0.0
   */
  spec = g_param_spec_int("compression-level",
                          "Compression level",
                          "The level for compression",
                          G_MININT,
                          G_MAXINT,
                          options.compression_level,
                          static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class,
                                  PROP_WRITE_OPTIONS_COMPRESSION_LEVEL,
                                  spec);

  /**
   * GArrowWriteOptions:use-threads:
   *
   * Whether to use the global CPU thread pool.
   *
   * Since: 1.0.0
   */
  spec = g_param_spec_boolean("use-threads",
                              "Use threads",
                              "Whether to use the global CPU thread pool",
                              options.use_threads,
                              static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class,
                                  PROP_WRITE_OPTIONS_USE_THREADS,
                                  spec);
}

/**
 * garrow_write_options_new:
 *
 * Returns: A newly created #GArrowWriteOptions.
 *
 * Since: 1.0.0
 */
GArrowWriteOptions *
garrow_write_options_new(void)
{
  auto options = g_object_new(GARROW_TYPE_WRITE_OPTIONS, NULL);
  return GARROW_WRITE_OPTIONS(options);
}

G_END_DECLS
