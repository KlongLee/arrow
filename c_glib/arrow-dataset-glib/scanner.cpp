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

#include <arrow/util/iterator.h>

#include <arrow-glib/error.hpp>
#include <arrow-glib/schema.hpp>

#include <arrow-dataset-glib/scanner.hpp>

G_BEGIN_DECLS

/**
 * SECTION: scanner
 * @section_id: scanner
 * @title: Scanner classes
 * @include: arrow-dataset-glib/arrow-glib.h
 *
 * #GADScanContext is a class for a scan context.
 *
 * #GADScanOptions is a class for a set of scan options.
 *
 * #GADScanTask is an abstract class for a scan task.
 *
 * #GADInMemoryScanTask is a class for a scan task of record batches.
 */

/* arrow::dataset::ScanContext */

typedef struct GADScanContextPrivate_ {
  std::shared_ptr<arrow::dataset::ScanContext> scan_context;
} GADScanContextPrivate;

enum {
  PROP_SCAN_CONTEXT = 1,
  PROP_MEMORY_POOL,
  PROP_USE_THREADS,
};

G_DEFINE_TYPE_WITH_PRIVATE(GADScanContext,
                           gad_scan_context,
                           G_TYPE_OBJECT)

#define GAD_SCAN_CONTEXT_GET_PRIVATE(obj)         \
  static_cast<GADScanContextPrivate *>(           \
     gad_scan_context_get_instance_private(       \
       GAD_SCAN_CONTEXT(obj)))

static void
gad_scan_context_finalize(GObject *object)
{
  auto priv = GAD_SCAN_CONTEXT_GET_PRIVATE(object);

  priv->scan_context.~shared_ptr();

  G_OBJECT_CLASS(gad_scan_context_parent_class)->finalize(object);
}

static void
gad_scan_context_set_property(GObject *object,
                              guint prop_id,
                              const GValue *value,
                              GParamSpec *pspec)
{
  auto priv = GAD_SCAN_CONTEXT_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_SCAN_CONTEXT:
    priv->scan_context =
      *static_cast<std::shared_ptr<arrow::dataset::ScanContext> *>(g_value_get_pointer(value));
    break;
  case PROP_USE_THREADS:
    priv->scan_context->use_threads = g_value_get_boolean(value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gad_scan_context_get_property(GObject *object,
                              guint prop_id,
                              GValue *value,
                              GParamSpec *pspec)
{
  auto priv = GAD_SCAN_CONTEXT_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_USE_THREADS:
    g_value_set_boolean(value, priv->scan_context->use_threads);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gad_scan_context_init(GADScanContext *object)
{
  auto priv = GAD_SCAN_CONTEXT_GET_PRIVATE(object);
  new(&priv->scan_context) std::shared_ptr<arrow::dataset::ScanContext>;
}

static void
gad_scan_context_class_init(GADScanContextClass *klass)
{
  GObjectClass *gobject_class;
  GParamSpec *spec;

  gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = gad_scan_context_finalize;
  gobject_class->set_property = gad_scan_context_set_property;
  gobject_class->get_property = gad_scan_context_get_property;

  auto scan_context = arrow::dataset::ScanContext();

  spec = g_param_spec_pointer("scan-context",
                              "ScanContext",
                              "The raw std::shared<arrow::dataset::ScanContext> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_SCAN_CONTEXT, spec);

  /**
   * GADScanContext:use-threads:
   *
   * Indicate if the Scanner should make use of a ThreadPool.
   *
   * Since: 1.0.0
   */
  spec = g_param_spec_boolean("use-threads",
                              "Use threads",
                              "Indicate if the Scanner should make use of a ThreadPool",
                              scan_context.use_threads,
                              static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_USE_THREADS, spec);
}

/**
 * gad_scan_context_new:
 *
 * Returns: A newly created #GADScanContext.
 */
GADScanContext *gad_scan_context_new(void)
{
  auto arrow_scan_context = std::make_shared<arrow::dataset::ScanContext>();
  return gad_scan_context_new_raw(&arrow_scan_context);
}

/* arrow::dataset::ScanOptions */

typedef struct GADScanOptionsPrivate_ {
  std::shared_ptr<arrow::dataset::ScanOptions> scan_options;
} GADScanOptionsPrivate;

enum {
  PROP_SCAN_OPTIONS = 1,
  PROP_FILTER,
  PROP_EVALUATOR,
  PROP_PROJECTOR,
  PROP_BATCH_SIZE,
};

G_DEFINE_TYPE_WITH_PRIVATE(GADScanOptions,
                           gad_scan_options,
                           G_TYPE_OBJECT)

#define GAD_SCAN_OPTIONS_GET_PRIVATE(obj)         \
  static_cast<GADScanOptionsPrivate *>(           \
     gad_scan_options_get_instance_private(       \
       GAD_SCAN_OPTIONS(obj)))

static void
gad_scan_options_finalize(GObject *object)
{
  auto priv = GAD_SCAN_OPTIONS_GET_PRIVATE(object);

  priv->scan_options.~shared_ptr();

  G_OBJECT_CLASS(gad_scan_options_parent_class)->finalize(object);
}

static void
gad_scan_options_set_property(GObject *object,
                              guint prop_id,
                              const GValue *value,
                              GParamSpec *pspec)
{
  auto priv = GAD_SCAN_OPTIONS_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_SCAN_OPTIONS:
    priv->scan_options =
      *static_cast<std::shared_ptr<arrow::dataset::ScanOptions> *>(g_value_get_pointer(value));
    break;
  case PROP_BATCH_SIZE:
    priv->scan_options->batch_size = g_value_get_int64(value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gad_scan_options_get_property(GObject *object,
                              guint prop_id,
                              GValue *value,
                              GParamSpec *pspec)
{
  auto priv = GAD_SCAN_OPTIONS_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_BATCH_SIZE:
    g_value_set_int64(value, priv->scan_options->batch_size);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gad_scan_options_init(GADScanOptions *object)
{
  auto priv = GAD_SCAN_OPTIONS_GET_PRIVATE(object);
  new(&priv->scan_options) std::shared_ptr<arrow::dataset::ScanOptions>;
}

static void
gad_scan_options_class_init(GADScanOptionsClass *klass)
{
  GObjectClass *gobject_class;
  GParamSpec *spec;

  gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = gad_scan_options_finalize;
  gobject_class->set_property = gad_scan_options_set_property;
  gobject_class->get_property = gad_scan_options_get_property;

  auto scan_options = arrow::dataset::ScanOptions::Make(arrow::schema({}));

  spec = g_param_spec_pointer("scan-options",
                              "ScanOptions",
                              "The raw std::shared<arrow::dataset::ScanOptions> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_SCAN_OPTIONS, spec);

  // TODO: PROP_FILTER
  // TODO: PROP_EVALUATOR
  // TODO: PROP_PROJECTOR

  /**
   * GADScanOptions:batch-size:
   *
   * Maximum row count for scanned batches.
   *
   * Since: 1.0.0
   */
  spec = g_param_spec_int64("batch-size",
                            "Batch size",
                            "Maximum row count for scanned batches",
                            0,
                            G_MAXINT64,
                            scan_options->batch_size,
                            static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_BATCH_SIZE, spec);
}

/**
 * gad_scan_options_new:
 * @schema: A #GArrowSchema.
 *
 * Returns: A newly created #GADScanOptions.
 *
 * Since: 1.0.0
 */
GADScanOptions *gad_scan_options_new(GArrowSchema *schema)
{
  auto arrow_schema = garrow_schema_get_raw(schema);
  auto arrow_scan_options = arrow::dataset::ScanOptions::Make(arrow_schema);
  return gad_scan_options_new_raw(&arrow_scan_options);
}

/**
 * gad_scan_options_replace_schema:
 * @scan_options: A #GADScanOptions.
 * @schema: A #GArrowSchema.
 *
 * Returns: (transfer full):
 *   A copy of the #GADScanOptions with the given #GArrowSchema.
 *
 * Since: 1.0.0
 */
GADScanOptions *gad_scan_options_replace_schema(GADScanOptions *scan_options,
                                                GArrowSchema *schema)
{
  auto priv = GAD_SCAN_OPTIONS_GET_PRIVATE(scan_options);
  auto arrow_schema = garrow_schema_get_raw(schema);
  auto arrow_scan_options_copy = priv->scan_options->ReplaceSchema(arrow_schema);
  return gad_scan_options_new_raw(&arrow_scan_options_copy);
}

/* arrow::dataset::ScanTask */

typedef struct GADScanTaskPrivate_ {
  std::shared_ptr<arrow::dataset::ScanTask> scan_task;
} GADScanTaskPrivate;

enum {
  PROP_SCAN_TASK = 1,
};

G_DEFINE_ABSTRACT_TYPE_WITH_PRIVATE(GADScanTask,
                                    gad_scan_task,
                                    G_TYPE_OBJECT)

#define GAD_SCAN_TASK_GET_PRIVATE(obj)         \
  static_cast<GADScanTaskPrivate *>(           \
     gad_scan_task_get_instance_private(       \
       GAD_SCAN_TASK(obj)))

static void
gad_scan_task_finalize(GObject *object)
{
  auto priv = GAD_SCAN_TASK_GET_PRIVATE(object);

  priv->scan_task.~shared_ptr();

  G_OBJECT_CLASS(gad_scan_task_parent_class)->finalize(object);
}

static void
gad_scan_task_set_property(GObject *object,
                           guint prop_id,
                           const GValue *value,
                           GParamSpec *pspec)
{
  auto priv = GAD_SCAN_TASK_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_SCAN_TASK:
    priv->scan_task =
      *static_cast<std::shared_ptr<arrow::dataset::ScanTask> *>(g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gad_scan_task_get_property(GObject *object,
                           guint prop_id,
                           GValue *value,
                           GParamSpec *pspec)
{
  switch (prop_id) {
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gad_scan_task_init(GADScanTask *object)
{
  auto priv = GAD_SCAN_TASK_GET_PRIVATE(object);
  new(&priv->scan_task) std::shared_ptr<arrow::dataset::ScanTask>;
}

static void
gad_scan_task_class_init(GADScanTaskClass *klass)
{
  GObjectClass *gobject_class;
  GParamSpec *spec;

  gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = gad_scan_task_finalize;
  gobject_class->set_property = gad_scan_task_set_property;
  gobject_class->get_property = gad_scan_task_get_property;

  spec = g_param_spec_pointer("scan-task",
                              "ScanTask",
                              "The raw std::shared<arrow::dataset::ScanTask> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_SCAN_TASK, spec);
}

/**
 * gad_scan_task_execute:
 * @scan_task: A #GADScanTask.
 * @error: (nullable): REturn location for a #GError or %NULL.
 *
 * Returns: (nullable): (transfer full): A newly created #GArrowRecordBatchIterator,
 *   or %NULL on error.
 *
 * Since: 1.0.0
 */
GArrowRecordBatchIterator *gad_scan_task_execute(GADScanTask *scan_task,
                                                 GError **error)
{
  auto priv = GAD_SCAN_TASK_GET_PRIVATE(scan_task);
  auto arrow_result = priv->scan_task->Execute();
  if (garrow::check(error, arrow_result, "[datasets][scan-task][execute]")) {
    auto arrow_record_batch_iteraor = std::move(*arrow_result);
    return garrow_record_batch_iterator_new_raw(&arrow_record_batch_iteraor);
  } else {
    return NULL;
  }
}

G_END_DECLS

GADScanContext *
gad_scan_context_new_raw(std::shared_ptr<arrow::dataset::ScanContext> *arrow_scan_context)
{
  auto scan_context =
    GAD_SCAN_CONTEXT(g_object_new(GAD_TYPE_SCAN_CONTEXT,
                                  "scan-context", arrow_scan_context,
                                  NULL));
  return scan_context;
}

GADScanOptions *
gad_scan_options_new_raw(std::shared_ptr<arrow::dataset::ScanOptions> *arrow_scan_options)
{
  auto scan_options =
    GAD_SCAN_OPTIONS(g_object_new(GAD_TYPE_SCAN_OPTIONS,
                                  "scan-options", arrow_scan_options,
                                  NULL));
  return scan_options;
}
