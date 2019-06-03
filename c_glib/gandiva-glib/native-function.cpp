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

#include <gandiva-glib/native-function.hpp>

#include <gandiva-glib/function-signature.hpp>

G_BEGIN_DECLS

/**
 * SECTION: native-function
 * @short_description: NativeFunction class
 * @title: NativeFunction class
 *
 * Since: 0.14.0
 */

typedef struct GGandivaNativeFunctionPrivate_ {
  const gandiva::NativeFunction *native_function;
} GGandivaNativeFunctionPrivate;

enum {
  PROP_NATIVE_FUNCTION = 1
};

G_DEFINE_TYPE_WITH_PRIVATE(GGandivaNativeFunction,
                           ggandiva_native_function,
                           G_TYPE_OBJECT)

#define GGANDIVA_NATIVE_FUNCTION_GET_PRIVATE(obj)      \
    static_cast<GGandivaNativeFunctionPrivate *>(      \
        ggandiva_native_function_get_instance_private( \
          GGANDIVA_NATIVE_FUNCTION(obj)))

static void
ggandiva_native_function_dispose(GObject *object)
{
  G_OBJECT_CLASS(ggandiva_native_function_parent_class)->dispose(object);
}

static void
ggandiva_native_function_finalize(GObject *object)
{
  G_OBJECT_CLASS(ggandiva_native_function_parent_class)->finalize(object);
}

static void
ggandiva_native_function_set_property(GObject *object,
                                      guint prop_id,
                                      const GValue *value,
                                      GParamSpec *pspec)
{
  auto priv = GGANDIVA_NATIVE_FUNCTION_GET_PRIVATE(object);
  switch (prop_id) {
  case PROP_NATIVE_FUNCTION:
    priv->native_function
      = static_cast<const gandiva::NativeFunction *>(g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
ggandiva_native_function_init(GGandivaNativeFunction *object)
{
}

static void
ggandiva_native_function_class_init(GGandivaNativeFunctionClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->dispose = ggandiva_native_function_dispose;
  gobject_class->finalize = ggandiva_native_function_finalize;
  gobject_class->set_property = ggandiva_native_function_set_property;

  GParamSpec *spec;
  spec = g_param_spec_pointer("native_function",
                              "NativeFunction",
                              "The raw gandiva::NativeFunction *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_NATIVE_FUNCTION, spec);
}

/**
 * ggandiva_native_function_get_function_signature:
 * @native_function: A #GGandivaNativeFunction.
 *
 * Returns: (transfer full): A #GGandivaFunctionSignature that represents
 *   the signature of the native function.
 *
 * Since: 0.14.0
 */
GGandivaFunctionSignature *
ggandiva_native_function_get_function_signature(GGandivaNativeFunction *native_function)
{
  auto priv = GGANDIVA_NATIVE_FUNCTION_GET_PRIVATE(native_function);
  auto gandiva_function_signature = priv->native_function->signature();
  return ggandiva_function_signature_new_raw(&gandiva_function_signature);
}

/**
 * ggandiva_native_function_get_string_function_signature:
 * @native_function: A #GGandivaNativeFunction.
 *
 * Returns: (transfer full):
 *   The string representation of the signature of the native function.
 *
 * Since: 0.14.0
 */
gchar *
ggandiva_native_function_get_string_function_signature(GGandivaNativeFunction *native_function)
{
  auto priv = GGANDIVA_NATIVE_FUNCTION_GET_PRIVATE(native_function);
  auto gandiva_function_signature = priv->native_function->signature();
  return g_strdup(gandiva_function_signature.ToString().c_str());
}

/**
 * ggandiva_native_function_get_result_nullable_type:
 * @native_function: A #GGandivaNativeFunction.
 *
 * Returns:
 *   A value of #GGandivaResultNullableType.
 *
 * Since: 0.14.0
 */
GGandivaResultNullableType
ggandiva_native_function_get_result_nullable_type(GGandivaNativeFunction *native_function)
{
  auto priv = GGANDIVA_NATIVE_FUNCTION_GET_PRIVATE(native_function);
  auto gandiva_result_nullable_type = priv->native_function->result_nullable_type();
  return static_cast<GGandivaResultNullableType>(gandiva_result_nullable_type);
}

/**
 * ggandiva_native_function_needs_context:
 * @native_function: A #GGandivaNativeFunction.
 *
 * Returns:
 *   %TRUE if the native function needs a context for evaluation,
 *   %FALSE otherwise.
 *
 * Since: 0.14.0
 */
gboolean
ggandiva_native_function_needs_context(GGandivaNativeFunction *native_function)
{
  auto priv = GGANDIVA_NATIVE_FUNCTION_GET_PRIVATE(native_function);
  return priv->native_function->NeedsContext();
}

/**
 * ggandiva_native_function_needs_function_holder:
 * @native_function: A #GGandivaNativeFunction.
 *
 * Returns:
 *   %TRUE if the native function needs a function holder for evaluation,
 *   %FALSE otherwise.
 *
 * Since: 0.14.0
 */
gboolean
ggandiva_native_function_needs_function_holder(GGandivaNativeFunction *native_function)
{
  auto priv = GGANDIVA_NATIVE_FUNCTION_GET_PRIVATE(native_function);
  return priv->native_function->NeedsFunctionHolder();
}

/**
 * ggandiva_native_function_can_return_errors:
 * @native_function: A #GGandivaNativeFunction.
 *
 * Returns:
 *   %TRUE if the native function has the possibility of returning errors,
 *   %FALSE otherwise.
 *
 * Since: 0.14.0
 */
gboolean
ggandiva_native_function_can_return_errors(GGandivaNativeFunction *native_function)
{
  auto priv = GGANDIVA_NATIVE_FUNCTION_GET_PRIVATE(native_function);
  return priv->native_function->CanReturnErrors();
}

G_END_DECLS

GGandivaNativeFunction *
ggandiva_native_function_new_raw(const gandiva::NativeFunction *gandiva_native_function)
{
  auto native_function
    = GGANDIVA_NATIVE_FUNCTION(g_object_new(GGANDIVA_TYPE_NATIVE_FUNCTION,
                                            "native_function",
                                            gandiva_native_function,
                                            NULL));
  return native_function;
}

const gandiva::NativeFunction *
ggandiva_native_function_get_raw(GGandivaNativeFunction *native_function)
{
  auto priv = GGANDIVA_NATIVE_FUNCTION_GET_PRIVATE(native_function);
  return priv->native_function;
}
