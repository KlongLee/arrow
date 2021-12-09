// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "arrow/util/tracing_internal.h"

#include <iostream>
#include <sstream>
#include <thread>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable : 4522)
#endif
#include <google/protobuf/util/json_util.h>

#include <opentelemetry/exporters/ostream/span_exporter.h>
#include <opentelemetry/exporters/otlp/otlp_http_exporter.h>
#include <opentelemetry/exporters/otlp/otlp_recordable_utils.h>
#include <opentelemetry/sdk/trace/batch_span_processor.h>
#include <opentelemetry/sdk/trace/recordable.h>
#include <opentelemetry/sdk/trace/span_data.h>
#include <opentelemetry/sdk/trace/tracer_provider.h>
#include <opentelemetry/trace/provider.h>

#include <opentelemetry/exporters/otlp/protobuf_include_prefix.h>
#include <opentelemetry/exporters/otlp/protobuf_include_suffix.h>
#include <opentelemetry/proto/collector/trace/v1/trace_service.pb.h>
#ifdef _MSC_VER
#pragma warning(pop)
#endif

#include "arrow/util/io_util.h"
#include "arrow/util/logging.h"
#include "arrow/util/make_unique.h"

namespace arrow {
namespace internal {
namespace tracing {

namespace nostd = opentelemetry::nostd;
namespace otel = opentelemetry;

constexpr char kTracingBackendEnvVar[] = "ARROW_TRACING_BACKEND";

namespace {

namespace sdktrace = opentelemetry::sdk::trace;

// Custom JSON stdout exporter. Leverages the OTLP HTTP exporter's
// utilities to log the same format that would be sent to OTLP.
class OtlpStdoutExporter final : public sdktrace::SpanExporter {
 public:
  OtlpStdoutExporter() { protobuf_json_options_.add_whitespace = false; }

  std::unique_ptr<sdktrace::Recordable> MakeRecordable() noexcept override {
    // The header for the Recordable definition is not installed, work around that
    return exporter_.MakeRecordable();
  }
  otel::sdk::common::ExportResult Export(
      const nostd::span<std::unique_ptr<sdktrace::Recordable>>& spans) noexcept override {
    opentelemetry::proto::collector::trace::v1::ExportTraceServiceRequest request;
    otel::exporter::otlp::OtlpRecordableUtils::PopulateRequest(spans, &request);

    for (const auto& spans : request.resource_spans()) {
      std::string output;
      auto status = google::protobuf::util::MessageToJsonString(spans, &output,
                                                                protobuf_json_options_);
      if (ARROW_PREDICT_FALSE(!status.ok())) {
        return otel::sdk::common::ExportResult::kFailure;
      }
      std::cout << output << std::endl;
    }

    return otel::sdk::common::ExportResult::kSuccess;
  }
  bool Shutdown(std::chrono::microseconds timeout =
                    std::chrono::microseconds(0)) noexcept override {
    return exporter_.Shutdown(timeout);
  }

 private:
  opentelemetry::exporter::otlp::OtlpHttpExporter exporter_;
  google::protobuf::util::JsonPrintOptions protobuf_json_options_;
};

class ThreadIdSpanProcessor : public sdktrace::BatchSpanProcessor {
 public:
  using sdktrace::BatchSpanProcessor::BatchSpanProcessor;
  void OnEnd(std::unique_ptr<sdktrace::Recordable>&& span) noexcept override {
    std::stringstream thread_id;
    thread_id << std::this_thread::get_id();
    span->SetAttribute("thread_id", thread_id.str());
    sdktrace::BatchSpanProcessor::OnEnd(std::move(span));
  }
};

std::unique_ptr<sdktrace::SpanExporter> InitializeExporter() {
  auto maybe_env_var = arrow::internal::GetEnvVar(kTracingBackendEnvVar);
  if (maybe_env_var.ok()) {
    auto env_var = maybe_env_var.ValueOrDie();
    if (env_var == "ostream") {
      return arrow::internal::make_unique<otel::exporter::trace::OStreamSpanExporter>();
    } else if (env_var == "otlp_http") {
      namespace otlp = opentelemetry::exporter::otlp;
      otlp::OtlpHttpExporterOptions opts;
      return arrow::internal::make_unique<otlp::OtlpHttpExporter>(opts);
    } else if (env_var == "arrow_otlp_stdout") {
      return arrow::internal::make_unique<OtlpStdoutExporter>();
    } else if (!env_var.empty()) {
      ARROW_LOG(WARNING) << "Requested unknown backend " << kTracingBackendEnvVar << "="
                         << env_var;
    }
  }
  return nullptr;
}

nostd::shared_ptr<sdktrace::TracerProvider> InitializeSdkTracerProvider() {
  auto exporter = InitializeExporter();
  if (exporter) {
    sdktrace::BatchSpanProcessorOptions options;
    options.max_queue_size = 16384;
    options.schedule_delay_millis = std::chrono::milliseconds(500);
    options.max_export_batch_size = 16384;
    auto processor =
        arrow::internal::make_unique<ThreadIdSpanProcessor>(std::move(exporter), options);
    return std::make_shared<sdktrace::TracerProvider>(std::move(processor));
  }
  return nostd::shared_ptr<sdktrace::TracerProvider>();
}

class FlushLog {
 public:
  explicit FlushLog(nostd::shared_ptr<sdktrace::TracerProvider> provider)
      : provider_(std::move(provider)) {}
  ~FlushLog() {
    if (provider_) {
      provider_->ForceFlush(std::chrono::microseconds(1000000));
    }
  }
  nostd::shared_ptr<sdktrace::TracerProvider> provider_;
};

nostd::shared_ptr<sdktrace::TracerProvider> GetSdkTracerProvider() {
  static FlushLog flush_log = FlushLog(InitializeSdkTracerProvider());
  return flush_log.provider_;
}

nostd::shared_ptr<otel::trace::TracerProvider> InitializeTracing() {
  nostd::shared_ptr<otel::trace::TracerProvider> provider = GetSdkTracerProvider();
  if (provider) otel::trace::Provider::SetTracerProvider(provider);
  return otel::trace::Provider::GetTracerProvider();
}

otel::trace::TracerProvider* GetTracerProvider() {
  static nostd::shared_ptr<otel::trace::TracerProvider> provider = InitializeTracing();
  return provider.get();
}
}  // namespace

opentelemetry::trace::Tracer* GetTracer() {
  static nostd::shared_ptr<opentelemetry::trace::Tracer> tracer =
      GetTracerProvider()->GetTracer("arrow");
  return tracer.get();
}

}  // namespace tracing
}  // namespace internal
}  // namespace arrow
