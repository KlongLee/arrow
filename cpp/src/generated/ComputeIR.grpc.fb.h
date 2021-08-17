// Generated by the gRPC C++ plugin.
// If you make any local change, they will be lost.
// source: ComputeIR
#ifndef GRPC_ComputeIR__INCLUDED
#define GRPC_ComputeIR__INCLUDED

#include "ComputeIR_generated.h"
#include "flatbuffers/grpc.h"

#include <grpc++/impl/codegen/async_stream.h>
#include <grpc++/impl/codegen/async_unary_call.h>
#include <grpc++/impl/codegen/method_handler_impl.h>
#include <grpc++/impl/codegen/proto_utils.h>
#include <grpc++/impl/codegen/rpc_method.h>
#include <grpc++/impl/codegen/service_type.h>
#include <grpc++/impl/codegen/status.h>
#include <grpc++/impl/codegen/stub_options.h>
#include <grpc++/impl/codegen/sync_stream.h>

namespace grpc {
class CompletionQueue;
class Channel;
class ServerCompletionQueue;
class ServerContext;
}  // namespace grpc

namespace org {
namespace apache {
namespace arrow {
namespace flatbuf {
namespace computeir {

class Interactive final {
 public:
  static constexpr char const* service_full_name() {
    return "org.apache.arrow.flatbuf.computeir.Interactive";
  }
  class StubInterface {
   public:
    virtual ~StubInterface() {}
    virtual ::grpc::Status explain(::grpc::ClientContext* context, const flatbuffers::grpc::Message<Plan>& request, flatbuffers::grpc::Message<Plan>* response) = 0;
    std::unique_ptr< ::grpc::ClientAsyncResponseReaderInterface< flatbuffers::grpc::Message<Plan>>> Asyncexplain(::grpc::ClientContext* context, const flatbuffers::grpc::Message<Plan>& request, ::grpc::CompletionQueue* cq) {
      return std::unique_ptr< ::grpc::ClientAsyncResponseReaderInterface< flatbuffers::grpc::Message<Plan>>>(AsyncexplainRaw(context, request, cq));
    }
    std::unique_ptr< ::grpc::ClientAsyncResponseReaderInterface< flatbuffers::grpc::Message<Plan>>> PrepareAsyncexplain(::grpc::ClientContext* context, const flatbuffers::grpc::Message<Plan>& request, ::grpc::CompletionQueue* cq) {
      return std::unique_ptr< ::grpc::ClientAsyncResponseReaderInterface< flatbuffers::grpc::Message<Plan>>>(PrepareAsyncexplainRaw(context, request, cq));
    }
    std::unique_ptr< ::grpc::ClientReaderInterface< flatbuffers::grpc::Message<Literal>>> execute(::grpc::ClientContext* context, const flatbuffers::grpc::Message<Plan>& request) {
      return std::unique_ptr< ::grpc::ClientReaderInterface< flatbuffers::grpc::Message<Literal>>>(executeRaw(context, request));
    }
    std::unique_ptr< ::grpc::ClientAsyncReaderInterface< flatbuffers::grpc::Message<Literal>>> Asyncexecute(::grpc::ClientContext* context, const flatbuffers::grpc::Message<Plan>& request, ::grpc::CompletionQueue* cq, void* tag) {
      return std::unique_ptr< ::grpc::ClientAsyncReaderInterface< flatbuffers::grpc::Message<Literal>>>(AsyncexecuteRaw(context, request, cq, tag));
    }
    std::unique_ptr< ::grpc::ClientAsyncReaderInterface< flatbuffers::grpc::Message<Literal>>> PrepareAsyncexecute(::grpc::ClientContext* context, const flatbuffers::grpc::Message<Plan>& request, ::grpc::CompletionQueue* cq) {
      return std::unique_ptr< ::grpc::ClientAsyncReaderInterface< flatbuffers::grpc::Message<Literal>>>(PrepareAsyncexecuteRaw(context, request, cq));
    }
  private:
    virtual ::grpc::ClientAsyncResponseReaderInterface< flatbuffers::grpc::Message<Plan>>* AsyncexplainRaw(::grpc::ClientContext* context, const flatbuffers::grpc::Message<Plan>& request, ::grpc::CompletionQueue* cq) = 0;
    virtual ::grpc::ClientAsyncResponseReaderInterface< flatbuffers::grpc::Message<Plan>>* PrepareAsyncexplainRaw(::grpc::ClientContext* context, const flatbuffers::grpc::Message<Plan>& request, ::grpc::CompletionQueue* cq) = 0;
    virtual ::grpc::ClientReaderInterface< flatbuffers::grpc::Message<Literal>>* executeRaw(::grpc::ClientContext* context, const flatbuffers::grpc::Message<Plan>& request) = 0;
    virtual ::grpc::ClientAsyncReaderInterface< flatbuffers::grpc::Message<Literal>>* AsyncexecuteRaw(::grpc::ClientContext* context, const flatbuffers::grpc::Message<Plan>& request, ::grpc::CompletionQueue* cq, void* tag) = 0;
    virtual ::grpc::ClientAsyncReaderInterface< flatbuffers::grpc::Message<Literal>>* PrepareAsyncexecuteRaw(::grpc::ClientContext* context, const flatbuffers::grpc::Message<Plan>& request, ::grpc::CompletionQueue* cq) = 0;
  };
  class Stub final : public StubInterface {
   public:
    Stub(const std::shared_ptr< ::grpc::ChannelInterface>& channel);
    ::grpc::Status explain(::grpc::ClientContext* context, const flatbuffers::grpc::Message<Plan>& request, flatbuffers::grpc::Message<Plan>* response) override;
    std::unique_ptr< ::grpc::ClientAsyncResponseReader< flatbuffers::grpc::Message<Plan>>> Asyncexplain(::grpc::ClientContext* context, const flatbuffers::grpc::Message<Plan>& request, ::grpc::CompletionQueue* cq) {
      return std::unique_ptr< ::grpc::ClientAsyncResponseReader< flatbuffers::grpc::Message<Plan>>>(AsyncexplainRaw(context, request, cq));
    }
    std::unique_ptr< ::grpc::ClientAsyncResponseReader< flatbuffers::grpc::Message<Plan>>> PrepareAsyncexplain(::grpc::ClientContext* context, const flatbuffers::grpc::Message<Plan>& request, ::grpc::CompletionQueue* cq) {
      return std::unique_ptr< ::grpc::ClientAsyncResponseReader< flatbuffers::grpc::Message<Plan>>>(PrepareAsyncexplainRaw(context, request, cq));
    }
    std::unique_ptr< ::grpc::ClientReader< flatbuffers::grpc::Message<Literal>>> execute(::grpc::ClientContext* context, const flatbuffers::grpc::Message<Plan>& request) {
      return std::unique_ptr< ::grpc::ClientReader< flatbuffers::grpc::Message<Literal>>>(executeRaw(context, request));
    }
    std::unique_ptr< ::grpc::ClientAsyncReader< flatbuffers::grpc::Message<Literal>>> Asyncexecute(::grpc::ClientContext* context, const flatbuffers::grpc::Message<Plan>& request, ::grpc::CompletionQueue* cq, void* tag) {
      return std::unique_ptr< ::grpc::ClientAsyncReader< flatbuffers::grpc::Message<Literal>>>(AsyncexecuteRaw(context, request, cq, tag));
    }
    std::unique_ptr< ::grpc::ClientAsyncReader< flatbuffers::grpc::Message<Literal>>> PrepareAsyncexecute(::grpc::ClientContext* context, const flatbuffers::grpc::Message<Plan>& request, ::grpc::CompletionQueue* cq) {
      return std::unique_ptr< ::grpc::ClientAsyncReader< flatbuffers::grpc::Message<Literal>>>(PrepareAsyncexecuteRaw(context, request, cq));
    }
  
   private:
    std::shared_ptr< ::grpc::ChannelInterface> channel_;
    ::grpc::ClientAsyncResponseReader< flatbuffers::grpc::Message<Plan>>* AsyncexplainRaw(::grpc::ClientContext* context, const flatbuffers::grpc::Message<Plan>& request, ::grpc::CompletionQueue* cq) override;
    ::grpc::ClientAsyncResponseReader< flatbuffers::grpc::Message<Plan>>* PrepareAsyncexplainRaw(::grpc::ClientContext* context, const flatbuffers::grpc::Message<Plan>& request, ::grpc::CompletionQueue* cq) override;
    ::grpc::ClientReader< flatbuffers::grpc::Message<Literal>>* executeRaw(::grpc::ClientContext* context, const flatbuffers::grpc::Message<Plan>& request) override;
    ::grpc::ClientAsyncReader< flatbuffers::grpc::Message<Literal>>* AsyncexecuteRaw(::grpc::ClientContext* context, const flatbuffers::grpc::Message<Plan>& request, ::grpc::CompletionQueue* cq, void* tag) override;
    ::grpc::ClientAsyncReader< flatbuffers::grpc::Message<Literal>>* PrepareAsyncexecuteRaw(::grpc::ClientContext* context, const flatbuffers::grpc::Message<Plan>& request, ::grpc::CompletionQueue* cq) override;
    const ::grpc::internal::RpcMethod rpcmethod_explain_;
    const ::grpc::internal::RpcMethod rpcmethod_execute_;
  };
  static std::unique_ptr<Stub> NewStub(const std::shared_ptr< ::grpc::ChannelInterface>& channel, const ::grpc::StubOptions& options = ::grpc::StubOptions());
  
  class Service : public ::grpc::Service {
   public:
    Service();
    virtual ~Service();
    virtual ::grpc::Status explain(::grpc::ServerContext* context, const flatbuffers::grpc::Message<Plan>* request, flatbuffers::grpc::Message<Plan>* response);
    virtual ::grpc::Status execute(::grpc::ServerContext* context, const flatbuffers::grpc::Message<Plan>* request, ::grpc::ServerWriter< flatbuffers::grpc::Message<Literal>>* writer);
  };
  template <class BaseClass>
  class WithAsyncMethod_explain : public BaseClass {
   private:
    void BaseClassMustBeDerivedFromService(const Service *service) {}
   public:
    WithAsyncMethod_explain() {
      ::grpc::Service::MarkMethodAsync(0);
    }
    ~WithAsyncMethod_explain() override {
      BaseClassMustBeDerivedFromService(this);
    }
    // disable synchronous version of this method
    ::grpc::Status explain(::grpc::ServerContext* context, const flatbuffers::grpc::Message<Plan>* request, flatbuffers::grpc::Message<Plan>* response) final override {
      abort();
      return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "");
    }
    void Requestexplain(::grpc::ServerContext* context, flatbuffers::grpc::Message<Plan>* request, ::grpc::ServerAsyncResponseWriter< flatbuffers::grpc::Message<Plan>>* response, ::grpc::CompletionQueue* new_call_cq, ::grpc::ServerCompletionQueue* notification_cq, void *tag) {
      ::grpc::Service::RequestAsyncUnary(0, context, request, response, new_call_cq, notification_cq, tag);
    }
  };
  template <class BaseClass>
  class WithAsyncMethod_execute : public BaseClass {
   private:
    void BaseClassMustBeDerivedFromService(const Service *service) {}
   public:
    WithAsyncMethod_execute() {
      ::grpc::Service::MarkMethodAsync(1);
    }
    ~WithAsyncMethod_execute() override {
      BaseClassMustBeDerivedFromService(this);
    }
    // disable synchronous version of this method
    ::grpc::Status execute(::grpc::ServerContext* context, const flatbuffers::grpc::Message<Plan>* request, ::grpc::ServerWriter< flatbuffers::grpc::Message<Literal>>* writer) final override {
      abort();
      return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "");
    }
    void Requestexecute(::grpc::ServerContext* context, flatbuffers::grpc::Message<Plan>* request, ::grpc::ServerAsyncWriter< flatbuffers::grpc::Message<Literal>>* writer, ::grpc::CompletionQueue* new_call_cq, ::grpc::ServerCompletionQueue* notification_cq, void *tag) {
      ::grpc::Service::RequestAsyncServerStreaming(1, context, request, writer, new_call_cq, notification_cq, tag);
    }
  };
  typedef   WithAsyncMethod_explain<  WithAsyncMethod_execute<  Service   >   >   AsyncService;
  template <class BaseClass>
  class WithGenericMethod_explain : public BaseClass {
   private:
    void BaseClassMustBeDerivedFromService(const Service *service) {}
   public:
    WithGenericMethod_explain() {
      ::grpc::Service::MarkMethodGeneric(0);
    }
    ~WithGenericMethod_explain() override {
      BaseClassMustBeDerivedFromService(this);
    }
    // disable synchronous version of this method
    ::grpc::Status explain(::grpc::ServerContext* context, const flatbuffers::grpc::Message<Plan>* request, flatbuffers::grpc::Message<Plan>* response) final override {
      abort();
      return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "");
    }
  };
  template <class BaseClass>
  class WithGenericMethod_execute : public BaseClass {
   private:
    void BaseClassMustBeDerivedFromService(const Service *service) {}
   public:
    WithGenericMethod_execute() {
      ::grpc::Service::MarkMethodGeneric(1);
    }
    ~WithGenericMethod_execute() override {
      BaseClassMustBeDerivedFromService(this);
    }
    // disable synchronous version of this method
    ::grpc::Status execute(::grpc::ServerContext* context, const flatbuffers::grpc::Message<Plan>* request, ::grpc::ServerWriter< flatbuffers::grpc::Message<Literal>>* writer) final override {
      abort();
      return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "");
    }
  };
  template <class BaseClass>
  class WithStreamedUnaryMethod_explain : public BaseClass {
   private:
    void BaseClassMustBeDerivedFromService(const Service *service) {}
   public:
    WithStreamedUnaryMethod_explain() {
      ::grpc::Service::MarkMethodStreamed(0,
        new ::grpc::internal::StreamedUnaryHandler< flatbuffers::grpc::Message<Plan>, flatbuffers::grpc::Message<Plan>>(std::bind(&WithStreamedUnaryMethod_explain<BaseClass>::Streamedexplain, this, std::placeholders::_1, std::placeholders::_2)));
    }
    ~WithStreamedUnaryMethod_explain() override {
      BaseClassMustBeDerivedFromService(this);
    }
    // disable regular version of this method
    ::grpc::Status explain(::grpc::ServerContext* context, const flatbuffers::grpc::Message<Plan>* request, flatbuffers::grpc::Message<Plan>* response) final override {
      abort();
      return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "");
    }
    // replace default version of method with streamed unary
    virtual ::grpc::Status Streamedexplain(::grpc::ServerContext* context, ::grpc::ServerUnaryStreamer< flatbuffers::grpc::Message<Plan>,flatbuffers::grpc::Message<Plan>>* server_unary_streamer) = 0;
  };
  typedef   WithStreamedUnaryMethod_explain<  Service   >   StreamedUnaryService;
  template <class BaseClass>
  class WithSplitStreamingMethod_execute : public BaseClass {
   private:
    void BaseClassMustBeDerivedFromService(const Service *service) {}
   public:
    WithSplitStreamingMethod_execute() {
      ::grpc::Service::MarkMethodStreamed(1,
        new ::grpc::internal::SplitServerStreamingHandler< flatbuffers::grpc::Message<Plan>, flatbuffers::grpc::Message<Literal>>(std::bind(&WithSplitStreamingMethod_execute<BaseClass>::Streamedexecute, this, std::placeholders::_1, std::placeholders::_2)));
    }
    ~WithSplitStreamingMethod_execute() override {
      BaseClassMustBeDerivedFromService(this);
    }
    // disable regular version of this method
    ::grpc::Status execute(::grpc::ServerContext* context, const flatbuffers::grpc::Message<Plan>* request, ::grpc::ServerWriter< flatbuffers::grpc::Message<Literal>>* writer) final override {
      abort();
      return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "");
    }
    // replace default version of method with split streamed
    virtual ::grpc::Status Streamedexecute(::grpc::ServerContext* context, ::grpc::ServerSplitStreamer< flatbuffers::grpc::Message<Plan>,flatbuffers::grpc::Message<Literal>>* server_split_streamer) = 0;
  };
  typedef   WithSplitStreamingMethod_execute<  Service   >   SplitStreamedService;
  typedef   WithStreamedUnaryMethod_explain<  WithSplitStreamingMethod_execute<  Service   >   >   StreamedService;
};

}  // namespace computeir
}  // namespace flatbuf
}  // namespace arrow
}  // namespace apache
}  // namespace org


#endif  // GRPC_ComputeIR__INCLUDED
