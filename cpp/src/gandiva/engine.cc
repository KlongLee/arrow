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

#include "gandiva/engine.h"

#include <iostream>
#include <sstream>
#include <string>
#include <unordered_set>
#include <utility>

#include <llvm/Analysis/Passes.h>
#include <llvm/Analysis/TargetTransformInfo.h>
#include <llvm/Bitcode/BitcodeReader.h>
#include <llvm/ExecutionEngine/MCJIT.h>
#include <llvm/IR/DataLayout.h>
#include <llvm/IR/LegacyPassManager.h>
#include <llvm/IR/Verifier.h>
#include <llvm/Linker/Linker.h>
#include <llvm/Support/DynamicLibrary.h>
#include <llvm/Support/TargetSelect.h>
#include <llvm/Support/raw_ostream.h>
#include <llvm/Transforms/IPO.h>
#include <llvm/Transforms/IPO/PassManagerBuilder.h>
#include <llvm/Transforms/Scalar.h>
#include <llvm/Transforms/Scalar/GVN.h>
#include <llvm/Transforms/Vectorize.h>
#include "gandiva/gdv_function_stubs.h"

namespace gandiva {

std::once_flag init_once_flag;

bool Engine::init_once_done_ = false;
std::set<std::string> Engine::loaded_libs_ = {};
std::mutex Engine::mtx_;

// One-time initializations.
void Engine::InitOnce() {
  DCHECK_EQ(init_once_done_, false);

  llvm::InitializeNativeTarget();
  llvm::InitializeNativeTargetAsmPrinter();
  llvm::InitializeNativeTargetAsmParser();
  llvm::InitializeNativeTargetDisassembler();

  llvm::sys::DynamicLibrary::LoadLibraryPermanently(nullptr);

  init_once_done_ = true;
}

/// factory method to construct the engine.
Status Engine::Make(std::shared_ptr<Configuration> config,
                    std::unique_ptr<Engine>* engine) {
  std::unique_ptr<Engine> engine_obj(new Engine());

  std::call_once(init_once_flag, [&engine_obj] { engine_obj->InitOnce(); });
  engine_obj->context_.reset(new llvm::LLVMContext());
  engine_obj->ir_builder_.reset(new llvm::IRBuilder<>(*(engine_obj->context())));
  engine_obj->types_.reset(new LLVMTypes(*(engine_obj->context())));

  // Create the execution engine
  std::unique_ptr<llvm::Module> cg_module(
      new llvm::Module("codegen", *(engine_obj->context())));
  engine_obj->module_ = cg_module.get();

  llvm::EngineBuilder engineBuilder(std::move(cg_module));
  engineBuilder.setEngineKind(llvm::EngineKind::JIT);
  engineBuilder.setOptLevel(llvm::CodeGenOpt::Aggressive);
  engineBuilder.setErrorStr(&(engine_obj->llvm_error_));
  engine_obj->execution_engine_.reset(engineBuilder.create());
  if (engine_obj->execution_engine_ == NULL) {
    engine_obj->module_ = NULL;
    return Status::CodeGenError(engine_obj->llvm_error_);
  }

  // Add mappings for functions that can be accessed from LLVM/IR module.
  engine_obj->AddGlobalMappings();

  auto status = engine_obj->LoadPreCompiledIRFiles(config->byte_code_file_path());
  GANDIVA_RETURN_NOT_OK(status);

  *engine = std::move(engine_obj);
  return Status::OK();
}

// Handling for pre-compiled IR libraries.
Status Engine::LoadPreCompiledIRFiles(const std::string& byte_code_file_path) {
  /// Read from file into memory buffer.
  llvm::ErrorOr<std::unique_ptr<llvm::MemoryBuffer>> buffer_or_error =
      llvm::MemoryBuffer::getFile(byte_code_file_path);
  if (!buffer_or_error) {
    std::stringstream ss;
    ss << "Could not load module from IR " << byte_code_file_path << ": "
       << buffer_or_error.getError().message();
    return Status::CodeGenError(ss.str());
  }
  std::unique_ptr<llvm::MemoryBuffer> buffer = move(buffer_or_error.get());

  /// Parse the IR module.
  llvm::Expected<std::unique_ptr<llvm::Module>> module_or_error =
      llvm::getOwningLazyBitcodeModule(move(buffer), *context());
  if (!module_or_error) {
    std::string error_string;
    llvm::handleAllErrors(module_or_error.takeError(), [&](llvm::ErrorInfoBase& eib) {
      error_string = eib.message();
    });
    return Status::CodeGenError(error_string);
  }
  std::unique_ptr<llvm::Module> ir_module = move(module_or_error.get());

  /// Verify the IR module
  if (llvm::verifyModule(*ir_module, &llvm::errs())) {
    return Status::CodeGenError("verify of IR Module failed");
  }

  // Link this to the primary module.
  if (llvm::Linker::linkModules(*module_, move(ir_module))) {
    return Status::CodeGenError("failed to link IR Modules");
  }
  return Status::OK();
}

// Optimise and compile the module.
Status Engine::FinalizeModule(bool optimise_ir, bool dump_ir) {
  if (dump_ir) {
    DumpIR("Before optimise");
  }

  // Setup an optimiser pipeline
  std::unique_ptr<llvm::legacy::PassManager> pass_manager(
      new llvm::legacy::PassManager());

  // First round : get rid of all functions that don't need to be compiled.
  // This helps in reducing the overall compilation time.
  // (Adapted from Apache Impala)
  //
  // Done by marking all the unused functions as internal, and then, running
  // a pass for dead code elimination.
  std::unordered_set<std::string> used_functions;
  used_functions.insert(functions_to_compile_.begin(), functions_to_compile_.end());

  pass_manager->add(
      llvm::createInternalizePass([&used_functions](const llvm::GlobalValue& func) {
        return (used_functions.find(func.getName().str()) != used_functions.end());
      }));
  pass_manager->add(llvm::createGlobalDCEPass());
  pass_manager->run(*module_);

  if (optimise_ir) {
    // Second round : misc passes to allow for inlining, vectorization, ..
    pass_manager.reset(new llvm::legacy::PassManager());
    llvm::TargetIRAnalysis target_analysis =
        execution_engine_->getTargetMachine()->getTargetIRAnalysis();
    pass_manager->add(llvm::createTargetTransformInfoWrapperPass(target_analysis));
    pass_manager->add(llvm::createFunctionInliningPass());
    pass_manager->add(llvm::createInstructionCombiningPass());
    pass_manager->add(llvm::createPromoteMemoryToRegisterPass());
    pass_manager->add(llvm::createGVNPass());
    pass_manager->add(llvm::createNewGVNPass());
    pass_manager->add(llvm::createCFGSimplificationPass());
    pass_manager->add(llvm::createLoopVectorizePass());
    pass_manager->add(llvm::createSLPVectorizerPass());
    pass_manager->add(llvm::createGlobalOptimizerPass());

    // run the optimiser
    llvm::PassManagerBuilder pass_builder;
    pass_builder.OptLevel = 2;
    pass_builder.populateModulePassManager(*pass_manager);
    pass_manager->run(*module_);

    if (dump_ir) {
      DumpIR("After optimise");
    }
  }

  if (llvm::verifyModule(*module_, &llvm::errs())) {
    return Status::CodeGenError("verify of module failed after optimisation passes");
  }

  // do the compilation
  execution_engine_->finalizeObject();
  module_finalized_ = true;
  return Status::OK();
}

void* Engine::CompiledFunction(llvm::Function* irFunction) {
  DCHECK(module_finalized_);
  return execution_engine_->getPointerToFunction(irFunction);
}

void Engine::AddGlobalMappingForFunc(const std::string& name, llvm::Type* ret_type,
                                     const std::vector<llvm::Type*>& args,
                                     void* function_ptr) {
  auto prototype = llvm::FunctionType::get(ret_type, args, false /*isVarArg*/);
  auto fn = llvm::Function::Create(prototype, llvm::GlobalValue::ExternalLinkage, name,
                                   module());
  execution_engine_->addGlobalMapping(fn, function_ptr);
}

void Engine::AddGlobalMappings() {
  std::vector<llvm::Type*> args;

  // gdv_fn_like_utf8_utf8
  args = {types().i64_type(), types().i8_ptr_type(), types().i32_type(),
          types().i8_ptr_type(), types().i32_type()};
  AddGlobalMappingForFunc("gdv_fn_like_utf8_utf8", types().i1_type(), args,
                          reinterpret_cast<void*>(gdv_fn_like_utf8_utf8));

  // gdv_fn_to_date_utf8_utf8_int32
  args = {types().i64_type(),
          types().i8_ptr_type(),
          types().i32_type(),
          types().i1_type(),
          types().i8_ptr_type(),
          types().i32_type(),
          types().i1_type(),
          types().i32_type(),
          types().i1_type(),
          types().i64_type(),
          types().ptr_type(types().i8_type())};
  AddGlobalMappingForFunc("gdv_fn_to_date_utf8_utf8_int32", types().i64_type(), args,
                          reinterpret_cast<void*>(gdv_fn_to_date_utf8_utf8_int32));

  // gdv_fn_context_set_error_msg
  args = {types().i64_type(), types().i8_ptr_type()};
  AddGlobalMappingForFunc("gdv_fn_context_set_error_msg", types().void_type(), args,
                          reinterpret_cast<void*>(gdv_fn_context_set_error_msg));
}

void Engine::DumpIR(std::string prefix) {
  std::string str;

  llvm::raw_string_ostream stream(str);
  module_->print(stream, NULL);
  std::cout << "====" << prefix << "===" << str << "\n";
}

}  // namespace gandiva
